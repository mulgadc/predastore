package predastore

import (
	"cmp"
	"context"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
)

// What a shard position holds, measured against its placement record's epoch.
const (
	ShardMatch   = "match"
	ShardOlder   = "older"
	ShardNewer   = "newer"
	ShardMissing = "missing"
	ShardError   = "error"
)

const (
	defaultVerifyWorkers  = 4
	defaultVerifyRecheck  = 20 * time.Second
	defaultVerifyPageSize = 512
	verifyStatTimeout     = 15 * time.Second
)

// VerifyOptions tunes VerifyShards. A zero value takes each default.
type VerifyOptions struct {
	// Workers is how many objects are Stat'd at once. Default 4.
	Workers int
	// Recheck is how long to wait before re-reading and re-checking the
	// objects short of full, so a write landing mid-scan is not called damage.
	Recheck time.Duration
	// PageSize is how many records each scan page reads. Default 512.
	PageSize int
}

// ShardPosition is what one node holds at one shard position of an object.
type ShardPosition struct {
	Index     int    `json:"index"`
	Node      NodeID `json:"node"`
	State     string `json:"state"`
	HeldEpoch uint64 `json:"held_epoch,omitempty"`
	Error     string `json:"error,omitempty"`
}

// NonFullObject is an object with at least one position not at its epoch.
type NonFullObject struct {
	Hash       string          `json:"hash"`
	Name       string          `json:"name,omitempty"`
	Size       int64           `json:"size"`
	WriteEpoch uint64          `json:"write_epoch"`
	Match      int             `json:"match"`
	Shards     []ShardPosition `json:"shards"`
}

// NodeShardCounts is one blob node's share of the checked positions.
type NodeShardCounts struct {
	Node    NodeID `json:"node"`
	Owned   int    `json:"owned"`
	Match   int    `json:"match"`
	Older   int    `json:"older"`
	Newer   int    `json:"newer"`
	Missing int    `json:"missing"`
	Error   int    `json:"error"`
}

// ShardReport is the outcome of one VerifyShards run.
type ShardReport struct {
	Rows        int `json:"rows"`
	Records     int `json:"placement_records"`
	Empty       int `json:"empty"`
	Undecodable int `json:"undecodable"`
	// Vanished counts records deleted or emptied between the scan and the recheck.
	Vanished int `json:"vanished"`
	Checked  int `json:"checked"`
	Full     int `json:"full"`
	// ByMatch maps a count of positions at the record's epoch to how many
	// objects have exactly that many.
	ByMatch    map[int]int       `json:"by_match"`
	Nodes      []NodeShardCounts `json:"nodes"`
	NonFull    []NonFullObject   `json:"non_full"`
	DurationMs int64             `json:"duration_ms"`
}

type verifyMeta interface {
	LeaderScanFrom(ctx context.Context, prefix, after string, limit int) ([]meta.Item, error)
	LeaderGet(ctx context.Context, key string) ([]byte, error)
}

type shardStater interface {
	Stat(ctx context.Context, node NodeID, req blob.StatRequest) (*blob.StatResponse, error)
}

var (
	_ verifyMeta  = (*meta.Client)(nil)
	_ shardStater = (*blob.Client)(nil)
)

// VerifyShards audits every placement record against the blob nodes, read
// only: it reads records through the meta leader and Stats each shard
// position, issuing no write of any kind. rootCAs is as for NodeStatus.
func VerifyShards(ctx context.Context, cfg *Config, rootCAs *x509.CertPool, opts VerifyOptions) (ShardReport, error) {
	var metaIDs, blobIDs []NodeID
	for _, h := range cfg.Hosts {
		for _, n := range h.Nodes {
			switch n.Role {
			case RoleMeta:
				metaIDs = append(metaIDs, n.ID)
			case RoleBlob:
				blobIDs = append(blobIDs, n.ID)
			}
		}
	}
	if len(metaIDs) == 0 || len(blobIDs) == 0 {
		return ShardReport{}, fmt.Errorf("configuration names %d meta and %d blob nodes, need at least one of each",
			len(metaIDs), len(blobIDs))
	}

	quic, err := transport.NewQUICDialTransport("", 0, transport.WithRootCAs(rootCAs))
	if err != nil {
		return ShardReport{}, fmt.Errorf("create verify transport: %w", err)
	}
	defer quic.Close()

	res, err := rpc.NewRemoteResolver(cfg, quic)
	if err != nil {
		return ShardReport{}, fmt.Errorf("build verify resolver: %w", err)
	}
	connPool := rpc.NewConnPool(metaIDs[0], res)
	defer connPool.Close()
	cli := rpc.NewClient(connPool)

	mc, err := meta.NewClient(meta.ClientConfig{Client: cli, Replicas: metaIDs})
	if err != nil {
		return ShardReport{}, fmt.Errorf("create verify meta client: %w", err)
	}
	bc, err := blob.NewClient(blob.ClientConfig{Client: cli})
	if err != nil {
		return ShardReport{}, fmt.Errorf("create verify blob client: %w", err)
	}

	return verifyShards(ctx, mc, bc, blobIDs, opts)
}

type verifyObject struct {
	hash  [32]byte
	place handlers.ObjectToShardNodes
}

func verifyShards(ctx context.Context, mc verifyMeta, bc shardStater, blobIDs []NodeID, opts VerifyOptions) (ShardReport, error) {
	start := time.Now()
	opts.Workers = cmp.Or(opts.Workers, defaultVerifyWorkers)
	opts.Recheck = cmp.Or(opts.Recheck, defaultVerifyRecheck)
	opts.PageSize = cmp.Or(opts.PageSize, defaultVerifyPageSize)

	report := ShardReport{ByMatch: map[int]int{}}
	objs, names, err := scanPlacements(ctx, mc, opts.PageSize, &report)
	if err != nil {
		return ShardReport{}, err
	}

	first, err := statObjects(ctx, bc, objs, opts.Workers)
	if err != nil {
		return ShardReport{}, err
	}

	var suspects []verifyObject
	for _, o := range objs {
		if matching(first[o.hash]) < len(first[o.hash]) {
			suspects = append(suspects, o)
		}
	}
	refreshed := map[[32]byte]verifyObject{}
	if len(suspects) > 0 {
		select {
		case <-ctx.Done():
			return ShardReport{}, ctx.Err()
		case <-time.After(opts.Recheck):
		}
		var again []verifyObject
		for _, o := range suspects {
			raw, err := mc.LeaderGet(ctx, handlers.TableKey(model.TableObjects, string(o.hash[:])))
			if errors.Is(err, meta.ErrNotFound) {
				report.Vanished++
				continue
			}
			if err != nil {
				return ShardReport{}, fmt.Errorf("re-read placement record: %w", err)
			}
			place, err := handlers.DecodePlacement(raw)
			if err != nil || place.Size == 0 {
				report.Vanished++
				continue
			}
			o.place = place
			again = append(again, o)
			refreshed[o.hash] = o
		}
		second, err := statObjects(ctx, bc, again, opts.Workers)
		if err != nil {
			return ShardReport{}, err
		}
		for _, o := range suspects {
			delete(first, o.hash)
		}
		maps.Copy(first, second)
	}

	nodes := map[NodeID]*NodeShardCounts{}
	for _, id := range blobIDs {
		nodes[id] = &NodeShardCounts{Node: id}
	}
	for _, o := range objs {
		positions, ok := first[o.hash]
		if !ok {
			continue
		}
		if r, ok := refreshed[o.hash]; ok {
			o = r
		}
		report.Checked++
		m := matching(positions)
		report.ByMatch[m]++
		for _, p := range positions {
			n, ok := nodes[p.Node]
			if !ok {
				n = &NodeShardCounts{Node: p.Node}
				nodes[p.Node] = n
			}
			n.Owned++
			switch p.State {
			case ShardMatch:
				n.Match++
			case ShardOlder:
				n.Older++
			case ShardNewer:
				n.Newer++
			case ShardMissing:
				n.Missing++
			default:
				n.Error++
			}
		}
		if m == len(positions) {
			report.Full++
			continue
		}
		report.NonFull = append(report.NonFull, NonFullObject{
			Hash: hex.EncodeToString(o.hash[:]), Name: names[o.hash],
			Size: o.place.Size, WriteEpoch: o.place.WriteEpoch, Match: m, Shards: positions,
		})
	}
	for _, n := range nodes {
		report.Nodes = append(report.Nodes, *n)
	}
	slices.SortFunc(report.Nodes, func(a, b NodeShardCounts) int { return cmp.Compare(a.Node, b.Node) })
	slices.SortStableFunc(report.NonFull, func(a, b NonFullObject) int { return cmp.Compare(a.Match, b.Match) })
	report.DurationMs = time.Since(start).Milliseconds()

	return report, nil
}

// scanPlacements pages the objects table, returning the records with a body to
// check and the object names that resolve to each hash.
func scanPlacements(ctx context.Context, mc verifyMeta, pageSize int, report *ShardReport) ([]verifyObject, map[[32]byte]string, error) {
	const namePrefix = "arn:aws:s3:::"
	prefix := handlers.TableKey(model.TableObjects, "")
	names := map[[32]byte]string{}
	var objs []verifyObject
	cursor := ""
	for {
		items, err := mc.LeaderScanFrom(ctx, prefix, cursor, pageSize)
		if err != nil {
			return nil, nil, fmt.Errorf("scan placement records: %w", err)
		}
		for _, it := range items {
			cursor = it.Key
			report.Rows++
			if hash, ok := handlers.ObjectHashOfKey(it.Key); ok {
				place, err := handlers.DecodePlacement(it.Value)
				switch {
				case err != nil:
					report.Undecodable++
				case place.Size == 0:
					report.Empty++
				default:
					report.Records++
					objs = append(objs, verifyObject{hash: hash, place: place})
				}
				continue
			}
			key := strings.TrimPrefix(it.Key, prefix)
			if strings.HasPrefix(key, namePrefix) && len(it.Value) == 32 {
				names[[32]byte(it.Value)] = strings.TrimPrefix(key, namePrefix)
			}
		}
		if len(items) < pageSize {
			return objs, names, nil
		}
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
	}
}

// statObjects asks every node what it holds at each position of each object.
func statObjects(ctx context.Context, bc shardStater, objs []verifyObject, workers int) (map[[32]byte][]ShardPosition, error) {
	out := make(map[[32]byte][]ShardPosition, len(objs))
	var mu sync.Mutex
	work := make(chan verifyObject)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for o := range work {
				positions := statObject(ctx, bc, o)
				mu.Lock()
				out[o.hash] = positions
				mu.Unlock()
			}
		})
	}
feed:
	for _, o := range objs {
		select {
		case <-ctx.Done():
			break feed
		case work <- o:
		}
	}
	close(work)
	wg.Wait()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return out, nil
}

func statObject(ctx context.Context, bc shardStater, o verifyObject) []ShardPosition {
	nodes := o.place.AllNodes()
	positions := make([]ShardPosition, len(nodes))
	for i, n := range nodes {
		sctx, cancel := context.WithTimeout(ctx, verifyStatTimeout)
		held, err := bc.Stat(sctx, n, blob.StatRequest{Key: o.hash, Index: uint32(i)})
		cancel()
		p := ShardPosition{Index: i, Node: n}
		switch {
		case errors.Is(err, blob.ErrNotFound):
			p.State = ShardMissing
		case err != nil:
			p.State, p.Error = ShardError, err.Error()
		case held.Epoch == o.place.WriteEpoch:
			p.State, p.HeldEpoch = ShardMatch, held.Epoch
		case held.Epoch < o.place.WriteEpoch:
			p.State, p.HeldEpoch = ShardOlder, held.Epoch
		default:
			p.State, p.HeldEpoch = ShardNewer, held.Epoch
		}
		positions[i] = p
	}

	return positions
}

func matching(positions []ShardPosition) int {
	m := 0
	for _, p := range positions {
		if p.State == ShardMatch {
			m++
		}
	}

	return m
}
