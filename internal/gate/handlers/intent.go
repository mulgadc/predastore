package handlers

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
)

// IntentState is where a conditional write stands. Only a transaction that
// checks IntentPending moves it, so exactly one of committed and aborted wins.
type IntentState string

const (
	// IntentPending: the shards may be arriving and nothing is decided.
	IntentPending IntentState = "pending"
	// IntentCommitted: the write's metadata is published; its shards must be
	// committed and the generation it displaced released.
	IntentCommitted IntentState = "committed"
	// IntentAborted: the write can never publish; its shards must be aborted.
	IntentAborted IntentState = "aborted"
)

// IntentPrefix starts every stored intent key, so the table is one scan.
const IntentPrefix = model.TableIntents + "/"

// IntentID names one write: the hash its shards are stored under and its
// write epoch. Two writers of one key never share an epoch, so never a row.
type IntentID struct {
	Hash  [32]byte
	Epoch uint64
}

// Key is the stored meta key of the intent row.
func (id IntentID) Key() string {
	var buf [32 + 8]byte
	copy(buf[:32], id.Hash[:])
	binary.BigEndian.PutUint64(buf[32:], id.Epoch)
	return IntentPrefix + string(buf[:])
}

// ParseIntentKey recovers the ID from a stored intent key, as a scan returns
// it, and reports false for any other key.
func ParseIntentKey(stored string) (IntentID, bool) {
	rest, ok := strings.CutPrefix(stored, IntentPrefix)
	if !ok || len(rest) != 32+8 {
		return IntentID{}, false
	}
	id := IntentID{Hash: [32]byte([]byte(rest[:32])), Epoch: binary.BigEndian.Uint64([]byte(rest[32:]))}
	return id, id.Epoch != 0
}

// IntentRelease is the generation a committed write displaced: its epoch and
// the node of each shard index.
type IntentRelease struct {
	Epoch uint64          `json:"epoch"`
	Nodes []config.NodeID `json:"nodes"`
}

// Intent is the durable obligation of one conditional write. It names every
// node that can hold one of the write's shards: the owner of each index, and
// the handoff node at every index.
type Intent struct {
	State     IntentState `json:"state"`
	Bucket    string      `json:"bucket"`
	Key       string      `json:"key"`
	VersionID string      `json:"version_id,omitempty"`
	// Nodes is the owner of each shard index, as the placement names it.
	Nodes []config.NodeID `json:"nodes"`
	// Handoff is the node a refused shard lands on, zero when there is none.
	Handoff config.NodeID `json:"handoff,omitempty"`
	// Release is what to release once the shards commit, nil for nothing.
	Release *IntentRelease `json:"release,omitempty"`
	Created time.Time      `json:"created"`
}

// intentFormat versions the stored encoding, so a later field set can be
// told from this one rather than read as it.
const intentFormat = 1

type storedIntent struct {
	Intent

	Format int `json:"v"`
}

var errIntentFormat = errors.New("malformed intent row")

// EncodeIntent renders an intent row. The bytes are what a transaction checks,
// so a caller keeps the bytes it wrote rather than encoding again.
func EncodeIntent(in Intent) ([]byte, error) {
	if err := in.validate(); err != nil {
		return nil, err
	}
	return json.Marshal(storedIntent{Format: intentFormat, Intent: in})
}

// DecodeIntent reads an intent row.
func DecodeIntent(data []byte) (Intent, error) {
	var s storedIntent
	if err := json.Unmarshal(data, &s); err != nil {
		return Intent{}, fmt.Errorf("%w: %w", errIntentFormat, err)
	}
	if s.Format != intentFormat {
		return Intent{}, fmt.Errorf("%w: format %d", errIntentFormat, s.Format)
	}
	if err := s.validate(); err != nil {
		return Intent{}, err
	}
	return s.Intent, nil
}

func (in Intent) validate() error {
	switch in.State {
	case IntentPending, IntentCommitted, IntentAborted:
	default:
		return fmt.Errorf("%w: state %q", errIntentFormat, in.State)
	}
	if len(in.Nodes) == 0 {
		return fmt.Errorf("%w: no shard nodes", errIntentFormat)
	}
	return nil
}

// withState returns a copy of the intent in state s.
func (in Intent) withState(s IntentState) Intent {
	in.State = s
	return in
}

// shardPos is one (node, shard index) pair an action is sent to.
type shardPos struct {
	node  config.NodeID
	index int
}

// candidates are every position that can hold one of the write's shards.
func (in Intent) candidates() []shardPos {
	out := make([]shardPos, 0, 2*len(in.Nodes))
	for i, n := range in.Nodes {
		out = append(out, shardPos{n, i})
		if in.Handoff != 0 && in.Handoff != n {
			out = append(out, shardPos{in.Handoff, i})
		}
	}
	return out
}

// releases are every position that can hold the displaced generation.
func (in Intent) releases() []shardPos {
	if in.Release == nil {
		return nil
	}
	out := make([]shardPos, 0, 2*len(in.Release.Nodes))
	for i, n := range in.Release.Nodes {
		out = append(out, shardPos{n, i})
		if in.Handoff != 0 && in.Handoff != n {
			out = append(out, shardPos{in.Handoff, i})
		}
	}
	return out
}

// IntentBlob is what settling an intent needs of the blob nodes.
type IntentBlob interface {
	Commit(ctx context.Context, nodeID config.NodeID, req blob.CommitRequest) (superseded bool, err error)
	Abort(ctx context.Context, nodeID config.NodeID, req blob.CommitRequest) error
	Release(ctx context.Context, nodeID config.NodeID, req blob.ReleaseRequest) error
}

var _ IntentBlob = (*blob.Client)(nil)

// IntentCommit is what committing an intent's shards achieved.
type IntentCommit struct {
	// Durable counts the shard indexes durable at the write's epoch on some
	// candidate: published, or overtaken by a generation held behind it.
	Durable int
	// Overtaken counts the commits a newer generation had already beaten.
	Overtaken int
	// Complete is true only when every candidate answered its commit and
	// every release succeeded. Only then may the intent row be deleted.
	Complete bool
}

// commitAnswered reports whether a commit settled its position: published,
// overtaken, or nothing prepared there at that epoch.
func commitAnswered(err error) bool {
	return err == nil || errors.Is(err, blob.ErrNotPrepared) || errors.Is(err, blob.ErrOvertakenUnproven)
}

// CommitIntent commits the write's shards on every candidate, then, once all
// of them answered, releases the displaced generation. A position that did
// not answer leaves the result incomplete, and releases wait for a later pass.
func CommitIntent(ctx context.Context, bc IntentBlob, id IntentID, in Intent) IntentCommit {
	durable := make([]bool, len(in.Nodes))
	var (
		mu                   sync.Mutex
		overtaken, unsettled int
	)
	forEachPos(in.candidates(), func(p shardPos) {
		over, err := bc.Commit(ctx, p.node, commitRequest(id, p.index))
		mu.Lock()
		defer mu.Unlock()
		if err == nil {
			durable[p.index] = true
			if over {
				overtaken++
			}
		}
		if commitAnswered(err) {
			return
		}
		unsettled++
		slog.WarnContext(ctx, "Conditional write commit unanswered; the intent stays for the reaper",
			"node", p.node, "index", p.index, "epoch", fmt.Sprintf("%016x", id.Epoch), "err", err)
	})

	result := IntentCommit{Overtaken: overtaken}
	for _, ok := range durable {
		if ok {
			result.Durable++
		}
	}
	if unsettled > 0 {
		return result
	}
	result.Complete = releaseIntent(ctx, bc, id, in)
	return result
}

// releaseIntent releases the displaced generation at every position that can
// hold it, and reports whether every release succeeded.
func releaseIntent(ctx context.Context, bc IntentBlob, id IntentID, in Intent) bool {
	var failed atomic.Bool
	forEachPos(in.releases(), func(p shardPos) {
		err := bc.Release(ctx, p.node, blob.ReleaseRequest{
			Key:   id.Hash,
			Index: uint32(p.index), //nolint:gosec // G115: index bounded by the shard count.
			Epoch: in.Release.Epoch,
		})
		if err != nil {
			failed.Store(true)
			slog.WarnContext(ctx, "Displaced generation release failed; the intent stays for the reaper",
				"node", p.node, "index", p.index, "epoch", fmt.Sprintf("%016x", in.Release.Epoch), "err", err)
		}
	})
	return !failed.Load()
}

// AbortIntent aborts the write's shards on every candidate and reports whether
// each one confirmed that nothing is prepared and no writer is still open.
func AbortIntent(ctx context.Context, bc IntentBlob, id IntentID, in Intent) bool {
	var failed atomic.Bool
	forEachPos(in.candidates(), func(p shardPos) {
		if err := bc.Abort(ctx, p.node, commitRequest(id, p.index)); err != nil {
			failed.Store(true)
			slog.WarnContext(ctx, "Conditional write abort unconfirmed; the intent stays for the reaper",
				"node", p.node, "index", p.index, "epoch", fmt.Sprintf("%016x", id.Epoch), "err", err)
		}
	})
	return !failed.Load()
}

func commitRequest(id IntentID, index int) blob.CommitRequest {
	return blob.CommitRequest{
		Key:   id.Hash,
		Index: uint32(index), //nolint:gosec // G115: index bounded by the shard count.
		Epoch: id.Epoch,
	}
}

// forEachPos runs fn concurrently against every position and waits.
func forEachPos(positions []shardPos, fn func(shardPos)) {
	var wg sync.WaitGroup
	for _, p := range positions {
		wg.Go(func() { fn(p) })
	}
	wg.Wait()
}

// IntentTxn is what fencing an intent needs of the meta client.
type IntentTxn interface {
	Txn(ctx context.Context, checks []meta.TxnCheck, writes []meta.TxnWrite) (meta.TxnOutcome, []meta.TxnValue, error)
}

var _ IntentTxn = (*meta.Client)(nil)

// FenceResult is what a fence learned about a pending intent.
type FenceResult int

const (
	// FenceUnknown: the fence was refused or its outcome is unknown, so the
	// intent is undecided as far as this caller knows.
	FenceUnknown FenceResult = iota
	// FenceApplied: this fence moved the intent to aborted.
	FenceApplied
	// FenceCommitted: the write's own transaction won.
	FenceCommitted
	// FenceAborted: another fence already aborted it.
	FenceAborted
	// FenceGone: the row is absent, so its cleanup already completed.
	FenceGone
)

// FenceIntent moves a pending intent to aborted, so its write can never
// publish, and answers the intent as it now stands. pending is the stored row
// exactly as read; a row whose bytes differ is reported, never overwritten.
func FenceIntent(ctx context.Context, mc IntentTxn, id IntentID, pending []byte) (FenceResult, Intent, error) {
	in, err := DecodeIntent(pending)
	if err != nil {
		return FenceUnknown, Intent{}, err
	}
	if in.State != IntentPending {
		return FenceUnknown, Intent{}, fmt.Errorf("%w: fence of a %s intent", errIntentFormat, in.State)
	}
	aborted := in.withState(IntentAborted)
	abortedRow, err := EncodeIntent(aborted)
	if err != nil {
		return FenceUnknown, Intent{}, err
	}
	key := []byte(id.Key())
	outcome, current, err := mc.Txn(ctx,
		[]meta.TxnCheck{{Key: key, Value: pending}},
		[]meta.TxnWrite{{Key: key, Value: abortedRow}})
	switch outcome {
	case meta.TxnApplied:
		return FenceApplied, aborted, nil
	case meta.TxnMismatch:
		return fenceMismatch(current)
	default:
		return FenceUnknown, Intent{}, err
	}
}

func fenceMismatch(current []meta.TxnValue) (FenceResult, Intent, error) {
	if len(current) != 1 {
		return FenceUnknown, Intent{}, fmt.Errorf("fence mismatch answered %d values", len(current))
	}
	if !current[0].Exists {
		return FenceGone, Intent{}, nil
	}
	in, err := DecodeIntent(current[0].Value)
	if err != nil {
		return FenceUnknown, Intent{}, err
	}
	switch in.State {
	case IntentCommitted:
		return FenceCommitted, in, nil
	case IntentAborted:
		return FenceAborted, in, nil
	default:
		return FenceUnknown, in, fmt.Errorf("fence mismatch on a %s intent", in.State)
	}
}
