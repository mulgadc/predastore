package repair

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The first fragment of a fresh segment starts after the segment header, and
// its ciphertext after the fragment header. These are the engine's sizes.
const (
	testSegHeaderSize  = 14
	testFragHeaderSize = 32
)

var realHostSeq atomic.Uint64

// realNodes is a set of real blob nodes, each on its own engine store, and the
// real blob client that reaches them over the pipe transport.
type realNodes struct {
	t      *testing.T
	host   string
	client *blob.Client
	dirs   map[config.NodeID]string
	stops  map[config.NodeID]func()
}

const realBasePort = 7600

func startRealNodes(t *testing.T, ids []config.NodeID) *realNodes {
	t.Helper()
	host := fmt.Sprintf("repair-real-%d-%s", realHostSeq.Add(1), strings.ReplaceAll(t.Name(), "/", "-"))
	const clientNode = config.NodeID(99)
	nodes := make([]config.Node, 0, len(ids)+1)
	for _, id := range ids {
		nodes = append(nodes, config.Node{ID: id, Role: config.RoleBlob, Port: realBasePort + int(id)})
	}
	nodes = append(nodes, config.Node{ID: clientNode, Role: config.RoleBlob, Port: realBasePort + int(clientNode)})
	clusterCfg := &config.Config{Hosts: []config.Host{{ID: 1, Addr: host, Nodes: nodes}}}

	r := &realNodes{t: t, host: host, dirs: map[config.NodeID]string{}, stops: map[config.NodeID]func(){}}
	for _, id := range ids {
		r.dirs[id] = t.TempDir()
		r.start(id, storetest.TestAEAD())
	}

	clientTr := transport.NewPipeTransport(host, realBasePort+int(clientNode))
	t.Cleanup(func() { clientTr.Close() })
	res, err := rpc.NewResolver(clusterCfg, clientNode, clientTr)
	require.NoError(t, err)
	pool := rpc.NewConnPool(clientNode, res)
	t.Cleanup(func() { _ = pool.Close() })
	r.client, err = blob.NewClient(blob.ClientConfig{
		Client: rpc.NewClient(pool), EnvelopeTimeout: 5 * time.Second, IdleTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	return r
}

// start runs node id on its data directory under aead.
func (r *realNodes) start(id config.NodeID, aead cipher.AEAD) {
	t := r.t
	t.Helper()
	tr := transport.NewPipeTransport(r.host, realBasePort+int(id))
	ln, err := tr.Listen()
	require.NoError(t, err)
	srv, err := blob.New(blob.Config{
		NodeID: id, DataDir: r.dirs[id], AEAD: aead, Listeners: []transport.Listener{ln},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			<-done
			tr.Close()
		})
	}
	r.stops[id] = stop
	t.Cleanup(stop)
}

// restart stops node id and starts it again on the same store under aead.
func (r *realNodes) restart(id config.NodeID, aead cipher.AEAD) {
	r.stops[id]()
	r.start(id, aead)
}

// readShard reads one whole shard from a node at epoch, as the gate's GET does.
func (r *realNodes) readShard(node config.NodeID, key [32]byte, index int, epoch uint64) ([]byte, error) {
	rc, err := r.client.Get(context.Background(), node, blob.GetRequest{
		Key: key, Index: uint32(index), RangeStart: -1, RangeEnd: -1, Epoch: epoch,
	})
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// store writes one object to the real nodes as a completed PUT leaves it, and
// records its placement.
func (r *realNodes) store(c *cluster, name string) ([32]byte, handlers.ObjectToShardNodes, [][]byte) {
	t := r.t
	t.Helper()
	ctx := context.Background()
	body := bytes.Repeat([]byte(name), (64<<10)/len(name))
	hash := sha256.Sum256([]byte(name))
	place := c.placeFor(hash, int64(len(body)), 7)
	shards := encodeStripe(t, c.k, c.m, body)
	for index, node := range place.AllNodes() {
		req := blob.PutRequest{Key: hash, Index: uint32(index), Size: int64(len(shards[index])), Epoch: place.WriteEpoch}
		_, err := r.client.Put(ctx, node, req, bytes.NewReader(shards[index]))
		require.NoError(t, err)
		_, err = r.client.Commit(ctx, node, blob.CommitRequest{Key: hash, Index: req.Index, Epoch: place.WriteEpoch})
		require.NoError(t, err)
	}
	c.record(hash, place)
	return hash, place, shards
}

// corruptOnlyShard flips one ciphertext byte of the first fragment in the one
// segment that holds data on a node, which holds a single shard.
func corruptOnlyShard(t *testing.T, dir string) {
	t.Helper()
	segs, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	require.NoError(t, err)
	var held []string
	for _, seg := range segs {
		info, err := os.Stat(seg)
		require.NoError(t, err)
		if info.Size() > testSegHeaderSize+testFragHeaderSize {
			held = append(held, seg)
		}
	}
	require.Len(t, held, 1, "the node must hold exactly one segment with data")

	f, err := os.OpenFile(held[0], os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	b := make([]byte, 1)
	_, err = f.ReadAt(b, testSegHeaderSize+testFragHeaderSize)
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b, testSegHeaderSize+testFragHeaderSize)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
}

// s3Meta is a map that serves both the S3 handlers and repair, so one store
// holds what a PUT wrote and what a repair pass scans.
type s3Meta struct {
	mu   sync.Mutex
	rows map[string][]byte
}

func (m *s3Meta) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.rows[key]
	if !ok {
		return nil, meta.ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

func (m *s3Meta) LeaderGet(ctx context.Context, key string) ([]byte, error) { return m.Get(ctx, key) }

func (m *s3Meta) Put(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows[key] = append([]byte(nil), value...)
	return nil
}

func (m *s3Meta) Swap(_ context.Context, key string, value []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	prev := m.rows[key]
	m.rows[key] = append([]byte(nil), value...)
	return prev, nil
}

func (m *s3Meta) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.rows, key)
	return nil
}

func (m *s3Meta) Scan(ctx context.Context, prefix string, limit int) ([]meta.Item, error) {
	return m.LeaderScanFrom(ctx, prefix, "", limit)
}

func (m *s3Meta) ScanFrom(ctx context.Context, prefix, after string, limit int) ([]meta.Item, error) {
	return m.LeaderScanFrom(ctx, prefix, after, limit)
}

func (m *s3Meta) LeaderScanFrom(_ context.Context, prefix, after string, limit int) ([]meta.Item, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.rows))
	for k := range m.rows {
		if strings.HasPrefix(k, prefix) && k > after {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	if limit > 0 && len(keys) > limit {
		keys = keys[:limit]
	}
	items := make([]meta.Item, len(keys))
	for i, k := range keys {
		items[i] = meta.Item{Key: k, Value: append([]byte(nil), m.rows[k]...)}
	}
	return items, nil
}

var (
	_ handlers.MetaClient = (*s3Meta)(nil)
	_ MetaClient          = (*s3Meta)(nil)
)

// serveS3 sends one object request through an S3 handler.
func serveS3(h http.Handler, method, key string, body []byte) *httptest.ResponseRecorder {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	ctx := handlers.WithBucket(context.Background(), model.Bucket{Name: "bkt"})
	ctx = handlers.WithObject(ctx, model.Object{Bucket: model.Bucket{Name: "bkt"}, Key: key})
	req := httptest.NewRequest(method, "/bkt/"+key, rd).WithContext(ctx)
	req.ContentLength = int64(len(body))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// s3Fixture is real blob nodes behind the S3 PUT and GET handlers, and one
// object written through them.
type s3Fixture struct {
	c      *cluster
	nodes  *realNodes
	mc     *s3Meta
	get    http.Handler
	body   []byte
	hash   [32]byte
	place  handlers.ObjectToShardNodes
	shards [][]byte
}

func newS3Fixture(t *testing.T, k, m, nodeCount int) *s3Fixture {
	t.Helper()
	f := &s3Fixture{c: newCluster(t, k, m, nodeCount), mc: &s3Meta{rows: map[string][]byte{}}}
	f.nodes = startRealNodes(t, f.c.nodes)
	epochs, err := handlers.NewEpochMinter(99)
	require.NoError(t, err)
	cfg := handlers.Config{
		Region: "ap-southeast-2", DataShards: k, ParityShards: m,
		DegradedWrites: true, HintedHandoff: true, Epochs: epochs,
	}
	cache := handlers.NewBucketCache([]handlers.BucketConfig{{Name: "bkt"}})
	f.get = handlers.GetObject(f.mc, f.nodes.client, f.c.ring, cache, cfg)

	f.body = bytes.Repeat([]byte("integrity "), 6<<10)
	rr := serveS3(handlers.PutObject(f.mc, f.nodes.client, f.c.ring, cache, cfg), http.MethodPut, "k", f.body)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	f.hash = model.ObjectHash("bkt", "k")
	raw, err := f.mc.Get(context.Background(), handlers.TableKey(model.TableObjects, string(f.hash[:])))
	require.NoError(t, err)
	f.place, err = handlers.DecodePlacement(raw)
	require.NoError(t, err)
	f.shards = make([][]byte, k+m)
	for index, node := range f.place.AllNodes() {
		f.shards[index], err = f.nodes.readShard(node, f.hash, index, f.place.WriteEpoch)
		require.NoError(t, err)
	}
	return f
}

// suspect reports whether the node holding shard index marks it suspect.
func (f *s3Fixture) suspect(t *testing.T, index int) bool {
	t.Helper()
	held, err := f.nodes.client.Stat(context.Background(), f.place.AllNodes()[index], blob.StatRequest{Key: f.hash, Index: uint32(index)}) //nolint:gosec // G115: a stripe index.
	require.NoError(t, err)
	return held.Suspect
}

// getAnswersTheObject sends an S3 GET and checks it answers the stored bytes.
func (f *s3Fixture) getAnswersTheObject(t *testing.T) {
	t.Helper()
	rr := serveS3(f.get, http.MethodGet, "k", nil)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, f.body, rr.Body.Bytes(), "the GET answers the stored bytes, rebuilt from the other shards")
}

func (f *s3Fixture) pass(t *testing.T, node config.NodeID) {
	t.Helper()
	svc, err := New(Config{
		Nodes: []config.NodeID{node}, Ring: f.c.ring, Meta: f.mc, Blob: f.nodes.client,
		DataShards: f.c.k, ParityShards: f.c.m, Workers: 1,
	})
	require.NoError(t, err)
	require.NoError(t, svc.Pass(context.Background()))
}

// An S3 GET that meets a shard whose bytes fail authentication on its node is
// answered from the other shards and marks the shard. The node's repair pass
// rewrites it from its peers and reads it back, which clears the mark.
func TestARepairPassReplacesAShardThatFailedAuthenticationOnGet(t *testing.T) {
	f := newS3Fixture(t, 2, 1, 4)
	target := f.place.DataShardNodes[0]
	corruptOnlyShard(t, f.nodes.dirs[target])

	f.getAnswersTheObject(t)
	require.Eventually(t, func() bool { return f.suspect(t, 0) }, 5*time.Second, 5*time.Millisecond,
		"the GET must mark the shard it could not read")

	f.pass(t, target)
	assert.Eventually(t, func() bool { return !f.suspect(t, 0) }, 5*time.Second, 5*time.Millisecond,
		"the pass's own read back must clear the mark")

	got, err := f.nodes.readShard(target, f.hash, 0, f.place.WriteEpoch)
	require.NoError(t, err, "after the pass the node serves its shard again")
	assert.Equal(t, f.shards[0], got, "the rewritten shard holds the bytes the PUT stored")
	for index, node := range f.place.AllNodes()[1:] {
		got, err := f.nodes.readShard(node, f.hash, index+1, f.place.WriteEpoch)
		require.NoError(t, err)
		assert.Equal(t, f.shards[index+1], got, "a healthy shard is left as it was")
	}
}

// With two bad shards in an RS(2,2) stripe, the rebuild of one must not stop
// at the other bad peer: it tries the next peer and rebuilds from the two
// healthy ones.
func TestARepairPassRebuildsPastAPeerThatFailsAuthentication(t *testing.T) {
	f := newS3Fixture(t, 2, 2, 5)
	target := f.place.DataShardNodes[0]
	corruptOnlyShard(t, f.nodes.dirs[target])
	corruptOnlyShard(t, f.nodes.dirs[f.place.DataShardNodes[1]])

	f.getAnswersTheObject(t)
	require.Eventually(t, func() bool { return f.suspect(t, 0) && f.suspect(t, 1) }, 5*time.Second, 5*time.Millisecond)

	f.pass(t, target)
	got, err := f.nodes.readShard(target, f.hash, 0, f.place.WriteEpoch)
	require.NoError(t, err, "the rebuild must reach the healthy peers")
	assert.Equal(t, f.shards[0], got)
}

// A node started under a key that is not the cluster's fails authentication on
// every shard it holds, and a repair pass must not replace them. Started again
// under the cluster's key, the node serves its original shard.
func TestARepairPassUnderAWrongKeyKeepsTheShardTheRightKeyReads(t *testing.T) {
	const k, m = 2, 1
	c := newCluster(t, k, m, 4)
	nodes := startRealNodes(t, c.nodes)
	ctx := context.Background()
	hash, place, shards := nodes.store(c, "wrong-key")
	target := place.DataShardNodes[0]

	wrong, err := masterkey.NewAEAD(bytes.Repeat([]byte{0x43}, 32))
	require.NoError(t, err)
	nodes.restart(target, wrong)
	require.Eventually(t, func() bool {
		_, err := nodes.readShard(target, hash, 0, place.WriteEpoch)
		return err != nil && !errors.Is(err, blob.ErrNotFound)
	}, 5*time.Second, 20*time.Millisecond, "the node under the wrong key must fail authentication")

	svc, err := New(Config{
		Nodes: []config.NodeID{target}, Ring: c.ring, Meta: c.meta, Blob: nodes.client,
		DataShards: k, ParityShards: m, Workers: 1,
	})
	require.NoError(t, err)
	require.NoError(t, svc.Pass(ctx))

	nodes.restart(target, storetest.TestAEAD())
	var got []byte
	require.Eventually(t, func() bool {
		got, err = nodes.readShard(target, hash, 0, place.WriteEpoch)
		return err == nil
	}, 5*time.Second, 20*time.Millisecond, "under the cluster's key the node serves its shard")
	assert.Equal(t, shards[0], got)
}
