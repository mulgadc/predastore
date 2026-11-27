package repair

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
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
		Key: key, Index: uint32(index), RangeStart: -1, RangeEnd: -1, Epoch: epoch, //nolint:gosec // G115: a stripe index.
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
		req := blob.PutRequest{Key: hash, Index: uint32(index), Size: int64(len(shards[index])), Epoch: place.WriteEpoch} //nolint:gosec // G115: a stripe index.
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

// A GET that meets a shard whose bytes fail authentication on its node is
// answered from the other shards. The node's repair pass must then rewrite
// that shard from its peers, so the node serves it again.
func TestARepairPassReplacesAShardThatFailedAuthenticationOnGet(t *testing.T) {
	const k, m = 2, 1
	c := newCluster(t, k, m, 4)
	nodes := startRealNodes(t, c.nodes)
	ctx := context.Background()

	hash, place, shards := nodes.store(c, "corrupt-on-get")

	target := place.DataShardNodes[0]
	corruptOnlyShard(t, nodes.dirs[target])
	_, err := nodes.readShard(target, hash, 0, place.WriteEpoch)
	require.Error(t, err, "the flipped byte must fail authentication on the node")

	svc, err := New(Config{
		Nodes: []config.NodeID{target}, Ring: c.ring, Meta: c.meta, Blob: nodes.client,
		DataShards: k, ParityShards: m, Workers: 1,
	})
	require.NoError(t, err)
	require.NoError(t, svc.Pass(ctx))

	got, err := nodes.readShard(target, hash, 0, place.WriteEpoch)
	require.NoError(t, err, "after the pass the node serves its shard again")
	assert.Equal(t, shards[0], got, "the rewritten shard holds the bytes the PUT stored")
	for index, node := range place.AllNodes()[1:] {
		got, err := nodes.readShard(node, hash, index+1, place.WriteEpoch)
		require.NoError(t, err)
		assert.Equal(t, shards[index+1], got, "a healthy shard is left as it was")
	}
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
