package blob_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	epochHost           = "epoch-test-host"
	epochServerPort     = 6311
	epochClientPort     = 6312
	epochServerNode     = config.NodeID(1)
	epochClientNodeID   = config.NodeID(2)
	epochTestKeyByte    = 0x77
	epochTestShardIndex = 3
)

// startBlobNode runs a real blob node over a pipe transport, backed by a real
// engine store, and returns a client addressed to it. Everything the epoch
// protocol asserts is a property of that pair, so neither side is faked.
func startBlobNode(t *testing.T) *blob.Client {
	t.Helper()

	clusterCfg := &config.Config{
		Hosts: []config.Host{{
			ID:   1,
			Addr: epochHost,
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleBlob, Port: epochServerPort},
				{ID: 2, Role: config.RoleBlob, Port: epochClientPort},
			},
		}},
	}

	serverTr := transport.NewPipeTransport(epochHost, epochServerPort)
	t.Cleanup(func() { serverTr.Close() })
	ln, err := serverTr.Listen()
	require.NoError(t, err)

	srv, err := blob.New(blob.Config{
		NodeID:    epochServerNode,
		DataDir:   t.TempDir(),
		AEAD:      storetest.TestAEAD(),
		Listeners: []transport.Listener{ln},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	clientTr := transport.NewPipeTransport(epochHost, epochClientPort)
	t.Cleanup(func() { clientTr.Close() })
	clientRes, err := rpc.NewResolver(clusterCfg, 2, clientTr)
	require.NoError(t, err)

	client, err := blob.NewClient(blob.ClientConfig{
		Client:          rpc.NewClient(rpc.NewConnPool(2, clientRes)),
		EnvelopeTimeout: 5 * time.Second,
		IdleTimeout:     5 * time.Second,
	})
	require.NoError(t, err)

	return client
}

func epochKey() [32]byte { return [32]byte{epochTestKeyByte} }

// put streams one shard to the node under the given epoch, without committing.
func put(t *testing.T, c *blob.Client, epoch uint64, body []byte) {
	t.Helper()
	_, err := c.Put(context.Background(), epochServerNode, blob.PutRequest{
		Key:   epochKey(),
		Index: epochTestShardIndex,
		Size:  int64(len(body)),
		Epoch: epoch,
	}, bytes.NewReader(body))
	require.NoError(t, err)
}

// get reads the shard, demanding the given epoch.
func get(t *testing.T, c *blob.Client, epoch uint64) ([]byte, error) {
	t.Helper()
	r, err := c.Get(context.Background(), epochServerNode, blob.GetRequest{
		Key:        epochKey(),
		Index:      epochTestShardIndex,
		RangeStart: -1,
		RangeEnd:   -1,
		Epoch:      epoch,
	})
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}

func commit(t *testing.T, c *blob.Client, epoch uint64) error {
	t.Helper()

	_, err := c.Commit(context.Background(), epochServerNode, blob.CommitRequest{
		Key: epochKey(), Index: epochTestShardIndex, Epoch: epoch,
	})

	return err
}

// A shard is invisible between its put and its commit, so an overwrite serves
// its previous generation for the whole of that window.
func TestBlobPutIsInvisibleUntilCommitted(t *testing.T) {
	c := startBlobNode(t)
	first, second := []byte("generation one"), []byte("generation two")

	put(t, c, 1, first)
	require.NoError(t, commit(t, c, 1))

	put(t, c, 2, second)

	got, err := get(t, c, 1)
	require.NoError(t, err, "the committed generation must still serve")
	assert.Equal(t, first, got)

	require.NoError(t, commit(t, c, 2))

	got, err = get(t, c, 2)
	require.NoError(t, err)
	assert.Equal(t, second, got)
}

// The correctness property: a node holding the wrong generation must say so
// rather than hand back bytes that would be spliced into a plausible wrong
// object.
func TestBlobGetRefusesAStaleShard(t *testing.T) {
	c := startBlobNode(t)

	put(t, c, 1, []byte("generation one"))
	require.NoError(t, commit(t, c, 1))

	_, err := get(t, c, 2)
	require.ErrorIs(t, err, blob.ErrEpochMismatch)
	assert.Contains(t, err.Error(), "node holds 0000000000000001",
		"the error must name the epoch the node actually has")
}

// A gate that published the placement record and died before committing leaves
// the record naming a generation the node has prepared but not published.
// Completing that commit is the only outcome that can be right, and any reader
// can drive it — which is what stops the crash window from being a permanent
// unreadable object.
func TestBlobGetCompletesAnAbandonedCommit(t *testing.T) {
	c := startBlobNode(t)
	first, second := []byte("generation one"), []byte("generation two")

	put(t, c, 1, first)
	require.NoError(t, commit(t, c, 1))

	// The writer's commit never arrives.
	put(t, c, 2, second)

	got, err := get(t, c, 2)
	require.NoError(t, err, "a prepared shard under the requested epoch must be published and served")
	assert.Equal(t, second, got)

	// Published, not merely served once: an epoch-less get reads the live row,
	// so this is the node's own answer to what it now holds.
	got, err = get(t, c, 0)
	require.NoError(t, err)
	assert.Equal(t, second, got, "the completed commit did not become the live generation")

	// The generation it superseded is retained, not destroyed: a record still
	// naming it has to resolve.
	got, err = get(t, c, 1)
	require.NoError(t, err)
	assert.Equal(t, first, got)
}

// A get with no epoch reads whatever the node holds. Only a caller with no
// placement record should do that, and repair is the one that will.
func TestBlobGetWithoutAnEpochReadsWhateverIsThere(t *testing.T) {
	c := startBlobNode(t)
	body := []byte("generation one")

	put(t, c, 1, body)
	require.NoError(t, commit(t, c, 1))

	got, err := get(t, c, 0)
	require.NoError(t, err)
	assert.Equal(t, body, got)
}

// Zero is reserved as invalid, so a caller that forgot the epoch is refused
// rather than storing a shard nothing can ever match.
func TestBlobPutRejectsAZeroEpoch(t *testing.T) {
	c := startBlobNode(t)
	body := []byte("payload")

	_, err := c.Put(context.Background(), epochServerNode, blob.PutRequest{
		Key: epochKey(), Index: epochTestShardIndex, Size: int64(len(body)),
	}, bytes.NewReader(body))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "write epoch")
}

// A commit may be driven more than once by a caller that cannot tell whether
// the first one landed, so the second must not report a successful write as a
// failure.
func TestBlobCommitIsIdempotent(t *testing.T) {
	c := startBlobNode(t)
	body := []byte("payload")

	put(t, c, 1, body)
	require.NoError(t, commit(t, c, 1))
	require.NoError(t, commit(t, c, 1), "a repeated commit must report success")

	got, err := get(t, c, 1)
	require.NoError(t, err)
	assert.Equal(t, body, got)
}

// Committing something never prepared has to be distinguishable, because the
// caller's answer is to rewrite the shard rather than retry.
func TestBlobCommitWithoutAPutIsReported(t *testing.T) {
	c := startBlobNode(t)

	require.ErrorIs(t, commit(t, c, 1), blob.ErrNotPrepared)
}

// Abort releases the space a failed write reserved rather than leaving the
// node to age it out, and it must leave the previous generation alone.
func TestBlobAbortDiscardsThePreparedShard(t *testing.T) {
	c := startBlobNode(t)
	first := []byte("generation one")

	put(t, c, 1, first)
	require.NoError(t, commit(t, c, 1))

	put(t, c, 2, []byte("generation two"))
	require.NoError(t, c.Abort(context.Background(), epochServerNode, blob.CommitRequest{
		Key: epochKey(), Index: epochTestShardIndex, Epoch: 2,
	}))

	require.ErrorIs(t, commit(t, c, 2), blob.ErrNotPrepared)

	got, err := get(t, c, 1)
	require.NoError(t, err, "abort must not touch the committed generation")
	assert.Equal(t, first, got)
}
