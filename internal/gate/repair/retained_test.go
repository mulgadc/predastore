package repair

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A node can hold the record's generation retained under a newer live one
// whose writer has not published. A repair pass leaves that generation in
// place, even on a node under another key that marks nothing, so the shard
// still reads when the cluster's key returns.
func TestARepairPassLeavesARetainedGenerationInPlace(t *testing.T) {
	f := newS3Fixture(t, 2, 1, 4)
	target := f.place.DataShardNodes[0]
	ctx := context.Background()
	newer := bytes.Repeat([]byte("n"), len(f.shards[0]))
	_, err := f.nodes.client.Put(ctx, target, blob.PutRequest{Key: f.hash, Index: 0, Size: int64(len(newer)), Epoch: f.place.WriteEpoch + 1}, bytes.NewReader(newer))
	require.NoError(t, err)
	_, err = f.nodes.client.Commit(ctx, target, blob.CommitRequest{Key: f.hash, Index: 0, Epoch: f.place.WriteEpoch + 1})
	require.NoError(t, err)

	wrong, err := masterkey.NewAEAD(bytes.Repeat([]byte{0x43}, 32))
	require.NoError(t, err)
	f.nodes.restart(target, wrong)

	blobs := &countingPuts{BlobClient: f.nodes.client}
	svc, err := New(Config{
		Nodes: []config.NodeID{target}, Ring: f.c.ring, Meta: f.mc, Blob: blobs,
		DataShards: 2, ParityShards: 1, Workers: 1,
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return svc.Pass(ctx) == nil }, 5*time.Second, 20*time.Millisecond)
	assert.Zero(t, blobs.puts.Load(), "a generation the node still holds is not rebuilt over")

	f.nodes.restart(target, storetest.TestAEAD())
	var got []byte
	require.Eventually(t, func() bool {
		got, err = f.nodes.readShard(target, f.hash, 0, f.place.WriteEpoch)
		return err == nil
	}, 5*time.Second, 20*time.Millisecond, "under the cluster's key the node serves the retained generation")
	assert.Equal(t, f.shards[0], got)
}
