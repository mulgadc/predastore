package handlers

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lateBlob answers for one node only after a delay, and ignores cancellation
// while it waits. Ignoring it is the point: the hedge cancels on return, so a
// cancellable read would abandon itself instead of landing late.
type lateBlob struct {
	*fakeBlob

	slow   config.NodeID
	delay  time.Duration
	landed chan struct{}
}

func (b *lateBlob) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	if node != b.slow {
		return b.fakeBlob.Get(ctx, node, req)
	}
	time.Sleep(b.delay)
	rc, err := b.fakeBlob.Get(context.WithoutCancel(ctx), node, req)
	close(b.landed)
	return rc, err
}

var _ BlobClient = (*lateBlob)(nil)

// The hedge returns once enough shards have landed, leaving the slower reads
// running. They must not write into the slice the caller is reconstructing
// from: the encoder decides what to rebuild by which entries are nil, so a
// shard appearing mid-flight changes that answer underneath it.
func TestShardBytesSnapshotsAgainstALateShard(t *testing.T) {
	t.Parallel()

	const late = 750 * time.Millisecond

	f := newWriteFixture(2, 1)
	want := randomBytes(t, 1<<16)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
	require.NoError(t, err)

	// The second data shard arrives long after the parity shard has already
	// made the read answerable, so it is still outstanding on return.
	slow := &lateBlob{fakeBlob: f.bc, slow: place.DataShardNodes[1], delay: late, landed: make(chan struct{})}

	start := time.Now()
	shards, _, err := shardBytes(ctx, slow, objectHash, place, 0)
	require.NoError(t, err)
	require.Less(t, time.Since(start), late,
		"the hedge must return before the late shard lands, or this proves nothing")

	missingOnReturn := missingShards(shards, len(place.DataShardNodes))
	require.Positive(t, missingOnReturn, "the late shard should be absent on return")

	<-slow.landed
	// The write follows the read the fake just answered; the race detector is
	// what catches the overlap, this margin is for the value assertion below.
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, missingOnReturn, missingShards(shards, len(place.DataShardNodes)),
		"a shard that arrived after the read returned mutated the caller's slice")
}
