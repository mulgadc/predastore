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

// The hedge abandons the slower shard and reconstructs from parity, leaving
// that read still running. It must not write into the buffers the encoder is
// reconstructing into: a block appearing mid-flight would change the answer
// underneath it, and the object served would be neither shard's bytes.
func TestALateShardDoesNotDisturbTheReadThatGaveUpOnIt(t *testing.T) {
	t.Parallel()

	const late = 750 * time.Millisecond

	f := newWriteFixture(2, 1)
	want := randomBytes(t, 1<<16)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
	require.NoError(t, err)

	// The second data shard arrives long after parity has already made the read
	// answerable, so it is still outstanding when the object is served.
	slow := &lateBlob{fakeBlob: f.bc, slow: place.DataShardNodes[1], delay: late, landed: make(chan struct{})}

	start := time.Now()
	got, degraded, err := readObject(ctx, slow, f.cfg, "b", "k", place, place.Size, 0)
	require.NoError(t, err)
	require.Less(t, time.Since(start), late,
		"the hedge must return before the late shard lands, or this proves nothing")
	assert.Equal(t, 1, degraded, "the abandoned shard should have been rebuilt from parity")
	assert.Equal(t, want, got)

	<-slow.landed
	// The write follows the read the fake just answered; the race detector is
	// what catches the overlap, this margin is for the value assertion below.
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, want, got, "a shard that arrived after the read returned mutated its buffers")
}
