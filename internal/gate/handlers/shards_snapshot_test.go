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

// lateBlob holds one node's open until the test releases it, and ignores
// cancellation while it waits. Ignoring it is the point: the hedge cancels on
// return, so a cancellable read would abandon itself instead of landing late.
//
// Once its peers have answered it runs the clock out for them, which is what
// makes the hedge fire. Each step waits for every shard to be between blocks,
// so the only shard the clock can convict is this one.
type lateBlob struct {
	*fakeBlob

	pacer  *shardPacer
	slow   config.NodeID
	index  int
	peers  int
	arrive chan struct{}
	hold   chan struct{}
	landed chan struct{}
}

func (b *lateBlob) Get(ctx context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	if node != b.slow {
		rc, err := b.fakeBlob.Get(ctx, node, req)
		b.arrive <- struct{}{}

		return rc, err
	}
	for range b.peers {
		<-b.arrive
	}
	go b.runOutTheClock()
	<-b.hold
	rc, err := b.fakeBlob.Get(context.WithoutCancel(ctx), node, req)
	close(b.landed)

	return rc, err
}

func (b *lateBlob) runOutTheClock() {
	for {
		select {
		case <-b.hold:
			return
		default:
		}
		b.pacer.awaitPeersIdle(b.index)
		b.pacer.clk.Advance(hedgeProbeInterval)
		time.Sleep(time.Millisecond)
	}
}

var _ BlobClient = (*lateBlob)(nil)

// The hedge abandons the slower shard and reconstructs from parity, leaving
// that read still running. It must not write into the buffers the encoder is
// reconstructing into: a block appearing mid-flight would change the answer
// underneath it, and the object served would be neither shard's bytes.
func TestALateShardDoesNotDisturbTheReadThatGaveUpOnIt(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	want := randomBytes(t, 1<<16)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))
	require.NoError(t, err)

	// The second data shard is released only once the read has returned, so it
	// is still outstanding when the object is served by construction rather than
	// by a delay the machine has to beat.
	pacer := newShardPacer()
	slow := &lateBlob{
		fakeBlob: f.bc, pacer: pacer, slow: place.DataShardNodes[1], index: 1,
		peers:  f.cfg.DataShards - 1,
		arrive: make(chan struct{}, f.cfg.TotalShards()),
		hold:   make(chan struct{}), landed: make(chan struct{}),
	}

	got, degraded, err := readObject(ctx, slow, f.cfg, "b", "k", place, place.Size, 0,
		withClock(pacer.clk), pacer.bind())
	require.NoError(t, err)
	select {
	case <-slow.landed:
		require.Fail(t, "the late shard landed before the read returned, so this proves nothing")
	default:
	}
	assert.Equal(t, 1, degraded, "the abandoned shard should have been rebuilt from parity")
	assert.Equal(t, want, got)

	close(slow.hold)
	<-slow.landed
	// The write follows the read the fake just answered; the race detector is
	// what catches the overlap, this margin is for the value assertion below.
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, want, got, "a shard that arrived after the read returned mutated its buffers")
}
