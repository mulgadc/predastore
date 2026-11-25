package blob_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/blob/engine"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func heldReq(epoch uint64, body []byte) blob.PutRequest {
	return blob.PutRequest{Key: epochKey(), Index: epochTestShardIndex, Size: int64(len(body)), Epoch: epoch}
}

func shardAt(epoch uint64) blob.CommitRequest {
	return blob.CommitRequest{Key: epochKey(), Index: epochTestShardIndex, Epoch: epoch}
}

// A held put answers Registered before its body and Holding after it, and the
// row it prepares commits like any other.
func TestHeldPutRegistersThenHolds(t *testing.T) {
	c := startBlobNode(t)
	ctx := context.Background()
	body := []byte("held generation")

	p, err := c.OpenHeld(ctx, epochServerNode, heldReq(1, body))
	require.NoError(t, err)
	resp, err := p.Send(ctx, bytes.NewReader(body))
	require.NoError(t, err)
	assert.True(t, resp.Holding, "a held put must confirm the row it prepared is held")

	require.NoError(t, commit(t, c, 1))
	got, err := get(t, c, 1)
	require.NoError(t, err)
	assert.Equal(t, body, got)
}

// An abort that reaches a held writer before its row exists hears Open, and
// the body sent after it prepares nothing. A later abort confirms nothing is
// left, so the caller learns when undo is complete.
func TestAbortDuringAnOpenHeldWriterLeavesNoRow(t *testing.T) {
	c := startBlobNode(t)
	ctx := context.Background()
	body := []byte("aborted before its body")

	p, err := c.OpenHeld(ctx, epochServerNode, heldReq(1, body))
	require.NoError(t, err)

	err = c.Abort(ctx, epochServerNode, shardAt(1))
	require.ErrorIs(t, err, blob.ErrWriterOpen)

	_, err = p.Send(ctx, bytes.NewReader(body))
	require.Error(t, err, "a held write aborted while open must not report a prepare")

	// The client can see its stream cut before the node has ended the writer,
	// so the confirming abort may still find it open for a moment.
	require.Eventually(t, func() bool {
		return c.Abort(ctx, epochServerNode, shardAt(1)) == nil
	}, 5*time.Second, 10*time.Millisecond, "once the writer has ended, abort must answer that nothing is open")
	require.ErrorIs(t, commit(t, c, 1), blob.ErrNotPrepared)
}

// A second held put of the same shard and epoch is refused before it has a
// writer, so it cannot prepare over the first or drop the first one's row.
func TestADuplicateHeldPutLeavesTheFirstIntact(t *testing.T) {
	c := startBlobNode(t)
	ctx := context.Background()
	body := []byte("the first held writer")

	first, err := c.OpenHeld(ctx, epochServerNode, heldReq(1, body))
	require.NoError(t, err)
	_, err = c.OpenHeld(ctx, epochServerNode, heldReq(1, body))
	require.Error(t, err, "a second held put of an open shard must be refused")
	require.NotErrorIs(t, err, blob.ErrHoldUnsupported, "the refusal must be answered, not timed out")

	resp, err := first.Send(ctx, bytes.NewReader(body))
	require.NoError(t, err)
	assert.True(t, resp.Holding)
	require.NoError(t, commit(t, c, 1))
	got, err := get(t, c, 1)
	require.NoError(t, err)
	assert.Equal(t, body, got)
}

// gatedStore holds a held writer's creation until released, as a node slow to
// register does. Its Abort always fails, as a node that crashes before any
// abort lands, so only a writer that prepares nothing leaves no row.
type gatedStore struct {
	*engine.Store

	gate  chan struct{}
	ended chan string
}

func (s *gatedStore) AppendHeld(key [32]byte, index uint32, size int64, epoch uint64) (engine.Writer, error) {
	<-s.gate
	w, err := s.Store.AppendHeld(key, index, size, epoch)
	if err != nil {
		return nil, err
	}
	return &endingWriter{Writer: w, ended: s.ended}, nil
}

func (s *gatedStore) Abort([32]byte, uint32, uint64) error {
	return errors.New("node crashed before the abort")
}

// endingWriter reports how the server ended a writer.
type endingWriter struct {
	engine.Writer

	ended chan string
}

func (w *endingWriter) Close() error {
	err := w.Writer.Close()
	w.ended <- "close"
	return err
}

func (w *endingWriter) Discard() error {
	err := w.Writer.Discard()
	w.ended <- "discard"
	return err
}

// A node that registers after the gate stops waiting gets no body, and its
// writer prepares no row at all, so nothing is left even if no abort ever
// lands. The same wait is what a node that predates held writes meets.
func TestLateRegistrationGetsNoBody(t *testing.T) {
	es, err := engine.Open(t.TempDir(), engine.WithAEAD(storetest.TestAEAD()), engine.WithFreeSpaceWatermark(0, 0))
	require.NoError(t, err)
	t.Cleanup(func() { _ = es.Close() })
	store := &gatedStore{Store: es, gate: make(chan struct{}), ended: make(chan string, 4)}
	c := startStoreNodeWithin(t, store, 200*time.Millisecond)
	ctx := context.Background()
	body := []byte("registered too late")

	_, err = c.OpenHeld(ctx, 1, heldReq(1, body))
	require.ErrorIs(t, err, blob.ErrHoldUnsupported, "OpenHeld must fail when the node does not register")

	close(store.gate)
	select {
	case how := <-store.ended:
		assert.Equal(t, "discard", how, "a writer with no body must be discarded, not prepared")
	case <-time.After(5 * time.Second):
		t.Fatal("the late writer never ended")
	}
	_, err = c.Commit(ctx, 1, blob.CommitRequest{Key: epochKey(), Index: epochTestShardIndex, Epoch: 1})
	require.ErrorIs(t, err, blob.ErrNotPrepared, "a late writer must leave no prepared row")
}

// An abort cuts off a held writer whose body has stalled, so the writer ends
// at once and a later abort confirms nothing is left.
func TestAbortCutsOffAStalledHeldWriter(t *testing.T) {
	c := startBlobNode(t)
	ctx := context.Background()
	body := []byte("a body that never arrives")

	p, err := c.OpenHeld(ctx, epochServerNode, heldReq(1, body))
	require.NoError(t, err)
	t.Cleanup(p.Abandon)

	require.ErrorIs(t, c.Abort(ctx, epochServerNode, shardAt(1)), blob.ErrWriterOpen)
	require.Eventually(t, func() bool {
		return c.Abort(ctx, epochServerNode, shardAt(1)) == nil
	}, 5*time.Second, 10*time.Millisecond, "the stalled writer must end once aborted")
	require.ErrorIs(t, commit(t, c, 1), blob.ErrNotPrepared)
}

// A get that names a retained generation at a position whose live row is gone
// serves that generation, rather than trying to commit it.
func TestGetServesARetainedEpochWithNoLiveRow(t *testing.T) {
	c := startBlobNode(t)
	ctx := context.Background()
	first := []byte("generation one")

	put(t, c, 1, first)
	require.NoError(t, commit(t, c, 1))
	put(t, c, 2, []byte("generation two"))
	require.NoError(t, commit(t, c, 2))

	resp, err := c.DeleteEpoch(ctx, epochServerNode, shardAt(2))
	require.NoError(t, err)
	require.True(t, resp.Deleted)

	got, err := get(t, c, 1)
	require.NoError(t, err)
	assert.Equal(t, first, got)
}

// A get for an epoch at a position with no live row is a record whose writer
// has not committed, so the node commits the held row and serves it.
func TestGetOfANewKeyCommitsTheNamedHeldEpoch(t *testing.T) {
	c := startBlobNode(t)
	ctx := context.Background()
	body := []byte("first generation, never committed by its writer")

	p, err := c.OpenHeld(ctx, epochServerNode, heldReq(5, body))
	require.NoError(t, err)
	_, err = p.Send(ctx, bytes.NewReader(body))
	require.NoError(t, err)

	got, err := get(t, c, 5)
	require.NoError(t, err)
	assert.Equal(t, body, got)

	stat, err := c.Stat(ctx, epochServerNode, blob.StatRequest{Key: epochKey(), Index: epochTestShardIndex})
	require.NoError(t, err)
	assert.Equal(t, uint64(5), stat.Epoch, "the get must have committed the named epoch")
}

// A get for an epoch nothing prepared commits nothing, and leaves a prepared
// row at another epoch for its own writer to decide.
func TestGetOfANewKeyAtAnUnpreparedEpochCommitsNothing(t *testing.T) {
	c := startBlobNode(t)
	ctx := context.Background()
	body := []byte("prepared at another epoch")

	p, err := c.OpenHeld(ctx, epochServerNode, heldReq(5, body))
	require.NoError(t, err)
	_, err = p.Send(ctx, bytes.NewReader(body))
	require.NoError(t, err)

	_, err = get(t, c, 6)
	require.ErrorIs(t, err, blob.ErrNotFound)

	_, err = c.Stat(ctx, epochServerNode, blob.StatRequest{Key: epochKey(), Index: epochTestShardIndex})
	require.ErrorIs(t, err, blob.ErrNotFound, "a get for an unprepared epoch must publish nothing")
	require.NoError(t, commit(t, c, 5), "the other epoch must stay prepared for its writer")
}

// DeleteEpoch over the wire removes the named generation and refuses epoch
// zero, which would match a legacy row.
func TestDeleteEpochRemovesOnlyTheNamedEpochOverTheWire(t *testing.T) {
	c := startBlobNode(t)
	ctx := context.Background()

	put(t, c, 1, []byte("generation one"))
	require.NoError(t, commit(t, c, 1))

	resp, err := c.DeleteEpoch(ctx, epochServerNode, shardAt(2))
	require.NoError(t, err)
	assert.False(t, resp.Deleted, "a delete of another epoch must remove nothing")
	got, err := get(t, c, 1)
	require.NoError(t, err)
	assert.Equal(t, []byte("generation one"), got)

	resp, err = c.DeleteEpoch(ctx, epochServerNode, shardAt(1))
	require.NoError(t, err)
	assert.True(t, resp.Deleted)
	_, err = get(t, c, 0)
	require.ErrorIs(t, err, blob.ErrNotFound)

	_, err = c.DeleteEpoch(ctx, epochServerNode, shardAt(0))
	require.Error(t, err, "epoch zero must be refused")
}
