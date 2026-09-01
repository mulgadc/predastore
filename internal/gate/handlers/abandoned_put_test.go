package handlers

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// abandonedFixture is a write fixture with one shard position on a node that
// prepares the shard and then does not report in time, which is what a drive
// with late completion interrupts looks like to the write path.
func abandonedFixture(t *testing.T, data, parity int, slow uint32) writeFixture {
	t.Helper()

	f := newWriteFixture(data, parity)
	f.cfg.DegradedWrites = true
	f.bc.abandonPutOn = func(index uint32) bool { return index == slow }

	return f
}

// A node that takes the body and goes quiet has not refused the write.
// Counting it missing is what made a drive that was only slow look like a
// cluster losing redundancy on every object.
func TestAnAbandonedPutIsNotCountedMissing(t *testing.T) {
	t.Parallel()

	f := abandonedFixture(t, 2, 1, 1)
	want := randomBytes(t, 1<<16)

	ctx := context.Background()
	objectHash := model.ObjectHash("b", "k")
	place, written, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(len(want)))

	require.NoError(t, err)
	assert.Equal(t, []int{1}, written.ambiguous, "the position has to be recorded as unsettled")
	assert.Empty(t, written.missing, "nothing was refused, so nothing is missing")
	assert.False(t, written.degraded(), "a shard that is present is not lost redundancy")

	// The commit the fixture issues after the record settles it, so the object
	// reads back at full width rather than being reconstructed from parity.
	got, _, err := readObject(ctx, f.bc, f.cfg, "b", "k", place, place.Size, 0)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

// The record keeps naming the node the shard was aimed at, which is what lets
// a read find it there and complete the commit itself.
func TestAnAbandonedPutKeepsItsHolderInTheRecord(t *testing.T) {
	t.Parallel()

	f := abandonedFixture(t, 2, 1, 2)
	body := randomBytes(t, 1<<15)

	objectHash := model.ObjectHash("b", "k")
	place, written, err := f.write(context.Background(), objectHash,
		bytes.NewReader(body), int64(len(body)))

	require.NoError(t, err)
	require.Equal(t, []int{2}, written.ambiguous)
	assert.Equal(t, place.AllNodes()[2], written.holders[2],
		"an unsettled position must still point at the node that has the shard")
}

// The commit that settles an ambiguous position has to actually be sent, or
// the shard stays prepared and invisible until some later read stumbles on it.
func TestAnAbandonedPutIsCommittedNotSkipped(t *testing.T) {
	t.Parallel()

	f := abandonedFixture(t, 2, 1, 0)
	body := randomBytes(t, 1<<15)

	_, written, err := f.write(context.Background(), model.ObjectHash("b", "k"),
		bytes.NewReader(body), int64(len(body)))

	require.NoError(t, err)
	require.Equal(t, []int{0}, written.ambiguous)
	assert.Equal(t, int64(f.cfg.TotalShards()), f.bc.commitCalls.Load(),
		"every position, settled or not, has to be committed")
}

// A write acknowledged to the client must not claim redundancy it has not
// proved, so an unsettled position still says nothing in the degraded header.
func TestPutObjectOmitsTheDegradedHeaderForAnAbandonedPut(t *testing.T) {
	t.Parallel()

	f := abandonedFixture(t, 2, 1, 1)
	w := httptest.NewRecorder()

	PutObject(f.mc, f.bc, f.ring, testCache(), f.cfg).
		ServeHTTP(w, objectPut("k", randomBytes(t, 1<<15)))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get(degradedWriteHeader),
		"a shard that turned out to be present is not a degraded write")
}
