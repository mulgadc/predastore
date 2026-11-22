package handlers

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ambiguousSwapMeta applies a swap and then reports that it failed, which is a
// response lost after the log took the command. Raft reports a leadership lost
// while committing exactly this way: the entry is in the log and the new leader
// can still commit it.
type ambiguousSwapMeta struct {
	*fakeMeta

	swaps atomic.Int64
}

var _ MetaClient = (*ambiguousSwapMeta)(nil)

func newAmbiguousSwapMeta(mc *fakeMeta) *ambiguousSwapMeta {
	return &ambiguousSwapMeta{fakeMeta: mc}
}

func (m *ambiguousSwapMeta) Swap(ctx context.Context, key string, value []byte) ([]byte, error) {
	if _, err := m.fakeMeta.Swap(ctx, key, value); err != nil {
		return nil, err
	}
	m.swaps.Add(1)

	return nil, errors.New("raft apply failed: leadership lost while committing log")
}

// wholeCopyRequest builds the CopyObject the router dispatches when the PUT
// carries no part number: a whole-object copy within the copy suite's bucket.
func wholeCopyRequest(srcKey, destKey string) *http.Request {
	r := copyObjectRequest(http.MethodPut, destKey, "")
	r.Header.Set("X-Amz-Copy-Source", "/"+copyTestBucket+"/"+srcKey)

	return r
}

// A writer that cannot tell whether its placement swap applied must not destroy
// the generation that swap may have published. CopyObject and
// CompleteMultipartUpload published the record before committing their shards,
// so the error path aborted the prepared shards the live record had just been
// pointed at, and an acknowledged destination object stopped reading.
//
// Verifies: ADR-0003:S7.
func TestS7_AnAmbiguousSwapDoesNotDestroyACopyDestination(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	source := randomBytes(t, 1<<16)
	settled := randomBytes(t, 1<<16)

	f.seedObject(t, "src", source)
	f.seedObject(t, "dst", settled)

	amb := newAmbiguousSwapMeta(f.mc)
	w := httptest.NewRecorder()
	CopyObject(amb, f.bc, f.ring, copyTestCache(), f.cfg).ServeHTTP(w, wholeCopyRequest("src", "dst"))
	require.Equal(t, http.StatusInternalServerError, w.Code,
		"a swap whose outcome is unknown has to be reported as a failure: %s", w.Body.String())
	require.Equal(t, int64(1), amb.swaps.Load(), "the copy never reached its placement swap")

	// The record may name either generation. Both have to be readable, because
	// the client was acknowledged for one and the cluster may have published
	// the other.
	w = httptest.NewRecorder()
	GetObject(f.mc, f.bc, f.ring, copyTestCache(), f.cfg).
		ServeHTTP(w, copyObjectRequest(http.MethodGet, "dst", ""))
	require.Equal(t, http.StatusOK, w.Code,
		"an ambiguous swap destroyed the destination object: %s", w.Body.String())
	got := w.Body.Bytes()
	assert.True(t, bytes.Equal(got, settled) || bytes.Equal(got, source),
		"the destination read back as neither generation, %d bytes", len(got))
}

// Completion has the same shape: it publishes the assembled object's record and
// then commits the shards it names.
//
// Verifies: ADR-0003:S7.
func TestS7_AnAmbiguousSwapDoesNotDestroyACompletedUpload(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	settled := randomBytes(t, 1<<16)

	w := httptest.NewRecorder()
	PutObject(f.mc, f.bc, f.ring, testCache(), f.cfg).ServeHTTP(w, objectPut("k", settled))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	f.seedUpload(t, "k", "u1", map[int]int64{1: 5 << 20, 2: 1 << 20})

	amb := newAmbiguousSwapMeta(f.mc)
	w = httptest.NewRecorder()
	CompleteMultipartUpload(amb, f.bc, f.ring, testCache(), f.cfg).
		ServeHTTP(w, completionRequest("k", "u1", completionBody()))
	require.Equal(t, http.StatusInternalServerError, w.Code,
		"a swap whose outcome is unknown has to be reported as a failure")
	require.Equal(t, int64(1), amb.swaps.Load(), "completion never reached its placement swap")

	w = httptest.NewRecorder()
	GetObject(f.mc, f.bc, f.ring, testCache(), f.cfg).ServeHTTP(w, objectGet("k"))
	require.Equal(t, http.StatusOK, w.Code,
		"an ambiguous swap destroyed the object the completion replaced: %s", w.Body.String())
	assert.Contains(t, []int{6 << 20, len(settled)}, w.Body.Len(),
		"the object read back as neither generation")
}
