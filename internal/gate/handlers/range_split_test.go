// A range that crosses a block boundary is still a few runs of the data shards'
// own bytes. These pin that such a range costs the nodes its own length, not
// the stripe it falls in, and that the bytes are still the object's.

package handlers

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getRange serves one ranged GET and reports the body and what the nodes
// delivered for it.
func getRange(t *testing.T, f writeFixture, start, end int64) (*httptest.ResponseRecorder, int64) {
	t.Helper()
	req := objectRequest(http.MethodGet, "k", "")
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))

	f.bc.getBytes.Store(0)
	w := httptest.NewRecorder()
	GetObject(f.mc, f.bc, f.ring, testCache(), f.cfg).ServeHTTP(w, req)

	return w, f.bc.getBytes.Load()
}

func publishedObject(t testing.TB, f writeFixture, size int64) []byte {
	t.Helper()
	body := make([]byte, size)
	_, err := rand.Read(body)
	require.NoError(t, err)
	objectHash := model.ObjectHash("b", "k")
	place, _, err := f.write(context.Background(), objectHash, bytes.NewReader(body), size)
	require.NoError(t, err)
	f.publish(t, objectHash, place)

	return body
}

// Before, a 4 KiB range across a block boundary read every data shard from the
// start of its stripe: 8 MiB from the nodes on this object for 4 KiB served.
func TestARangeAcrossABlockReadsOnlyItsOwnBytes(t *testing.T) {
	t.Parallel()

	const size = 3*streamBlockSize + 7919
	f := newWriteFixture(2, 1)
	body := publishedObject(t, f, size)

	start, end := int64(streamBlockSize-2048), int64(streamBlockSize+2047)
	w, read := getRange(t, f, start, end)

	require.Equal(t, http.StatusPartialContent, w.Code)
	require.Equal(t, body[start:end+1], w.Body.Bytes())
	assert.Equal(t, end-start+1, read, "a %d byte range read %d bytes from the nodes", end-start+1, read)
}

// viperblock's chunk is a 10-byte header and 1,024 blocks of 4 KiB, so under
// RS(2,1) the shard boundary falls 5 bytes before the end of block 511. Every
// read of that block crossed it.
func TestViperblockBlock511ReadsOnlyItsOwnBytes(t *testing.T) {
	t.Parallel()

	const size = 10 + 1024*4096
	f := newWriteFixture(2, 1)
	body := publishedObject(t, f, size)

	start := int64(10 + 511*4096)
	end := start + 4095
	w, read := getRange(t, f, start, end)

	require.Equal(t, http.StatusPartialContent, w.Code)
	require.Equal(t, body[start:end+1], w.Body.Bytes())
	assert.Equal(t, int64(4096), read, "block 511 read %d bytes from the nodes", read)
}

// A node that does not answer must not turn a crossing range into an error:
// the piece it held comes from parity, or the stripe reader serves the range.
func TestARangeAcrossABlockSurvivesALostShard(t *testing.T) {
	t.Parallel()

	const size = 10 + 1024*4096
	for _, lost := range []uint32{0, 1, 2} {
		t.Run(fmt.Sprintf("shard %d lost", lost), func(t *testing.T) {
			t.Parallel()
			f := newWriteFixture(2, 1)
			body := publishedObject(t, f, size)
			f.bc.failGetOn = func(index uint32) bool { return index == lost }

			start := int64(10 + 511*4096)
			end := start + 4095
			w, _ := getRange(t, f, start, end)

			require.Equal(t, http.StatusPartialContent, w.Code)
			assert.Equal(t, body[start:end+1], w.Body.Bytes())
		})
	}
}
