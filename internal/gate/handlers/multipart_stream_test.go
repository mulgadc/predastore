package handlers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// storePart writes one part the way UploadPart does, so completion reads it
// back through the same keys production uses.
func (f writeFixture) storePart(t *testing.T, bucket, key, uploadID string, partNumber int, data []byte) {
	t.Helper()
	ctx := context.Background()

	partKey := partObjectKey(key, uploadID, partNumber)
	objectHash := model.ObjectHash(bucket, partKey)

	place, _, err := f.write(ctx, objectHash, bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	record, err := EncodePlacement(place)
	require.NoError(t, err)
	require.NoError(t, metaPut(ctx, f.mc, model.TableObjects, partShardKey(uploadID, partNumber), record))
}

// completedParts names parts 1..n, which is the order S3 requires.
func completedParts(n int) []model.CompletedPart {
	parts := make([]model.CompletedPart, n)
	for i := range parts {
		parts[i] = model.CompletedPart{PartNumber: i + 1}
	}
	return parts
}

// Completion has to hand the write path the parts concatenated in part-number
// order, whatever order the fetches happen to finish in.
func TestStreamPartsConcatenatesInOrder(t *testing.T) {
	t.Parallel()

	const (
		parts    = 12
		partSize = 64 << 10
	)
	f := newWriteFixture(1, 0)

	var want []byte
	for i := 1; i <= parts; i++ {
		data := randomBytes(t, partSize)
		f.storePart(t, "b", "k", "upload-1", i, data)
		want = append(want, data...)
	}

	r := streamParts(context.Background(), f.mc, f.bc, nil, f.cfg, "b", "k", "upload-1", completedParts(parts))
	defer r.Close()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

// The whole point of streaming completion is that the object is never held at
// once. This bounds what the pipeline holds while the reader is slow.
func TestStreamPartsHoldsOnlyABoundedWindow(t *testing.T) {
	t.Parallel()

	const parts = maxParallelPartFetches * 4
	f := newWriteFixture(1, 0)
	for i := 1; i <= parts; i++ {
		f.storePart(t, "b", "k", "upload-1", i, randomBytes(t, 1024))
	}

	counting := &countingGets{BlobClient: f.bc}
	r := streamParts(context.Background(), f.mc, counting, nil, f.cfg, "b", "k", "upload-1", completedParts(parts))
	defer r.Close()

	// Read one part's worth, then let any runnable fetch make progress. Only
	// the window may have been fetched: an unbounded pipeline would have
	// pulled every part by now.
	_, err := io.ReadFull(r, make([]byte, 1024))
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	assert.LessOrEqual(t, counting.gets.Load(), int64(maxParallelPartFetches+1),
		"completion fetched ahead without bound")
}

// An abandoned completion must not leave the fetchers running: closing the
// reader is the only signal they get.
func TestStreamPartsCloseStopsTheFetchers(t *testing.T) {
	t.Parallel()

	const parts = maxParallelPartFetches * 4
	f := newWriteFixture(1, 0)
	for i := 1; i <= parts; i++ {
		f.storePart(t, "b", "k", "upload-1", i, randomBytes(t, 1024))
	}

	counting := &countingGets{BlobClient: f.bc}
	r := streamParts(context.Background(), f.mc, counting, nil, f.cfg, "b", "k", "upload-1", completedParts(parts))

	_, err := io.ReadFull(r, make([]byte, 1024))
	require.NoError(t, err)
	require.NoError(t, r.Close())

	time.Sleep(50 * time.Millisecond)
	settled := counting.gets.Load()
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, settled, counting.gets.Load(), "fetches continued after the reader was closed")
}

// A part that cannot be read has to fail the completion rather than silently
// producing a short object, so the error must reach the reader.
func TestStreamPartsSurfacesAFetchError(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(1, 0)
	f.storePart(t, "b", "k", "upload-1", 1, randomBytes(t, 1024))
	// Part 2 is never stored, so its placement lookup fails.

	r := streamParts(context.Background(), f.mc, f.bc, nil, f.cfg, "b", "k", "upload-1", completedParts(2))
	defer r.Close()

	_, err := io.ReadAll(r)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "part not found")
}

// countingGets counts part reads so a test can see how far ahead completion ran.
type countingGets struct {
	BlobClient

	gets atomic.Int64
}

func (c *countingGets) Get(ctx context.Context, id config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	c.gets.Add(1)
	return c.BlobClient.Get(ctx, id, req)
}

// A completed upload must read back byte for byte, at every erasure width.
func TestMultipartAssemblyRoundTrips(t *testing.T) {
	t.Parallel()

	for _, rs := range []struct{ data, parity int }{{1, 0}, {2, 1}} {
		t.Run(fmt.Sprintf("rs-%d-%d", rs.data, rs.parity), func(t *testing.T) {
			t.Parallel()

			const parts = 5
			f := newWriteFixture(rs.data, rs.parity)

			var want []byte
			var finalSize int64
			for i := 1; i <= parts; i++ {
				data := randomBytes(t, 100<<10)
				f.storePart(t, "b", "k", "upload-1", i, data)
				want = append(want, data...)
				finalSize += int64(len(data))
			}

			ctx := context.Background()
			objectHash := model.ObjectHash("b", "k")

			assembled := streamParts(ctx, f.mc, f.bc, nil, f.cfg, "b", "k", "upload-1", completedParts(parts))
			place, _, err := f.write(ctx, objectHash, assembled, finalSize)
			require.NoError(t, err)
			require.NoError(t, assembled.Close())

			got, _, err := readObject(ctx, f.bc, f.cfg, "b", "k", place, place.Size, 0)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}
