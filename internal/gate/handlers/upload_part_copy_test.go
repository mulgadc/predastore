package handlers

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A copy source is named in a header rather than the path, so unlike every
// other bucket in these tests it goes through full bucket-name validation and
// cannot be the one-character name the shared fixture uses.
const copyTestBucket = "copy-bucket"

func copyTestCache() *BucketCache {
	return NewBucketCache([]BucketConfig{{Name: copyTestBucket, Region: "ap-southeast-2"}})
}

// copyObjectRequest carries the resolved resource the router would have
// attached, against the copy suite's own bucket.
func copyObjectRequest(method, key, query string) *http.Request {
	r := httptest.NewRequest(method, "/"+copyTestBucket+"/"+key+"?"+query, nil)
	return r.WithContext(WithObject(r.Context(), model.Object{
		Bucket: model.Bucket{Name: copyTestBucket}, Key: key,
	}))
}

// seedObject stores a whole object the way PutObject does, so a copy source
// resolves through the same ARN and placement records production writes.
func (f writeFixture) seedObject(t *testing.T, key string, body []byte) {
	t.Helper()
	ctx := context.Background()

	objectHash := model.ObjectHash(copyTestBucket, key)
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)
	f.publish(t, objectHash, place)
	require.NoError(t, metaPut(ctx, f.mc, model.TableObjects, objectARN(copyTestBucket, key), objectHash[:]))
}

// seedEmptyUpload records an upload with no parts, which is the state a client
// is in when it starts copying parts into one.
func (f writeFixture) seedEmptyUpload(t *testing.T, key, uploadID string) {
	t.Helper()

	var meta bytes.Buffer
	require.NoError(t, gob.NewEncoder(&meta).Encode(model.UploadMetadata{
		UploadID: uploadID, Bucket: copyTestBucket, Key: key, CreatedAt: time.Now(),
	}))
	require.NoError(t, metaPut(context.Background(), f.mc, model.TableMultipart, uploadID, meta.Bytes()))
}

// copyPartRequest builds an UploadPartCopy the way the router dispatches one:
// a part number and upload id in the query, the source in the header.
func copyPartRequest(key, uploadID string, partNumber int, source, sourceRange string) *http.Request {
	r := copyObjectRequest(http.MethodPut, key,
		fmt.Sprintf("partNumber=%d&uploadId=%s", partNumber, uploadID))
	r.Header.Set("X-Amz-Copy-Source", source)
	if sourceRange != "" {
		r.Header.Set("X-Amz-Copy-Source-Range", sourceRange)
	}
	return r
}

// readStoredPart reads a part back through the placement UploadPartCopy
// recorded, which is the only thing that proves the part is retrievable.
func (f writeFixture) readStoredPart(t *testing.T, key, uploadID string, partNumber int) []byte {
	t.Helper()
	ctx := context.Background()

	record, err := metaGet(ctx, f.mc, model.TableObjects, partShardKey(uploadID, partNumber))
	require.NoError(t, err)
	place, err := DecodePlacement(record)
	require.NoError(t, err)

	partKey := partObjectKey(key, uploadID, partNumber)
	got, _, err := readObject(ctx, f.bc, f.cfg, copyTestBucket, partKey, place, place.Size, 0)
	require.NoError(t, err)
	return got
}

// partETag derives the expected ETag through the same hasher the write path
// uses, so the test asserts the value a client would compare against rather
// than a second opinion about how a part ETag is formed.
func partETag(t *testing.T, body []byte) string {
	t.Helper()
	h := model.NewPartETagHasher()
	_, err := h.Write(body)
	require.NoError(t, err)
	return model.PartETagFrom(h)
}

// The docker registry finishes every resumed blob upload by copying the object
// it just completed back in as a part. Without this the push fails on any blob
// written across more than one request.
func TestUploadPartCopyStoresTheWholeSource(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	source := randomBytes(t, 1<<18)
	f.seedObject(t, "src", source)
	f.seedEmptyUpload(t, "dst", "u1")

	w := httptest.NewRecorder()
	UploadPartCopy(f.mc, f.bc, f.ring, copyTestCache(), f.cfg).
		ServeHTTP(w, copyPartRequest("dst", "u1", 1, "/"+copyTestBucket+"/src", ""))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	// The ETag arrives in the body, not the header UploadPart sets: a client
	// that cannot read it here cannot complete the upload.
	var got CopyPartResult
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, partETag(t, source), got.ETag)
	assert.False(t, got.LastModified.IsZero())

	assert.Equal(t, source, f.readStoredPart(t, "dst", "u1", 1))
}

func TestUploadPartCopyHonoursTheSourceRange(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	source := randomBytes(t, 1<<16)
	f.seedObject(t, "src", source)
	f.seedEmptyUpload(t, "dst", "u1")

	const start, end = 1000, 40_999
	w := httptest.NewRecorder()
	UploadPartCopy(f.mc, f.bc, f.ring, copyTestCache(), f.cfg).ServeHTTP(w,
		copyPartRequest("dst", "u1", 2, "/"+copyTestBucket+"/src", fmt.Sprintf("bytes=%d-%d", start, end)))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	want := source[start : end+1]
	assert.Equal(t, want, f.readStoredPart(t, "dst", "u1", 2))

	var got CopyPartResult
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, partETag(t, want), got.ETag)
}

// An upload assembled entirely from copied parts has to read back byte for
// byte, which is the property the registry's blob digest check depends on.
func TestCompletedUploadOfCopiedPartsReadsBack(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	// Completion enforces the 5 MiB minimum on every part but the last, so a
	// copied part is only assemblable at a real part size.
	source := randomBytes(t, 6<<20)
	f.seedObject(t, "src", source)
	f.seedEmptyUpload(t, "dst", "u1")

	const split = int(model.MinPartSize)
	ranges := []string{fmt.Sprintf("bytes=0-%d", split-1), fmt.Sprintf("bytes=%d-%d", split, len(source)-1)}
	for i, spec := range ranges {
		w := httptest.NewRecorder()
		UploadPartCopy(f.mc, f.bc, f.ring, copyTestCache(), f.cfg).
			ServeHTTP(w, copyPartRequest("dst", "u1", i+1, "/"+copyTestBucket+"/src", spec))
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	}

	body, err := xml.Marshal(CompleteMultipartUploadRequest{
		Parts: []MultipartUploadPart{{PartNumber: 1}, {PartNumber: 2}},
	})
	require.NoError(t, err)

	complete := httptest.NewRequest(http.MethodPost, "/"+copyTestBucket+"/dst?uploadId=u1", bytes.NewReader(body))
	complete = complete.WithContext(WithObject(complete.Context(),
		model.Object{Bucket: model.Bucket{Name: copyTestBucket}, Key: "dst"}))

	w := httptest.NewRecorder()
	CompleteMultipartUpload(f.mc, f.bc, f.ring, copyTestCache(), f.cfg).ServeHTTP(w, complete)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	place, size, err := loadPlacement(context.Background(), f.mc, f.ring, f.cfg, copyTestBucket, "dst")
	require.NoError(t, err)
	require.Equal(t, int64(len(source)), size)

	got, _, err := readObject(context.Background(), f.bc, f.cfg, copyTestBucket, "dst", place, size, 0)
	require.NoError(t, err)
	assert.Equal(t, source, got)
}

func TestUploadPartCopyRefusesRequestsItCannotHonour(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	f.seedObject(t, "src", randomBytes(t, 4096))
	f.seedEmptyUpload(t, "dst", "u1")

	tests := []struct {
		name        string
		uploadID    string
		partNumber  int
		source      string
		sourceRange string
		header      string
		status      int
		code        string
	}{
		{
			name: "range past the end of the source", uploadID: "u1", partNumber: 1,
			source: "/" + copyTestBucket + "/src", sourceRange: "bytes=8000-9000",
			status: http.StatusRequestedRangeNotSatisfiable, code: "InvalidRange",
		},
		{
			name: "range missing its last byte", uploadID: "u1", partNumber: 1,
			source: "/" + copyTestBucket + "/src", sourceRange: "bytes=100-",
			status: http.StatusBadRequest, code: "InvalidArgument",
		},
		{
			name: "source that does not exist", uploadID: "u1", partNumber: 1,
			source: "/" + copyTestBucket + "/absent",
			status: http.StatusNotFound, code: "NoSuchKey",
		},
		{
			name: "upload that was never started", uploadID: "u-absent", partNumber: 1,
			source: "/" + copyTestBucket + "/src",
			status: http.StatusNotFound, code: "NoSuchUpload",
		},
		{
			name: "part number out of range", uploadID: "u1", partNumber: 20000,
			source: "/" + copyTestBucket + "/src",
			status: http.StatusBadRequest, code: "InvalidPart",
		},
		{
			name: "versioned source", uploadID: "u1", partNumber: 1,
			source: "/" + copyTestBucket + "/src?versionId=v2",
			status: http.StatusNotImplemented, code: "NotImplemented",
		},
		{
			name: "conditional source", uploadID: "u1", partNumber: 1,
			source: "/" + copyTestBucket + "/src", header: "X-Amz-Copy-Source-If-Match",
			status: http.StatusNotImplemented, code: "NotImplemented",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := copyPartRequest("dst", tc.uploadID, tc.partNumber, tc.source, tc.sourceRange)
			if tc.header != "" {
				r.Header.Set(tc.header, "\"etag\"")
			}

			w := httptest.NewRecorder()
			UploadPartCopy(f.mc, f.bc, f.ring, copyTestCache(), f.cfg).ServeHTTP(w, r)
			require.Equal(t, tc.status, w.Code, w.Body.String())

			var got S3Error
			require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &got))
			assert.Equal(t, tc.code, got.Code)

			// A refused copy must leave no part behind for completion to find.
			_, err := metaGet(context.Background(), f.mc, model.TableParts,
				multipartPartKey(tc.uploadID, tc.partNumber))
			assert.Error(t, err)
		})
	}
}
