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

const testBucket = "b"

// testCache declares the bucket in configuration, which is the cheaper of the
// two ways a bucket exists and needs no encoded global-state row.
func testCache() *BucketCache {
	return NewBucketCache([]BucketConfig{{Name: testBucket, Region: "ap-southeast-2"}})
}

// seedUpload records an upload and its parts the way CreateMultipartUpload and
// UploadPart do, so the handlers under test read what production wrote.
func (f writeFixture) seedUpload(t *testing.T, key, uploadID string, sizes map[int]int64) {
	t.Helper()
	ctx := context.Background()

	var meta bytes.Buffer
	require.NoError(t, gob.NewEncoder(&meta).Encode(model.UploadMetadata{
		UploadID: uploadID, Bucket: testBucket, Key: key, CreatedAt: time.Now(),
	}))
	require.NoError(t, metaPut(ctx, f.mc, model.TableMultipart, uploadID, meta.Bytes()))

	for partNumber, size := range sizes {
		f.storePart(t, testBucket, key, uploadID, partNumber, randomBytes(t, int(size)))

		var partBuf bytes.Buffer
		require.NoError(t, gob.NewEncoder(&partBuf).Encode(model.PartMetadata{
			PartNumber:   partNumber,
			Size:         size,
			ETag:         fmt.Sprintf("\"%032d\"", partNumber),
			LastModified: time.Now(),
		}))
		require.NoError(t, metaPut(ctx, f.mc, model.TableParts,
			multipartPartKey(uploadID, partNumber), partBuf.Bytes()))
	}
}

// objectRequest builds a request carrying the resolved resource the router
// would have attached.
func objectRequest(method, key, query string) *http.Request {
	r := httptest.NewRequest(method, "/"+testBucket+"/"+key+"?"+query, nil)
	return r.WithContext(WithObject(r.Context(), model.Object{
		Bucket: model.Bucket{Name: testBucket}, Key: key,
	}))
}

// Clients enumerate an upload's parts before completing it. Without this the
// route fell through to GetObject and they saw no parts at all.
func TestListPartsReturnsTheStoredParts(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(1, 0)
	f.seedUpload(t, "k", "u1", map[int]int64{1: 5 << 20, 2: 1 << 20})

	w := httptest.NewRecorder()
	ListParts(f.mc, testCache()).ServeHTTP(w, objectRequest(http.MethodGet, "k", "uploadId=u1"))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var got ListPartsResult
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, testBucket, got.Bucket)
	assert.Equal(t, "k", got.Key)
	assert.Equal(t, "u1", got.UploadId)
	assert.False(t, got.IsTruncated)

	require.Len(t, got.Parts, 2)
	assert.Equal(t, 1, got.Parts[0].PartNumber)
	assert.Equal(t, int64(5<<20), got.Parts[0].Size)
	assert.Equal(t, 2, got.Parts[1].PartNumber)
	assert.Equal(t, 2, got.NextPartNumberMarker)
}

// Paging is what makes the 10,000-part limit listable, so the marker has to be
// exclusive and truncation has to be reported.
func TestListPartsPagesFromTheMarker(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(1, 0)
	f.seedUpload(t, "k", "u1", map[int]int64{1: 1 << 10, 2: 1 << 10, 3: 1 << 10})

	w := httptest.NewRecorder()
	ListParts(f.mc, testCache()).ServeHTTP(w,
		objectRequest(http.MethodGet, "k", "uploadId=u1&part-number-marker=1&max-parts=1"))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var got ListPartsResult
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &got))
	require.Len(t, got.Parts, 1)
	assert.Equal(t, 2, got.Parts[0].PartNumber, "the marker is exclusive")
	assert.True(t, got.IsTruncated)
	assert.Equal(t, 2, got.NextPartNumberMarker)
}

func TestListPartsUnknownUploadIsAnError(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(1, 0)

	w := httptest.NewRecorder()
	ListParts(f.mc, testCache()).ServeHTTP(w, objectRequest(http.MethodGet, "k", "uploadId=missing"))

	assert.GreaterOrEqual(t, w.Code, 400)
}

// completionBody is the XML a client posts to finish an upload. An empty parts
// list is what MinIO-targeting clients send when they want every stored part.
func completionBody(parts ...int) string {
	body := `<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`
	for _, n := range parts {
		body += fmt.Sprintf(`<Part><PartNumber>%d</PartNumber><ETag>"%032d"</ETag></Part>`, n, n)
	}
	return body + `</CompleteMultipartUpload>`
}

func completionRequest(key, uploadID, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost,
		"/"+testBucket+"/"+key+"?uploadId="+uploadID, bytes.NewReader([]byte(body)))
	return r.WithContext(WithObject(r.Context(), model.Object{
		Bucket: model.Bucket{Name: testBucket}, Key: key,
	}))
}

// Warp and other MinIO-targeting clients complete with no part list at all and
// expect every uploaded part to be used. AWS rejects that, so it is a
// deliberate extension rather than something the spec asks for.
func TestCompleteMultipartUploadWithNoPartsUsesEveryStoredPart(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(1, 0)
	f.seedUpload(t, "k", "u1", map[int]int64{1: 5 << 20, 2: 1 << 20})

	w := httptest.NewRecorder()
	CompleteMultipartUpload(f.mc, f.bc, f.ring, testCache(), f.cfg).
		ServeHTTP(w, completionRequest("k", "u1", completionBody()))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var got CompleteMultipartUploadResult
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "k", got.Key)
	// Two parts assembled, which the ETag suffix records.
	assert.Contains(t, got.ETag, "-2")

	place, _, err := loadPlacement(context.Background(), f.mc, f.ring, f.cfg, testBucket, "k")
	require.NoError(t, err)
	assert.Equal(t, int64(6<<20), place.Size, "the object is the parts concatenated")
}

// A client that does name its parts is still held to exactly those, so the
// extension cannot mask a client that sent the wrong list.
func TestCompleteMultipartUploadHonoursAGivenPartList(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(1, 0)
	f.seedUpload(t, "k", "u1", map[int]int64{1: 5 << 20, 2: 1 << 20})

	w := httptest.NewRecorder()
	CompleteMultipartUpload(f.mc, f.bc, f.ring, testCache(), f.cfg).
		ServeHTTP(w, completionRequest("k", "u1", completionBody(3)))

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "Part 3 does not exist")
}
