package handlers

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// handlerFixture is a gate object surface -- PutObject through CopyObject --
// wired to the same fakeBlob/fakeMeta writeFixture the write-path tests use,
// so a test here proves the whole stack agrees rather than one function's
// return value.
type handlerFixture struct {
	writeFixture

	cache *BucketCache
}

func newHandlerFixture(buckets ...string) handlerFixture {
	return newHandlerFixtureRS(2, 1, buckets...)
}

func newHandlerFixtureRS(dataShards, parityShards int, buckets ...string) handlerFixture {
	entries := make([]BucketConfig, len(buckets))
	for i, b := range buckets {
		entries[i] = BucketConfig{Name: b}
	}
	return handlerFixture{
		writeFixture: newWriteFixture(dataShards, parityShards),
		cache:        NewBucketCache(entries),
	}
}

// objectCtx attaches the resolved bucket and object the way resolveObject
// would, so a handler can be called directly without routing through chi.
func objectCtx(bucket, key string) context.Context {
	ctx := WithBucket(context.Background(), model.Bucket{Name: bucket})
	return WithObject(ctx, model.Object{Bucket: model.Bucket{Name: bucket}, Key: key})
}

func (f handlerFixture) put(bucket, key string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"/"+key, bytes.NewReader(body)).
		WithContext(objectCtx(bucket, key))
	req.ContentLength = int64(len(body))
	rr := httptest.NewRecorder()
	PutObject(f.mc, f.bc, f.ring, f.cache, f.cfg).ServeHTTP(rr, req)
	return rr
}

func (f handlerFixture) head(bucket, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodHead, "/"+bucket+"/"+key, nil).WithContext(objectCtx(bucket, key))
	rr := httptest.NewRecorder()
	HeadObject(f.mc, f.ring, f.cache, f.cfg).ServeHTTP(rr, req)
	return rr
}

func (f handlerFixture) get(bucket, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil).WithContext(objectCtx(bucket, key))
	rr := httptest.NewRecorder()
	GetObject(f.mc, f.bc, f.ring, f.cache, f.cfg).ServeHTTP(rr, req)
	return rr
}

func (f handlerFixture) list(t *testing.T, bucket string) ListObjectsV2 {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/"+bucket, nil).
		WithContext(WithBucket(context.Background(), model.Bucket{Name: bucket}))
	rr := httptest.NewRecorder()
	ListObjects(f.mc, f.cache).ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var result ListObjectsV2
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &result))
	return result
}

func (f handlerFixture) copy(destBucket, destKey, copySource string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPut, "/"+destBucket+"/"+destKey, nil).
		WithContext(objectCtx(destBucket, destKey))
	req.Header.Set("X-Amz-Copy-Source", copySource)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	CopyObject(f.mc, f.bc, f.ring, f.cache, f.cfg).ServeHTTP(rr, req)
	return rr
}

func quotedHex(b []byte) string { return fmt.Sprintf("\"%x\"", b) }

// Single-part PUT of a known body must return the hex MD5 of that body,
// quoted, and HEAD, GET and the listing must all agree with it.
func TestSinglePartPutETagIsTheBodyMD5(t *testing.T) {
	f := newHandlerFixture("bucket")
	body := []byte("the quick brown fox jumps over the lazy dog")
	sum := md5.Sum(body)
	want := quotedHex(sum[:])

	putRR := f.put("bucket", "key", body)
	require.Equal(t, http.StatusOK, putRR.Code, putRR.Body.String())
	assert.Equal(t, want, putRR.Header().Get("ETag"), "PutObject ETag")

	headRR := f.head("bucket", "key")
	require.Equal(t, http.StatusOK, headRR.Code)
	assert.Equal(t, want, headRR.Header().Get("ETag"), "HeadObject ETag")

	getRR := f.get("bucket", "key")
	require.Equal(t, http.StatusOK, getRR.Code)
	assert.Equal(t, want, getRR.Header().Get("ETag"), "GetObject ETag")
	assert.Equal(t, body, getRR.Body.Bytes())

	listing := f.list(t, "bucket")
	require.NotNil(t, listing.Contents)
	require.Len(t, *listing.Contents, 1)
	assert.Equal(t, want, (*listing.Contents)[0].ETag, "ListObjects ETag")
}

// Overwriting a key must change its ETag. The name-derived scheme this
// replaces could never express this, so it is the regression that matters
// most in this set.
func TestOverwritingAKeyChangesItsETag(t *testing.T) {
	f := newHandlerFixture("bucket")

	first := f.put("bucket", "key", []byte("version one"))
	require.Equal(t, http.StatusOK, first.Code)
	etag1 := first.Header().Get("ETag")
	require.NotEmpty(t, etag1)

	second := f.put("bucket", "key", []byte("version two, and it is longer"))
	require.Equal(t, http.StatusOK, second.Code)
	etag2 := second.Header().Get("ETag")
	require.NotEmpty(t, etag2)

	assert.NotEqual(t, etag1, etag2, "overwriting a key must change its ETag")

	sum := md5.Sum([]byte("version two, and it is longer"))
	assert.Equal(t, quotedHex(sum[:]), etag2)

	headRR := f.head("bucket", "key")
	assert.Equal(t, etag2, headRR.Header().Get("ETag"), "HEAD must see the new ETag after an overwrite")
}

// A completed multipart upload must return the composite "-N" ETag, and
// HEAD, GET and the listing must all agree with what CompleteMultipartUpload
// returned -- not recompute it and not omit it.
func TestMultipartUploadETagIsTheCompositeForm(t *testing.T) {
	f := newHandlerFixture("bucket")
	const bucket, key = "bucket", "big-object"

	createRR := httptest.NewRecorder()
	CreateMultipartUpload(f.mc, f.cache).ServeHTTP(createRR,
		httptest.NewRequest(http.MethodPost, "/"+bucket+"/"+key, nil).WithContext(objectCtx(bucket, key)))
	require.Equal(t, http.StatusOK, createRR.Code, createRR.Body.String())
	var created InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(createRR.Body.Bytes(), &created))
	uploadID := created.UploadId
	require.NotEmpty(t, uploadID)

	part1 := bytes.Repeat([]byte{'a'}, int(model.MinPartSize)) // non-last parts must clear the 5MiB floor
	part2 := []byte("the final, short part")

	uploadPart := func(partNumber int, body []byte) string {
		req := httptest.NewRequest(http.MethodPut,
			fmt.Sprintf("/%s/%s?partNumber=%d&uploadId=%s", bucket, key, partNumber, url.QueryEscape(uploadID)),
			bytes.NewReader(body)).WithContext(objectCtx(bucket, key))
		req.ContentLength = int64(len(body))
		rr := httptest.NewRecorder()
		UploadPart(f.mc, f.bc, f.ring, f.cache, f.cfg).ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		return rr.Header().Get("ETag")
	}
	etag1 := uploadPart(1, part1)
	etag2 := uploadPart(2, part2)

	digest := model.CalculateMultipartDigest([]string{etag1, etag2})
	wantETag := fmt.Sprintf("\"%x-2\"", digest)

	completeBody, err := xml.Marshal(CompleteMultipartUploadRequest{Parts: []MultipartUploadPart{
		{PartNumber: 1, ETag: etag1}, {PartNumber: 2, ETag: etag2},
	}})
	require.NoError(t, err)
	completeReq := httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/%s/%s?uploadId=%s", bucket, key, url.QueryEscape(uploadID)), bytes.NewReader(completeBody)).
		WithContext(objectCtx(bucket, key))
	completeRR := httptest.NewRecorder()
	CompleteMultipartUpload(f.mc, f.bc, f.ring, f.cache, f.cfg).ServeHTTP(completeRR, completeReq)
	require.Equal(t, http.StatusOK, completeRR.Code, completeRR.Body.String())

	var completed CompleteMultipartUploadResult
	require.NoError(t, xml.Unmarshal(completeRR.Body.Bytes(), &completed))
	assert.Equal(t, wantETag, completed.ETag, "CompleteMultipartUpload ETag")

	headRR := f.head(bucket, key)
	require.Equal(t, http.StatusOK, headRR.Code)
	assert.Equal(t, wantETag, headRR.Header().Get("ETag"), "HeadObject must agree with CompleteMultipartUpload")

	getRR := f.get(bucket, key)
	require.Equal(t, http.StatusOK, getRR.Code)
	assert.Equal(t, wantETag, getRR.Header().Get("ETag"), "GetObject must agree with CompleteMultipartUpload")
	assert.Equal(t, len(part1)+len(part2), getRR.Body.Len())

	listing := f.list(t, bucket)
	require.NotNil(t, listing.Contents)
	require.Len(t, *listing.Contents, 1)
	assert.Equal(t, wantETag, (*listing.Contents)[0].ETag, "ListObjects must agree with CompleteMultipartUpload")
}

// rawLegacyPlacement builds a pre-digest placement record by hand. Version 1
// carries neither a block size nor a digest; version 2 adds the block size.
func rawLegacyPlacement(version byte, size int64, epoch uint64, blockSize int64, nodes []config.NodeID) []byte {
	b := []byte{placementMagic, version, byte(len(nodes))}
	b = binary.BigEndian.AppendUint64(b, uint64(size))
	b = binary.BigEndian.AppendUint64(b, epoch)
	if version >= 0x02 {
		b = binary.BigEndian.AppendUint64(b, uint64(blockSize))
	}
	for _, id := range nodes {
		b = binary.AppendUvarint(b, uint64(id))
	}
	return b
}

// A placement record written before content digests existed is not migrated:
// it is refused, so a store holding one is rebuilt rather than quietly serving
// objects whose ETag nothing can produce.
func TestObjectWithLegacyPlacementRecordIsRefused(t *testing.T) {
	// RS(1,0): a legacy record names exactly one node, so the ring width here
	// must match rather than triggering the shard-count mismatch a real
	// RS(2,1) placement would.
	f := newHandlerFixtureRS(1, 0, "bucket")
	const bucket = "bucket"

	seed := func(key string, raw []byte, body []byte, epoch uint64) {
		hash := model.ObjectHash(bucket, key)
		require.NoError(t, f.mc.Put(context.Background(), TableKey(model.TableObjects, string(hash[:])), raw))
		require.NoError(t, f.mc.Put(context.Background(), TableKey(model.TableObjects, objectARN(bucket, key)), hash[:]))
		f.bc.shards[shardID{key: hash, index: 0}] = fakeShard{data: body, epoch: epoch}
	}

	v1Body := []byte("a version one object")
	seed("legacy-v1", rawLegacyPlacement(0x01, int64(len(v1Body)), 0xdeadbeefcafef00d, 0, []config.NodeID{1}),
		v1Body, 0xdeadbeefcafef00d)

	v2Body := []byte("a version two object")
	seed("legacy-v2", rawLegacyPlacement(0x02, int64(len(v2Body)), 12345, int64(len(v2Body)), []config.NodeID{1}),
		v2Body, 12345)

	for _, tt := range []struct {
		name string
		key  string
	}{
		{"version 1", "legacy-v1"},
		{"version 2", "legacy-v2"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			headRR := f.head(bucket, tt.key)
			assert.NotEqual(t, http.StatusOK, headRR.Code, "HEAD must refuse a legacy record")

			getRR := f.get(bucket, tt.key)
			assert.NotEqual(t, http.StatusOK, getRR.Code, "GET must refuse a legacy record")

			listing := f.list(t, bucket)
			for _, c := range *listing.Contents {
				if c.Key == tt.key {
					assert.Empty(t, c.ETag, "the listing must not invent an ETag for a legacy record")
				}
			}
		})
	}
}

// CopyObject must produce a byte-identical destination. This is asserted on
// content and length, not on status code: a check that only looks at the
// response code would score the current zero-byte defect as a pass.
func TestCopyObjectProducesAByteIdenticalDestination(t *testing.T) {
	f := newHandlerFixture("src-bucket", "dst-bucket")
	body := bytes.Repeat([]byte("payload-"), 4096) // large enough to span multiple stripes

	putRR := f.put("src-bucket", "source-key", body)
	require.Equal(t, http.StatusOK, putRR.Code, putRR.Body.String())

	copyRR := f.copy("dst-bucket", "dest-key", "/src-bucket/source-key", nil)
	require.Equal(t, http.StatusOK, copyRR.Code, copyRR.Body.String())

	var result CopyObjectResult
	require.NoError(t, xml.Unmarshal(copyRR.Body.Bytes(), &result))
	sum := md5.Sum(body)
	assert.Equal(t, quotedHex(sum[:]), result.ETag)

	getRR := f.get("dst-bucket", "dest-key")
	require.Equal(t, http.StatusOK, getRR.Code)
	assert.Equal(t, len(body), getRR.Body.Len(), "destination length must match the source")
	assert.Equal(t, body, getRR.Body.Bytes(), "destination content must match the source byte for byte")
	assert.Equal(t, strconv.Itoa(len(body)), getRR.Header().Get("Content-Length"))
}

// A source larger than one stripe forces the write path around more than one
// iteration of its offset loop, which is where a streaming copy could lose
// or duplicate bytes that a single-stripe copy would never exercise.
func TestCopyObjectAcrossMultipleStripesIsByteIdentical(t *testing.T) {
	// RS(1,1): with one data shard, shardSize is the object size itself, so
	// crossing a second stripe only needs a little over one streamBlockSize
	// rather than one per data shard.
	f := newHandlerFixtureRS(1, 1, "src-bucket", "dst-bucket")
	body := randomBytes(t, streamBlockSize+4096)

	putRR := f.put("src-bucket", "source-key", body)
	require.Equal(t, http.StatusOK, putRR.Code, putRR.Body.String())

	copyRR := f.copy("dst-bucket", "dest-key", "/src-bucket/source-key", nil)
	require.Equal(t, http.StatusOK, copyRR.Code, copyRR.Body.String())

	var result CopyObjectResult
	require.NoError(t, xml.Unmarshal(copyRR.Body.Bytes(), &result))
	sum := md5.Sum(body)
	assert.Equal(t, quotedHex(sum[:]), result.ETag)

	getRR := f.get("dst-bucket", "dest-key")
	require.Equal(t, http.StatusOK, getRR.Code)
	assert.Equal(t, len(body), getRR.Body.Len(), "destination length must match the source")
	assert.True(t, bytes.Equal(body, getRR.Body.Bytes()), "destination must be byte-identical across multiple stripes")
}

// A source read that fails partway through must not leave a destination
// object behind: the copy aborts the shards it had placed and never reaches
// the commit that would make the destination visible to a later GET.
func TestCopyObjectReadFailureLeavesNoCommittedDestination(t *testing.T) {
	f := newHandlerFixture("src-bucket", "dst-bucket")
	body := randomBytes(t, 1<<16)
	require.Equal(t, http.StatusOK, f.put("src-bucket", "source-key", body).Code)

	srcPlace, _, err := loadPlacement(context.Background(), f.mc, f.ring, f.cfg, "src-bucket", "source-key")
	require.NoError(t, err)

	// RS(2,1) tolerates one lost data shard; taking down both puts the
	// source read past what the parity shard alone can rebuild.
	broken := &downBlob{fakeBlob: f.bc, down: srcPlace.DataShardNodes[0]}
	worse := &twoDownBlob{downBlob: broken, alsoDown: srcPlace.DataShardNodes[1]}

	req := httptest.NewRequest(http.MethodPut, "/dst-bucket/dest-key", nil).
		WithContext(objectCtx("dst-bucket", "dest-key"))
	req.Header.Set("X-Amz-Copy-Source", "/src-bucket/source-key")
	rr := httptest.NewRecorder()
	CopyObject(f.mc, worse, f.ring, f.cache, f.cfg).ServeHTTP(rr, req)
	require.NotEqual(t, http.StatusOK, rr.Code, "a read failure mid-copy must not answer 200")

	getRR := f.get("dst-bucket", "dest-key")
	assert.Equal(t, http.StatusNotFound, getRR.Code, "no destination object must be committed after a failed copy")
}

// CopyObject also has to work with the bucket/key form of x-amz-copy-source,
// without the leading slash S3 also accepts.
func TestCopyObjectAcceptsSourceWithoutLeadingSlash(t *testing.T) {
	f := newHandlerFixture("bucket")
	body := []byte("no leading slash")

	require.Equal(t, http.StatusOK, f.put("bucket", "src", body).Code)

	copyRR := f.copy("bucket", "dst", "bucket/src", nil)
	require.Equal(t, http.StatusOK, copyRR.Code, copyRR.Body.String())

	getRR := f.get("bucket", "dst")
	require.Equal(t, http.StatusOK, getRR.Code)
	assert.Equal(t, body, getRR.Body.Bytes())
}

// Every unsupported CopyObject variant must answer a proper S3 error and
// never a 200: a client that gets 200 believes a copy happened when it did
// not, which is the P0 this whole change exists to close.
func TestCopyObjectUnsupportedVariantsNeverAnswer200(t *testing.T) {
	f := newHandlerFixture("bucket")
	require.Equal(t, http.StatusOK, f.put("bucket", "src", []byte("body")).Code)

	tests := []struct {
		name       string
		destKey    string
		copySource string
		headers    map[string]string
		wantStatus int
	}{
		{
			name: "malformed copy source", destKey: "d1",
			copySource: "not-a-valid-source", wantStatus: http.StatusBadRequest,
		},
		{
			name: "nonexistent source", destKey: "d2",
			copySource: "/bucket/does-not-exist", wantStatus: http.StatusNotFound,
		},
		{
			name: "conditional copy-source-if-match", destKey: "d3",
			copySource: "/bucket/src",
			headers:    map[string]string{"X-Amz-Copy-Source-If-Match": `"abc"`},
			wantStatus: http.StatusNotImplemented,
		},
		{
			name: "versioned source", destKey: "d4",
			copySource: "/bucket/src?versionId=1a2b3c", wantStatus: http.StatusNotImplemented,
		},
		{
			name: "self-copy without metadata REPLACE", destKey: "src",
			copySource: "/bucket/src", wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := f.copy("bucket", tt.destKey, tt.copySource, tt.headers)
			assert.Equal(t, tt.wantStatus, rr.Code, rr.Body.String())
			assert.NotEqual(t, http.StatusOK, rr.Code)

			var s3err S3Error
			require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &s3err), "response must be a well-formed S3 error document")
			assert.NotEmpty(t, s3err.Code)
		})
	}
}
