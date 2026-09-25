package handlers

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAttributesFromRequest(t *testing.T) {
	h := http.Header{}
	h.Set("Content-Type", "audio/ogg")
	h.Set("X-Amz-Meta-Mixed-Case", "Value Kept As Sent")
	h.Set("X-Amz-Meta-Empty", "")
	h.Add("X-Amz-Meta-Repeated", "one")
	h.Add("X-Amz-Meta-Repeated", "two")
	h["x-amz-meta-raw"] = []string{"uncanonicalised"}
	h.Set("X-Amz-Meta", "not user metadata: no name separator")
	h.Set("Cache-Control", "no-store")

	got, err := attributesFromRequest(h)
	require.NoError(t, err)
	assert.Equal(t, ObjectAttributes{
		ContentType: "audio/ogg",
		Metadata: map[string]string{
			"mixed-case": "Value Kept As Sent",
			"empty":      "",
			"repeated":   "one,two",
			"raw":        "uncanonicalised",
		},
	}, got)
}

func TestAttributesFromRequestWithNoneIsEmpty(t *testing.T) {
	got, err := attributesFromRequest(http.Header{})
	require.NoError(t, err)
	assert.True(t, got.empty())
	assert.Nil(t, got.Metadata)
}

// The limit counts the bytes of each name and value, not the x-amz-meta-
// prefix, so exactly 2 KB is the largest set S3 accepts.
func TestAttributesFromRequestEnforcesTheMetadataLimit(t *testing.T) {
	withMetadataOfSize := func(n int) http.Header {
		h := http.Header{}
		h.Set("X-Amz-Meta-A", "x")
		h.Set("X-Amz-Meta-B", strings.Repeat("y", n-3))
		return h
	}

	_, err := attributesFromRequest(withMetadataOfSize(maxUserMetadataSize))
	require.NoError(t, err)

	_, err = attributesFromRequest(withMetadataOfSize(maxUserMetadataSize + 1))
	require.ErrorIs(t, err, model.ErrMetadataTooLargeError)
}

func TestSetAttributeHeaders(t *testing.T) {
	h := http.Header{}
	setAttributeHeaders(h, ObjectAttributes{ContentType: "text/plain", Metadata: map[string]string{"foo": "bar"}})
	assert.Equal(t, "text/plain", h.Get("Content-Type"))
	assert.Equal(t, []string{"bar"}, h["x-amz-meta-foo"], //nolint:staticcheck // the lowercase key is what is under test.
		"the name must reach the wire lowercased")
	assert.NotContains(t, h, "X-Amz-Meta-Foo")

	h = http.Header{}
	setAttributeHeaders(h, ObjectAttributes{})
	assert.Equal(t, defaultContentType, h.Get("Content-Type"))
	assert.Len(t, h, 1)
}

// S3 reads a header's bytes as Latin-1 and returns anything outside ASCII as
// a UTF-8 encoded word. The expected value is the one AWS documents for a PUT
// of the UTF-8 bytes of "ÄMÄZÕÑ S3".
func TestMetadataHeaderValueEncodesNonASCIIAsS3Does(t *testing.T) {
	assert.Equal(t, "AMAZONS3", metadataHeaderValue("AMAZONS3"))
	assert.Empty(t, metadataHeaderValue(""))
	assert.Equal(t, "=?UTF-8?B?w4PChE3Dg8KEWsODwpXDg8KRIFMz?=", metadataHeaderValue("ÄMÄZÕÑ S3"))
	assert.Equal(t, "=?UTF-8?B?SGVsbG8gV29ybGTDqQ==?=", metadataHeaderValue("Hello World\xe9"))
}

func TestPlacementRecordCarriesAttributes(t *testing.T) {
	rec := ObjectToShardNodes{
		Size: 3, WriteEpoch: 7, BlockSize: 1024, Timestamped: true,
		Digest: bytes.Repeat([]byte{0xcd}, 16), PartCount: 2,
		Attributes: ObjectAttributes{
			ContentType: "audio/ogg",
			Metadata: map[string]string{
				"key1": "value1", "empty": "", "latin1": "Hello World\xe9",
			},
		},
		DataShardNodes: []config.NodeID{6, 12}, ParityShardNodes: []config.NodeID{1 << 20},
	}

	encoded, err := EncodePlacement(rec)
	require.NoError(t, err)
	assert.Equal(t, byte(placementVersionV4), encoded[1])

	got, err := DecodePlacement(encoded)
	require.NoError(t, err)
	assert.Equal(t, rec, got)

	// Map order must not leak into the record, or identical writes would
	// produce different bytes.
	for range 20 {
		again, err := EncodePlacement(rec)
		require.NoError(t, err)
		require.Equal(t, encoded, again)
	}
}

func TestPlacementRecordWithOnlyAContentTypeIsVersion4(t *testing.T) {
	encoded, err := EncodePlacement(ObjectToShardNodes{
		Attributes:     ObjectAttributes{ContentType: "text/plain"},
		DataShardNodes: []config.NodeID{1},
	})
	require.NoError(t, err)
	assert.Equal(t, byte(placementVersionV4), encoded[1])

	got, err := DecodePlacement(encoded)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", got.Attributes.ContentType)
	assert.Nil(t, got.Attributes.Metadata)
}

// An object with no attributes stays a version 3 record, byte for byte what
// a gate that predates version 4 wrote and can read.
func TestPlacementRecordWithoutAttributesIsVersion3(t *testing.T) {
	rec := ObjectToShardNodes{
		Size: 1, Digest: bytes.Repeat([]byte{1}, 16),
		DataShardNodes: []config.NodeID{3, 6}, ParityShardNodes: []config.NodeID{9},
	}
	encoded, err := EncodePlacement(rec)
	require.NoError(t, err)
	assert.Equal(t, byte(placementVersionV3), encoded[1])
	assert.Len(t, encoded, placementFixedSizeV3+1+3)

	got, err := DecodePlacement(encoded)
	require.NoError(t, err)
	assert.True(t, got.Attributes.empty())
}

// A header a later version records is skipped, so the object stays readable
// with the attributes this version knows.
func TestPlacementRecordSkipsAnUnknownHeader(t *testing.T) {
	b := []byte{placementMagic, placementVersionV4, 1}
	b = append(b, make([]byte, placementFixedSizeV3-3)...)
	b = append(b, 0) // no digest
	b = append(b, 2)
	b = appendField(b, "cache-control")
	b = appendField(b, "no-store")
	b = appendField(b, "x-amz-meta-foo")
	b = appendField(b, "bar")
	b = append(b, 5) // one node id

	got, err := DecodePlacement(b)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"foo": "bar"}, got.Attributes.Metadata)
	assert.Equal(t, []config.NodeID{5}, got.DataShardNodes)
}

func TestPlacementRecordRejectsAMalformedHeaderSection(t *testing.T) {
	good, err := EncodePlacement(ObjectToShardNodes{
		Attributes:     ObjectAttributes{Metadata: map[string]string{"foo": "bar"}},
		DataShardNodes: []config.NodeID{1},
	})
	require.NoError(t, err)
	sectionStart := placementFixedSizeV3 + 1

	withCount := func(count byte) []byte {
		b := append([]byte(nil), good[:sectionStart]...)
		return append(b, count)
	}
	overlongField := append(withCount(1), 0x7f, 'a')

	tests := []struct {
		name  string
		input []byte
	}{
		{"no header count", good[:sectionStart]},
		{"count larger than the record", withCount(0x7f)},
		{"field longer than the record", overlongField},
		{"truncated inside a value", good[:len(good)-2]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodePlacement(tt.input)
			assert.Error(t, err)
		})
	}
}

// putWith is a PUT carrying extra request headers.
func (f handlerFixture) putWith(bucket, key string, body []byte, headers http.Header) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPut, "/"+bucket+"/"+key, bytes.NewReader(body)).
		WithContext(objectCtx(bucket, key))
	req.ContentLength = int64(len(body))
	maps.Copy(req.Header, headers)
	rr := httptest.NewRecorder()
	PutObject(f.mc, f.bc, f.ring, f.cache, f.cfg).ServeHTTP(rr, req)
	return rr
}

// userMetadata collects the user metadata a response carries, failing on any
// name that did not reach the header map lowercased.
func userMetadata(t *testing.T, h http.Header) map[string]string {
	t.Helper()
	got := map[string]string{}
	for name, values := range h {
		lower := strings.ToLower(name)
		key, ok := strings.CutPrefix(lower, userMetadataPrefix)
		if !ok {
			continue
		}
		assert.Equal(t, lower, name, "metadata header %q is not lowercase", name)
		require.Len(t, values, 1)
		got[key] = values[0]
	}
	return got
}

func s3ErrorCode(t *testing.T, rr *httptest.ResponseRecorder) string {
	t.Helper()
	var e S3Error
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &e), rr.Body.String())
	return e.Code
}

func metadataHeaders(contentType string, meta map[string]string) http.Header {
	h := http.Header{}
	if contentType != "" {
		h.Set("Content-Type", contentType)
	}
	for k, v := range meta {
		h.Set("X-Amz-Meta-"+k, v)
	}
	return h
}

func TestPutObjectAttributesRoundTrip(t *testing.T) {
	f := newHandlerFixture("bucket")
	sent := map[string]string{"meta1": "mymeta", "empty": "", "latin1": "Hello World\xe9"}
	meta := map[string]string{"meta1": "mymeta", "empty": "", "latin1": "=?UTF-8?B?SGVsbG8gV29ybGTDqQ==?="}

	put := f.putWith("bucket", "doc", []byte("bar"), metadataHeaders("text/bla", sent))
	require.Equal(t, http.StatusOK, put.Code, put.Body.String())

	head := f.head("bucket", "doc")
	require.Equal(t, http.StatusOK, head.Code)
	assert.Equal(t, "text/bla", head.Header().Get("Content-Type"))
	assert.Equal(t, meta, userMetadata(t, head.Header()))

	get := f.get("bucket", "doc")
	require.Equal(t, http.StatusOK, get.Code)
	assert.Equal(t, "text/bla", get.Header().Get("Content-Type"))
	assert.Equal(t, meta, userMetadata(t, get.Header()))
	assert.Equal(t, "bar", get.Body.String())
}

// A PUT replaces the object's attributes wholesale: one that sends none
// leaves none, and the Content-Type falls back to S3's default.
func TestPutObjectReplacesAttributes(t *testing.T) {
	f := newHandlerFixture("bucket")
	require.Equal(t, http.StatusOK,
		f.putWith("bucket", "doc", []byte("one"), metadataHeaders("text/plain", map[string]string{"meta1": "old"})).Code)
	require.Equal(t, http.StatusOK, f.put("bucket", "doc", []byte("two")).Code)

	for _, rr := range []*httptest.ResponseRecorder{f.head("bucket", "doc"), f.get("bucket", "doc")} {
		require.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "binary/octet-stream", rr.Header().Get("Content-Type"))
		assert.Empty(t, userMetadata(t, rr.Header()))
	}
}

func TestPutObjectRefusesOversizeMetadata(t *testing.T) {
	f := newHandlerFixture("bucket")
	oversize := map[string]string{"big": strings.Repeat("x", maxUserMetadataSize)}

	rr := f.putWith("bucket", "doc", []byte("body"), metadataHeaders("", oversize))
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Equal(t, "MetadataTooLarge", s3ErrorCode(t, rr))
	assert.Equal(t, http.StatusNotFound, f.head("bucket", "doc").Code, "a refused PUT must store nothing")
}

// Each version keeps the attributes it was written with.
func TestVersionedReadServesThatVersionsAttributes(t *testing.T) {
	f := newHandlerFixture("bucket")
	require.Equal(t, http.StatusOK, f.putVersioning(t, "bucket", "Enabled").Code)

	first := f.putWith("bucket", "doc", []byte("one"), metadataHeaders("text/plain", map[string]string{"rev": "1"}))
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusOK,
		f.putWith("bucket", "doc", []byte("two"), metadataHeaders("text/html", map[string]string{"rev": "2"})).Code)

	old := f.getVersion("bucket", "doc", first.Header().Get(versionIDHeader))
	require.Equal(t, http.StatusOK, old.Code)
	assert.Equal(t, "text/plain", old.Header().Get("Content-Type"))
	assert.Equal(t, map[string]string{"rev": "1"}, userMetadata(t, old.Header()))

	current := f.head("bucket", "doc")
	assert.Equal(t, "text/html", current.Header().Get("Content-Type"))
	assert.Equal(t, map[string]string{"rev": "2"}, userMetadata(t, current.Header()))
}

func TestMultipartUploadCarriesTheUploadsAttributes(t *testing.T) {
	f := newHandlerFixture("bucket")
	const bucket, key = "bucket", "big-object"
	meta := map[string]string{"foo": "bar"}

	createReq := httptest.NewRequest(http.MethodPost, "/"+bucket+"/"+key, nil).WithContext(objectCtx(bucket, key))
	createReq.Header = metadataHeaders("text/bla", meta)
	createRR := httptest.NewRecorder()
	CreateMultipartUpload(f.mc, f.cache).ServeHTTP(createRR, createReq)
	require.Equal(t, http.StatusOK, createRR.Code, createRR.Body.String())
	var created InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(createRR.Body.Bytes(), &created))

	part := []byte("the only part")
	partReq := httptest.NewRequest(http.MethodPut,
		fmt.Sprintf("/%s/%s?partNumber=1&uploadId=%s", bucket, key, url.QueryEscape(created.UploadId)),
		bytes.NewReader(part)).WithContext(objectCtx(bucket, key))
	partReq.ContentLength = int64(len(part))
	partRR := httptest.NewRecorder()
	UploadPart(f.mc, f.bc, f.ring, f.cache, f.cfg).ServeHTTP(partRR, partReq)
	require.Equal(t, http.StatusOK, partRR.Code, partRR.Body.String())

	completeBody, err := xml.Marshal(CompleteMultipartUploadRequest{Parts: []MultipartUploadPart{
		{PartNumber: 1, ETag: partRR.Header().Get("ETag")},
	}})
	require.NoError(t, err)
	completeReq := httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/%s/%s?uploadId=%s", bucket, key, url.QueryEscape(created.UploadId)),
		bytes.NewReader(completeBody)).WithContext(objectCtx(bucket, key))
	completeRR := httptest.NewRecorder()
	CompleteMultipartUpload(f.mc, f.bc, f.ring, f.cache, f.cfg).ServeHTTP(completeRR, completeReq)
	require.Equal(t, http.StatusOK, completeRR.Code, completeRR.Body.String())

	for _, rr := range []*httptest.ResponseRecorder{f.head(bucket, key), f.get(bucket, key)} {
		require.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "text/bla", rr.Header().Get("Content-Type"))
		assert.Equal(t, meta, userMetadata(t, rr.Header()))
	}
}

func TestCreateMultipartUploadRefusesOversizeMetadata(t *testing.T) {
	f := newHandlerFixture("bucket")
	req := httptest.NewRequest(http.MethodPost, "/bucket/doc", nil).WithContext(objectCtx("bucket", "doc"))
	req.Header = metadataHeaders("", map[string]string{"big": strings.Repeat("x", maxUserMetadataSize)})
	rr := httptest.NewRecorder()
	CreateMultipartUpload(f.mc, f.cache).ServeHTTP(rr, req)

	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Equal(t, "MetadataTooLarge", s3ErrorCode(t, rr))
}

func TestCopyObjectMetadataDirective(t *testing.T) {
	srcMeta := map[string]string{"key1": "value1", "key2": "value2"}
	source := "/" + copyTestBucket + "/src"

	seed := func(t *testing.T) handlerFixture {
		f := newHandlerFixture(copyTestBucket)
		rr := f.putWith(copyTestBucket, "src", []byte("foo"), metadataHeaders("audio/ogg", srcMeta))
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		return f
	}
	requestAttrs := map[string]string{
		"Content-Type": "audio/mpeg", "X-Amz-Meta-Key3": "value3", "X-Amz-Meta-Key2": "value2",
	}

	tests := []struct {
		name      string
		directive string
		wantType  string
		wantMeta  map[string]string
	}{
		{"no directive copies", "", "audio/ogg", srcMeta},
		{"COPY copies", "COPY", "audio/ogg", srcMeta},
		{"REPLACE takes the request's", "REPLACE", "audio/mpeg", map[string]string{"key3": "value3", "key2": "value2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := seed(t)
			headers := maps.Clone(requestAttrs)
			if tt.directive != "" {
				headers["X-Amz-Metadata-Directive"] = tt.directive
			}

			rr := f.copy(copyTestBucket, "dst", source, headers)
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

			for _, got := range []*httptest.ResponseRecorder{f.head(copyTestBucket, "dst"), f.get(copyTestBucket, "dst")} {
				require.Equal(t, http.StatusOK, got.Code)
				assert.Equal(t, tt.wantType, got.Header().Get("Content-Type"))
				assert.Equal(t, tt.wantMeta, userMetadata(t, got.Header()))
			}
		})
	}

	t.Run("REPLACE onto itself replaces", func(t *testing.T) {
		f := seed(t)
		rr := f.copy(copyTestBucket, "src", source, map[string]string{
			"X-Amz-Metadata-Directive": "REPLACE", "X-Amz-Meta-Foo": "bar",
		})
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

		got := f.get(copyTestBucket, "src")
		assert.Equal(t, map[string]string{"foo": "bar"}, userMetadata(t, got.Header()))
		assert.Equal(t, "binary/octet-stream", got.Header().Get("Content-Type"))
		assert.Equal(t, "foo", got.Body.String())
	})

	t.Run("an unknown directive is refused", func(t *testing.T) {
		f := seed(t)
		rr := f.copy(copyTestBucket, "dst", source, map[string]string{"X-Amz-Metadata-Directive": "MERGE"})
		require.Equal(t, http.StatusBadRequest, rr.Code)
		assert.Equal(t, "InvalidArgument", s3ErrorCode(t, rr))
	})

	t.Run("REPLACE with oversize metadata is refused", func(t *testing.T) {
		f := seed(t)
		rr := f.copy(copyTestBucket, "dst", source, map[string]string{
			"X-Amz-Metadata-Directive": "REPLACE", "X-Amz-Meta-Big": strings.Repeat("x", maxUserMetadataSize),
		})
		require.Equal(t, http.StatusBadRequest, rr.Code)
		assert.Equal(t, "MetadataTooLarge", s3ErrorCode(t, rr))
	})
}
