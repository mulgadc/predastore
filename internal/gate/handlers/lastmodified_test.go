package handlers

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bucketRequest builds a ListObjectsV2 request carrying the resolved bucket
// the router would have attached.
func bucketRequest(query string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/"+testBucket+"?"+query, nil)

	return r.WithContext(WithBucket(r.Context(), model.Bucket{Name: testBucket}))
}

// writeAt writes an object with the fixture's minter pinned, so the test knows
// exactly which millisecond every surface should report.
func (f writeFixture) writeAt(t *testing.T, key string, body []byte, at time.Time) {
	t.Helper()
	frozen(f.cfg.Epochs, at)

	ctx := context.Background()
	objectHash := model.ObjectHash(testBucket, key)
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)
	f.publish(t, objectHash, place)
	require.NoError(t, metaPut(ctx, f.mc, model.TableObjects, objectARN(testBucket, key), objectHash[:]))
}

// HEAD, GET and ListObjectsV2 used to give three different answers, two of
// which were wrong by decades. They read one field now, so they agree.
func TestEverySurfaceReportsTheSameWriteTime(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	at := time.UnixMilli(1_800_000_000_123).UTC()
	f.writeAt(t, "k", randomBytes(t, 1<<16), at)
	want := at.Format(httpTimeFormat)

	head := httptest.NewRecorder()
	HeadObject(f.mc, f.ring, testCache(), f.cfg).ServeHTTP(head, objectRequest(http.MethodHead, "k", ""))
	require.Equal(t, http.StatusOK, head.Code)
	assert.Equal(t, want, head.Header().Get("Last-Modified"))

	get := httptest.NewRecorder()
	GetObject(f.mc, f.bc, f.ring, testCache(), f.cfg).ServeHTTP(get, objectRequest(http.MethodGet, "k", ""))
	require.Equal(t, http.StatusOK, get.Code)
	assert.Equal(t, want, get.Header().Get("Last-Modified"))

	list := httptest.NewRecorder()
	ListObjects(f.mc, testCache()).ServeHTTP(list, bucketRequest(""))
	require.Equal(t, http.StatusOK, list.Code, list.Body.String())

	var got ListObjectsV2
	require.NoError(t, xml.Unmarshal(list.Body.Bytes(), &got))
	require.NotNil(t, got.Contents)
	require.Len(t, *got.Contents, 1)
	assert.True(t, (*got.Contents)[0].LastModified.Equal(at),
		"listing reported %s, want %s", (*got.Contents)[0].LastModified, at)
}

// The time is the object's, not the reader's: a listing that answered
// time.Now() looked plausible and defeated every incremental sync that read it.
func TestAnOverwriteAdvancesTheReportedTime(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	first := time.UnixMilli(1_800_000_000_000).UTC()
	f.writeAt(t, "k", randomBytes(t, 1<<16), first)

	head := httptest.NewRecorder()
	HeadObject(f.mc, f.ring, testCache(), f.cfg).ServeHTTP(head, objectRequest(http.MethodHead, "k", ""))
	require.Equal(t, first.Format(httpTimeFormat), head.Header().Get("Last-Modified"))

	second := first.Add(90 * time.Second)
	f.writeAt(t, "k", randomBytes(t, 1<<16), second)

	head = httptest.NewRecorder()
	HeadObject(f.mc, f.ring, testCache(), f.cfg).ServeHTTP(head, objectRequest(http.MethodHead, "k", ""))
	assert.Equal(t, second.Format(httpTimeFormat), head.Header().Get("Last-Modified"))
}

// A version 1 record's epoch is random, so there is no time to report. The
// header stays rather than disappearing: clients require one.
func TestAnUndatedRecordStillCarriesTheHeader(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(1, 0)
	ctx := context.Background()
	objectHash := model.ObjectHash(testBucket, "k")

	v1 := []byte{placementMagic, 0x01, 1}
	v1 = binary.BigEndian.AppendUint64(v1, 0)
	v1 = binary.BigEndian.AppendUint64(v1, 0x1112131415161718)
	v1 = append(v1, 1)
	require.NoError(t, metaPut(ctx, f.mc, model.TableObjects, string(objectHash[:]), v1))

	w := httptest.NewRecorder()
	HeadObject(f.mc, f.ring, testCache(), f.cfg).ServeHTTP(w, objectRequest(http.MethodHead, "k", ""))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, time.Time{}.Format(httpTimeFormat), w.Header().Get("Last-Modified"),
		"a random epoch must never be read as a date")
}

// ListParts serves what UploadPart stored, so a part now reports its own
// epoch: the two records one upload writes cannot disagree about when it was.
func TestListPartsReportsThePartsOwnWriteTime(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(1, 0)
	ctx := context.Background()

	var meta bytes.Buffer
	require.NoError(t, gob.NewEncoder(&meta).Encode(model.UploadMetadata{
		UploadID: "u1", Bucket: testBucket, Key: "k", CreatedAt: time.Now(),
	}))
	require.NoError(t, metaPut(ctx, f.mc, model.TableMultipart, "u1", meta.Bytes()))

	at := time.UnixMilli(1_800_000_000_456).UTC()
	frozen(f.cfg.Epochs, at)

	body := randomBytes(t, 1<<16)
	req := httptest.NewRequest(http.MethodPut,
		"/"+testBucket+"/k?uploadId=u1&partNumber=1", bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req = req.WithContext(WithObject(req.Context(), model.Object{
		Bucket: model.Bucket{Name: testBucket}, Key: "k",
	}))

	put := httptest.NewRecorder()
	UploadPart(f.mc, f.bc, f.ring, testCache(), f.cfg).ServeHTTP(put, req)
	require.Equal(t, http.StatusOK, put.Code, put.Body.String())

	list := httptest.NewRecorder()
	ListParts(f.mc, testCache()).ServeHTTP(list, objectRequest(http.MethodGet, "k", "uploadId=u1"))
	require.Equal(t, http.StatusOK, list.Code, list.Body.String())

	var got ListPartsResult
	require.NoError(t, xml.Unmarshal(list.Body.Bytes(), &got))
	require.Len(t, got.Parts, 1)
	assert.True(t, got.Parts[0].LastModified.Equal(at),
		"part reported %s, want %s", got.Parts[0].LastModified, at)
}
