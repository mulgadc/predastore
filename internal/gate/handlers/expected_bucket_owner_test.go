package handlers

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ownedBucket   = "owned-bucket"
	ownerAccount  = "111122223333"
	otherAccount  = "444455556666"
	ownedTestFile = "a.txt"
)

// seedOwnedBucket records a bucket in global state, which is the only place
// carrying an owner: a config-declared bucket exists with none.
func seedOwnedBucket(t *testing.T, mc MetaClient, bucket, accountID string) {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(model.BucketMetadata{
		Name:      bucket,
		Region:    "ap-southeast-2",
		AccountID: accountID,
	}))
	require.NoError(t, metaPut(context.Background(), mc, model.TableBuckets, bucket, buf.Bytes()))
}

// ownerReadFailingMeta fails the read of one key the way a replica set that
// cannot answer does: an error, not a missing row.
type ownerReadFailingMeta struct {
	*fakeMeta

	failOn string
}

var _ MetaClient = (*ownerReadFailingMeta)(nil)

func (m *ownerReadFailingMeta) Get(ctx context.Context, key string) ([]byte, error) {
	if key == m.failOn {
		return nil, errors.New("no replica would answer")
	}
	return m.fakeMeta.Get(ctx, key)
}

func (m *ownerReadFailingMeta) LeaderGet(ctx context.Context, key string) ([]byte, error) {
	if key == m.failOn {
		return nil, errors.New("no replica would answer")
	}
	return m.fakeMeta.LeaderGet(ctx, key)
}

// staleOwnerMeta answers an ordinary read of one bucket's record with an owner
// the bucket no longer has, and a leader read with the owner it does have.
// That is the difference between the two calls in production, and the only
// property of the real client a map does not have.
type staleOwnerMeta struct {
	*fakeMeta

	bucket     string
	staleOwner string

	mu          sync.Mutex
	leaderReads int
}

var _ MetaClient = (*staleOwnerMeta)(nil)

func (m *staleOwnerMeta) Get(ctx context.Context, key string) ([]byte, error) {
	if key == TableKey(model.TableBuckets, m.bucket) {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(model.BucketMetadata{
			Name:      m.bucket,
			Region:    "ap-southeast-2",
			AccountID: m.staleOwner,
		}); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	return m.fakeMeta.Get(ctx, key)
}

func (m *staleOwnerMeta) LeaderGet(ctx context.Context, key string) ([]byte, error) {
	if key == TableKey(model.TableBuckets, m.bucket) {
		m.mu.Lock()
		m.leaderReads++
		m.mu.Unlock()
	}
	return m.fakeMeta.Get(ctx, key)
}

// ownerRequest builds a request the router would leave for a bucket handler,
// carrying an expected owner when one is given.
func ownerRequest(method, target, bucket, expectOwner string) *http.Request {
	req := httptest.NewRequest(method, target, nil)
	ctx := WithBucket(req.Context(), model.Bucket{Name: bucket})
	if expectOwner != "" {
		req.Header.Set(ExpectedBucketOwnerHeader, expectOwner)
	}
	return req.WithContext(ctx)
}

// withBody gives a request a payload, for the handlers that parse one.
func withBody(req *http.Request, body string) *http.Request {
	replaced := httptest.NewRequest(req.Method, req.URL.String(), strings.NewReader(body))
	replaced.Header = req.Header
	return replaced.WithContext(req.Context())
}

// seedOwnedObject stores one object the way PutObject does, so a handler that
// refuses has something it could have destroyed.
func seedOwnedObject(t *testing.T, f writeFixture, bucket, key string, body []byte) {
	t.Helper()
	ctx := context.Background()
	objectHash := model.ObjectHash(bucket, key)

	place, _, err := f.write(ctx, objectHash, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)
	record, err := EncodePlacement(place)
	require.NoError(t, err)
	require.NoError(t, metaPut(ctx, f.mc, model.TableObjects, string(objectHash[:]), record))
	require.NoError(t, metaPut(ctx, f.mc, model.TableObjects, objectARN(bucket, key), objectHash[:]))
}

// seedOwnedUpload records an upload the way CreateMultipartUpload does.
func seedOwnedUpload(t *testing.T, f writeFixture, bucket, key, uploadID string) {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(model.UploadMetadata{
		UploadID: uploadID, Bucket: bucket, Key: key, CreatedAt: time.Now(),
	}))
	require.NoError(t, metaPut(context.Background(), f.mc, model.TableMultipart, uploadID, buf.Bytes()))
}

// ownerObjectRequest is ownerRequest for an object-level handler.
func ownerObjectRequest(method, target, bucket, key, expectOwner string) *http.Request {
	req := ownerRequest(method, target, bucket, expectOwner)
	ctx := WithObject(req.Context(), model.Object{Bucket: model.Bucket{Name: bucket}, Key: key})
	ctx = WithObjectAuthorizer(ctx, func(string, string) bool { return true })
	return req.WithContext(ctx)
}

// Every handler the account reaper drives must refuse a request whose expected
// owner is not the bucket's account. The reaper runs on a config credential,
// which bucketAccessAllowed waves through before it reads an owner at all, so
// this precondition is the only owner comparison that path has.
func TestExpectedBucketOwnerRefusesEveryDestructiveHandler(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		method  string
		target  string
		key     string
		body    string
		handler func(f writeFixture, cache *BucketCache) http.Handler
	}{
		{
			name: "DeleteBucket", method: http.MethodDelete, target: "/" + ownedBucket,
			handler: func(f writeFixture, cache *BucketCache) http.Handler {
				return DeleteBucket(f.mc, cache)
			},
		},
		{
			name: "ListObjects", method: http.MethodGet, target: "/" + ownedBucket + "?list-type=2",
			handler: func(f writeFixture, cache *BucketCache) http.Handler {
				return ListObjects(f.mc, cache)
			},
		},
		{
			name: "ListObjectVersions", method: http.MethodGet, target: "/" + ownedBucket + "?versions",
			handler: func(f writeFixture, cache *BucketCache) http.Handler {
				return ListObjectVersions(f.mc, cache)
			},
		},
		{
			name: "ListMultipartUploads", method: http.MethodGet, target: "/" + ownedBucket + "?uploads",
			handler: func(f writeFixture, cache *BucketCache) http.Handler {
				return ListMultipartUploads(f.mc, cache)
			},
		},
		{
			name: "DeleteObject", method: http.MethodDelete, target: "/" + ownedBucket + "/" + ownedTestFile,
			key: ownedTestFile,
			handler: func(f writeFixture, cache *BucketCache) http.Handler {
				return DeleteObject(f.mc, f.bc, cache, f.cfg)
			},
		},
		{
			name:   "AbortMultipartUpload",
			method: http.MethodDelete, target: "/" + ownedBucket + "/" + ownedTestFile + "?uploadId=u1",
			key: ownedTestFile,
			handler: func(f writeFixture, cache *BucketCache) http.Handler {
				return AbortMultipartUpload(f.mc, f.bc, cache)
			},
		},
		{
			name: "DeleteObjects", method: http.MethodPost, target: "/" + ownedBucket + "?delete",
			body: "<Delete><Object><Key>" + ownedTestFile + "</Key></Object></Delete>",
			handler: func(f writeFixture, cache *BucketCache) http.Handler {
				return DeleteObjects(f.mc, f.bc, cache, f.cfg)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newWriteFixture(4, 2)
			cache := NewBucketCache(nil)
			seedOwnedBucket(t, f.mc, ownedBucket, ownerAccount)

			// The object and the upload are real, so a refusal cannot be a
			// NoSuchKey or NoSuchUpload wearing the wrong status.
			seedOwnedObject(t, f, ownedBucket, ownedTestFile, []byte("body"))
			seedOwnedUpload(t, f, ownedBucket, ownedTestFile, "u1")

			var req *http.Request
			if tc.key == "" {
				req = ownerRequest(tc.method, tc.target, ownedBucket, otherAccount)
			} else {
				req = ownerObjectRequest(tc.method, tc.target, ownedBucket, tc.key, otherAccount)
			}
			if tc.body != "" {
				req = withBody(req, tc.body)
			}

			w := httptest.NewRecorder()
			tc.handler(f, cache).ServeHTTP(w, req)

			require.Equal(t, http.StatusForbidden, w.Code, "body: %s", w.Body.String())
			assert.Equal(t, string(model.ErrAccessDenied), decodeS3Error(t, w).Code)

			// A refused request must not have destroyed anything.
			_, err := metaGet(context.Background(), f.mc, model.TableObjects,
				objectARN(ownedBucket, ownedTestFile))
			assert.NoError(t, err, "the refused request removed the listing row")
			_, err = metaGet(context.Background(), f.mc, model.TableBuckets, ownedBucket)
			assert.NoError(t, err, "the refused request removed the bucket")
		})
	}
}

// A matching expected owner is not an error, so the precondition cannot be
// passing the test above by refusing everything.
func TestExpectedBucketOwnerAllowsTheRealOwner(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(4, 2)
	cache := NewBucketCache(nil)
	seedOwnedBucket(t, f.mc, ownedBucket, ownerAccount)

	w := httptest.NewRecorder()
	ListObjects(f.mc, cache).ServeHTTP(w,
		ownerRequest(http.MethodGet, "/"+ownedBucket+"?list-type=2", ownedBucket, ownerAccount))

	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
}

// No header asserts nothing, which is what keeps the change inert for every
// caller that does not send one.
func TestExpectedBucketOwnerAbsentHeaderIsUnchangedBehaviour(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(4, 2)
	cache := NewBucketCache(nil)
	seedOwnedBucket(t, f.mc, ownedBucket, ownerAccount)

	w := httptest.NewRecorder()
	ListObjects(f.mc, cache).ServeHTTP(w,
		ownerRequest(http.MethodGet, "/"+ownedBucket+"?list-type=2", ownedBucket, ""))

	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
}

// A config-declared bucket records no owner, so no expected owner can match
// one. Denying is the safe direction and nothing addresses a platform bucket
// with this header: the reaper lists only buckets an account owns.
func TestExpectedBucketOwnerDeniesAConfigBucket(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(4, 2)
	cache := NewBucketCache([]BucketConfig{{Name: ownedBucket, Region: "ap-southeast-2"}})

	w := httptest.NewRecorder()
	ListObjects(f.mc, cache).ServeHTTP(w,
		ownerRequest(http.MethodGet, "/"+ownedBucket+"?list-type=2", ownedBucket, ownerAccount))

	require.Equal(t, http.StatusForbidden, w.Code, "body: %s", w.Body.String())
	assert.Equal(t, string(model.ErrAccessDenied), decodeS3Error(t, w).Code)
}

// A tenant bucket deleted through another gate is gone, not forbidden. This
// gate's cache still holds the entry it created, and answering AccessDenied
// there would turn an already-deleted bucket into a permanent refusal for a
// caller retrying with the owner it had.
func TestExpectedBucketOwnerReportsADeletedTenantBucketAsMissing(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(4, 2)
	cache := NewBucketCache(nil)

	// Created here, so the cache knows it, then removed from global state the
	// way a delete served by another gate leaves it.
	cache.add(ownedBucket, "ap-southeast-2", ownerAccount, false)

	w := httptest.NewRecorder()
	DeleteBucket(f.mc, cache).ServeHTTP(w,
		ownerRequest(http.MethodDelete, "/"+ownedBucket, ownedBucket, ownerAccount))

	require.Equal(t, http.StatusNotFound, w.Code, "body: %s", w.Body.String())
	assert.Equal(t, string(model.ErrNoSuchBucket), decodeS3Error(t, w).Code)
}

// An owner that cannot be read is not an owner that matches. This guards a
// destructive call, so an unreadable bucket record fails the request closed
// rather than letting the delete through on an unproven precondition.
func TestExpectedBucketOwnerFailsClosedOnAnUnreadableRecord(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(4, 2)
	cache := NewBucketCache(nil)
	seedOwnedBucket(t, f.mc, ownedBucket, ownerAccount)
	mc := &ownerReadFailingMeta{fakeMeta: f.mc, failOn: TableKey(model.TableBuckets, ownedBucket)}

	w := httptest.NewRecorder()
	DeleteBucket(mc, cache).ServeHTTP(w,
		ownerRequest(http.MethodDelete, "/"+ownedBucket, ownedBucket, ownerAccount))

	require.Equal(t, http.StatusInternalServerError, w.Code, "body: %s", w.Body.String())
}

// The owner is read from a leader, not from whichever replica answers. A
// follower still holding the record of a bucket that has since changed hands
// would otherwise let the successor pass its predecessor's precondition.
func TestExpectedBucketOwnerReadsThroughTheLeader(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(4, 2)
	cache := NewBucketCache(nil)

	// The bucket now belongs to otherAccount. A stale replica still answers
	// with ownerAccount, which is the owner the caller is asserting.
	seedOwnedBucket(t, f.mc, ownedBucket, otherAccount)
	stale := &staleOwnerMeta{fakeMeta: f.mc, bucket: ownedBucket, staleOwner: ownerAccount}

	w := httptest.NewRecorder()
	DeleteBucket(stale, cache).ServeHTTP(w,
		ownerRequest(http.MethodDelete, "/"+ownedBucket, ownedBucket, ownerAccount))

	require.Equal(t, http.StatusForbidden, w.Code, "body: %s", w.Body.String())
	assert.Equal(t, string(model.ErrAccessDenied), decodeS3Error(t, w).Code)
	assert.Positive(t, stale.leaderReads, "the owner was not read through a leader")
}

// A copy holds its source bucket to x-amz-source-expected-bucket-owner before
// its source conditions, so a condition that holds cannot carry a copy past a wrong
// owner, and the real owner still copies.
// Verifies: Q-68.
func TestExpectedSourceOwnerGatesBothCopies(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(4, 2)
	cache := NewBucketCache(nil)
	seedOwnedBucket(t, f.mc, ownedBucket, ownerAccount)
	seedOwnedObject(t, f, ownedBucket, ownedTestFile, []byte("source bytes"))
	seedOwnedUpload(t, f, ownedBucket, "assembled", "u1")

	copies := map[string]struct {
		handler http.Handler
		target  string
		key     string
	}{
		"CopyObject":     {CopyObject(f.mc, f.bc, f.ring, cache, f.cfg), "/" + ownedBucket + "/dest", "dest"},
		"UploadPartCopy": {UploadPartCopy(f.mc, f.bc, f.ring, cache, f.cfg), "/" + ownedBucket + "/assembled?partNumber=1&uploadId=u1", "assembled"},
	}
	for name, c := range copies {
		for _, owner := range []string{otherAccount, ownerAccount} {
			req := ownerObjectRequest(http.MethodPut, c.target, ownedBucket, c.key, "")
			req.Header.Set("X-Amz-Copy-Source", ownedBucket+"/"+ownedTestFile)
			req.Header.Set("X-Amz-Copy-Source-If-Match", "*")
			req.Header.Set(ExpectedSourceBucketOwnerHeader, owner)
			f.mc.resetOps()

			w := httptest.NewRecorder()
			c.handler.ServeHTTP(w, req)

			if owner == ownerAccount {
				assert.Equal(t, http.StatusOK, w.Code, "%s, real owner: %s", name, w.Body.String())
				continue
			}
			assert.Equal(t, http.StatusForbidden, w.Code, "%s, wrong owner: %s", name, w.Body.String())
			assert.Contains(t, w.Body.String(), "<Code>AccessDenied</Code>", name)
			assert.Zero(t, f.mc.ops["put"]+f.mc.ops["swap"], "%s: metadata writes by a refused copy", name)
		}
	}
}
