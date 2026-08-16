package gate

//test:in-package — the listing is only reachable through the routed server,
// and the fake meta store it reads uploads from is unexported.

import (
	"bytes"
	"encoding/gob"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/iampolicy"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedUpload writes an in-flight upload the way CreateMultipartUpload would.
func seedUpload(t *testing.T, meta *fakeMeta, bucket, key, uploadID string, started time.Time) {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(model.UploadMetadata{
		UploadID: uploadID, Bucket: bucket, Key: key, CreatedAt: started,
	}))
	meta.rows[handlers.TableKey(model.TableMultipart, uploadID)] = buf.Bytes()
}

// uploadsServer holds two accounts' buckets and an upload in each, so a
// listing that reaches past its own bucket is visible.
func uploadsServer(t *testing.T) *Server {
	t.Helper()
	meta := newFakeMeta(t,
		model.BucketMetadata{Name: "owner-bucket", Region: "ap-southeast-2", AccountID: acctOwner},
		model.BucketMetadata{Name: "other-bucket", Region: "ap-southeast-2", AccountID: acctOther},
	)

	started := time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
	seedUpload(t, meta, "owner-bucket", "reports/2026.csv", "upload-b", started)
	seedUpload(t, meta, "owner-bucket", "images/disk.img", "upload-a", started)
	seedUpload(t, meta, "other-bucket", "someone-elses.bin", "upload-c", started)

	return newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta:   meta,
		CredProv: &stubCredProvider{creds: map[string]*auth.CredentialResult{
			keyOwner:  {SecretAccessKey: secret, AccountID: acctOwner, PolicyDocuments: []iampolicy.PolicyDocument{allowAllPolicy}},
			keyConfig: {SecretAccessKey: secret, AccountID: acctSys, SkipPolicyCheck: true},
		}},
	})
}

func listUploads(t *testing.T, server *Server, bucket, accessKey string) (int, handlers.ListMultipartUploadsResult) {
	t.Helper()
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, signedReq(t, http.MethodGet, "/"+bucket+"?uploads", accessKey))

	var result handlers.ListMultipartUploadsResult
	if rr.Code == http.StatusOK {
		require.NoError(t, xml.NewDecoder(rr.Body).Decode(&result))
	}
	return rr.Code, result
}

// The listing is what makes an abandoned upload reachable at all: aborting one
// needs its key and upload id, and nothing else reports them.
func TestListMultipartUploadsReturnsTheBucketsUploads(t *testing.T) {
	server := uploadsServer(t)

	status, result := listUploads(t, server, "owner-bucket", keyConfig)

	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, "owner-bucket", result.Bucket)
	require.Len(t, result.Uploads, 2)
	assert.Equal(t, "images/disk.img", result.Uploads[0].Key)
	assert.Equal(t, "upload-a", result.Uploads[0].UploadId)
	assert.False(t, result.Uploads[0].Initiated.IsZero(), "an abort needs to be able to tell an old upload from a live one")
}

// Uploads are keyed by upload id alone, so the whole table is scanned and
// filtered. Getting that filter wrong would show one tenant another's keys.
func TestListMultipartUploadsDoesNotReachIntoAnotherBucket(t *testing.T) {
	server := uploadsServer(t)

	_, result := listUploads(t, server, "owner-bucket", keyConfig)

	for _, upload := range result.Uploads {
		assert.NotEqual(t, "someone-elses.bin", upload.Key)
	}
}

// The scan has no order of its own, so two listings of the same bucket would
// otherwise differ run to run.
func TestListMultipartUploadsIsOrderedByKey(t *testing.T) {
	server := uploadsServer(t)

	_, result := listUploads(t, server, "owner-bucket", keyConfig)

	require.Len(t, result.Uploads, 2)
	assert.Equal(t, "images/disk.img", result.Uploads[0].Key)
	assert.Equal(t, "reports/2026.csv", result.Uploads[1].Key)
}

// Nothing in flight is an empty answer, not an error: a caller sweeping a
// bucket has to be able to tell "none" from "could not tell".
func TestListMultipartUploadsReturnsEmptyForABucketWithNone(t *testing.T) {
	server := newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta:   newFakeMeta(t, model.BucketMetadata{Name: "owner-bucket", Region: "ap-southeast-2", AccountID: acctOwner}),
		CredProv: &stubCredProvider{creds: map[string]*auth.CredentialResult{
			keyConfig: {SecretAccessKey: secret, AccountID: acctSys, SkipPolicyCheck: true},
		}},
	})

	status, result := listUploads(t, server, "owner-bucket", keyConfig)

	require.Equal(t, http.StatusOK, status)
	assert.Empty(t, result.Uploads)
	assert.False(t, result.IsTruncated)
}

// ?uploads must not be answered as an object listing, which is what the bucket
// GET does for every other query string.
func TestBucketGetWithoutUploadsStillListsObjects(t *testing.T) {
	server := uploadsServer(t)

	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, signedReq(t, http.MethodGet, "/owner-bucket", keyOwner))

	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "ListBucketResult")
}

func TestListMultipartUploadsReportsAMissingBucket(t *testing.T) {
	server := uploadsServer(t)

	status, _ := listUploads(t, server, "never-existed", keyConfig)

	assert.Equal(t, http.StatusNotFound, status)
}
