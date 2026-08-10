package s3

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/pkg/iampolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- validateRequestPath unit tests ---

func TestValidateRequestPath(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		wantErr error
	}{
		{"root", "/", nil},
		{"bucket", "/owner-bucket", nil},
		{"bucket trailing slash is CreateBucket", "/owner-bucket/", nil},
		{"object key", "/owner-bucket/secret/data", nil},
		{"key containing a dot", "/owner-bucket/file.txt", nil},
		{"key segment named dotdotdot", "/owner-bucket/.../data", nil},
		{"object key trailing slash", "/owner-bucket/secret/data/", errKeyTrailingSlash},
		{"encoded key trailing slash", "/owner-bucket/secret/data%2F", errKeyTrailingSlash},
		{"parent segment", "/owner-bucket/secret/../data", errDotSegment},
		{"encoded parent segment", "/owner-bucket/%2e%2e/data", errDotSegment},
		{"current segment", "/owner-bucket/./data", errDotSegment},
		{"parent segment as bucket", "/../etc", errDotSegment},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRequestPath(httptest.NewRequest(http.MethodGet, tt.target, nil))
			assert.Equal(t, tt.wantErr, err)
		})
	}
}

// --- authorization/dispatch parity integration tests ---

// recordingBackend reports the bucket and key each object route was dispatched
// with, so a test can prove the backend was never reached — or was reached with
// exactly the value that was authorized.
type recordingBackend struct {
	*stubBackend

	calls []string
}

func (b *recordingBackend) record(op, bucket, key string) {
	b.calls = append(b.calls, op+" "+bucket+"/"+key)
}

func (b *recordingBackend) GetObject(_ context.Context, req *backend.GetObjectRequest) (*backend.GetObjectResponse, error) {
	b.record("GetObject", req.Bucket, req.Key)
	return &backend.GetObjectResponse{
		Body:         io.NopCloser(strings.NewReader("payload")),
		ContentType:  "application/octet-stream",
		Size:         int64(len("payload")),
		ETag:         "\"etag\"",
		LastModified: time.Unix(0, 0).UTC(),
		StatusCode:   http.StatusOK,
	}, nil
}

func (b *recordingBackend) DeleteObject(_ context.Context, req *backend.DeleteObjectRequest) error {
	b.record("DeleteObject", req.Bucket, req.Key)
	return nil
}

func (b *recordingBackend) CreateBucket(_ context.Context, req *backend.CreateBucketRequest) (*backend.CreateBucketResponse, error) {
	b.record("CreateBucket", req.Bucket, "")
	return &backend.CreateBucketResponse{Location: "/" + req.Bucket}, nil
}

// guardrailPolicy is the idiomatic shape this defect defeated: a broad Allow
// over the bucket with an exact-ARN Deny carving out one object.
var guardrailPolicy = iampolicy.PolicyDocument{
	Version: "2012-10-17",
	Statement: []iampolicy.Statement{
		{
			Effect:   "Allow",
			Action:   iampolicy.StringOrArr{"s3:*"},
			Resource: iampolicy.StringOrArr{"arn:aws:s3:::owner-bucket", "arn:aws:s3:::owner-bucket/*", "arn:aws:s3:::new-bucket"},
		},
		{
			Effect:   "Deny",
			Action:   iampolicy.StringOrArr{"s3:GetObject", "s3:DeleteObject"},
			Resource: iampolicy.StringOrArr{"arn:aws:s3:::owner-bucket/secret/data"},
		},
	},
}

// exactObjectGrant grants one object by exact ARN and nothing else, so a
// request that succeeds under it can only have been authorized on that ARN.
func exactObjectGrant(key string) iampolicy.PolicyDocument {
	return iampolicy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []iampolicy.Statement{{
			Effect:   "Allow",
			Action:   iampolicy.StringOrArr{"s3:GetObject"},
			Resource: iampolicy.StringOrArr{"arn:aws:s3:::owner-bucket/" + key},
		}},
	}
}

func parityServer(t *testing.T, policy iampolicy.PolicyDocument) (*HTTP2Server, *recordingBackend) {
	t.Helper()
	be := &recordingBackend{stubBackend: &stubBackend{buckets: map[string]*backend.BucketMetadata{
		"owner-bucket": {Name: "owner-bucket", Region: "ap-southeast-2", AccountID: acctOwner},
	}}}
	credProv := &stubCredProvider{creds: map[string]*CredentialResult{
		keyOwner: {SecretAccessKey: secret, AccountID: acctOwner, PolicyDocuments: []iampolicy.PolicyDocument{policy}},
	}}
	cfg := &Config{Region: "ap-southeast-2"}
	return NewHTTP2ServerWithBackend(cfg, be, credProv), be
}

func serveSigned(t *testing.T, server *HTTP2Server, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, signedReq(t, method, target, keyOwner))
	return rr
}

// Paths that only normalise into their dispatched form never reach the
// authorization decision. They are a client error, not a policy outcome, so
// they must not be reported as AccessDenied.
func TestRequestPath_MalformedRejected(t *testing.T) {
	tests := []struct {
		name   string
		method string
		target string
	}{
		{"trailing slash GET", http.MethodGet, "/owner-bucket/secret/data/"},
		{"trailing slash DELETE", http.MethodDelete, "/owner-bucket/secret/data/"},
		{"encoded trailing slash", http.MethodGet, "/owner-bucket/secret/data%2F"},
		{"parent segment", http.MethodGet, "/owner-bucket/public/../secret/data"},
		{"encoded parent segment", http.MethodGet, "/owner-bucket/public/%2e%2e/secret/data"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, be := parityServer(t, guardrailPolicy)
			rr := serveSigned(t, server, tt.method, tt.target)

			assert.Equal(t, http.StatusBadRequest, rr.Code)
			assert.Contains(t, rr.Body.String(), "InvalidURI")
			assert.NotContains(t, rr.Body.String(), "AccessDenied")
			assert.Empty(t, be.calls, "backend must not be reached for a rejected path")
		})
	}
}

// An exact-ARN Deny must survive the spellings that do reach the decision.
func TestPathParity_ExplicitDenyNotBypassable(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			server, be := parityServer(t, guardrailPolicy)
			rr := serveSigned(t, server, method, "/owner-bucket/secret/data")

			assert.Equal(t, http.StatusForbidden, rr.Code)
			assert.Contains(t, rr.Body.String(), "AccessDenied")
			assert.Empty(t, be.calls, "backend must not be reached for a denied request")
		})
	}
}

// The invariant itself: for every spelling that survives validation, the ARN
// the policy was evaluated against is the bucket/key the backend receives. An
// exact-ARN grant makes a 200 proof that the two agree, since nothing else
// could have authorized it.
func TestPathParity_AuthorizedARNMatchesDispatch(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		wantKey string
	}{
		{"canonical", "/owner-bucket/secret/data", "secret/data"},
		{"over-encoded segment", "/owner-bucket/%73ecret/data", "%73ecret/data"},
		{"encoded separator", "/owner-bucket/secret%2Fdata", "secret%2Fdata"},
		{"encoded reserved char", "/owner-bucket/a%3Ab", "a%3Ab"},
		{"canonically escaped utf-8", "/owner-bucket/reports/r%C3%A9union.txt", "reports/réunion.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, be := parityServer(t, exactObjectGrant(tt.wantKey))
			rr := serveSigned(t, server, http.MethodGet, tt.target)

			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			assert.Equal(t, []string{"GetObject owner-bucket/" + tt.wantKey}, be.calls)
		})
	}
}

// The routing subject must be the StripSlashes rewrite, so the middleware that
// resolves it has to be registered after StripSlashes. Nothing observable in a
// response distinguishes the two orderings, so assert the registration itself.
func TestPathParity_TargetResolvedAfterStripSlashes(t *testing.T) {
	server, _ := parityServer(t, guardrailPolicy)

	stripIdx, targetIdx := -1, -1
	wantStrip := reflect.ValueOf(middleware.StripSlashes).Pointer()
	wantTarget := reflect.ValueOf(server.s3TargetMiddleware).Pointer()
	for i, mw := range server.router.Middlewares() {
		switch reflect.ValueOf(mw).Pointer() {
		case wantStrip:
			stripIdx = i
		case wantTarget:
			targetIdx = i
		}
	}

	require.NotEqual(t, -1, stripIdx, "StripSlashes is not registered")
	require.NotEqual(t, -1, targetIdx, "s3TargetMiddleware is not registered")
	assert.Less(t, stripIdx, targetIdx,
		"s3TargetMiddleware must run after StripSlashes or it resolves a different path than chi routes on")
}

// The percent-encoding variant of the split: the grant covers secret/data, and
// the router dispatches %73ecret/data, so the grant must not authorize it.
func TestPathParity_EncodedKeyDoesNotSatisfyExactGrant(t *testing.T) {
	server, be := parityServer(t, exactObjectGrant("secret/data"))
	rr := serveSigned(t, server, http.MethodGet, "/owner-bucket/%73ecret/data")

	assert.Equal(t, http.StatusForbidden, rr.Code)
	assert.Contains(t, rr.Body.String(), "AccessDenied")
	assert.Empty(t, be.calls, "backend must not be reached for a denied request")
}

// The Deny covers one object, not the prefix: everything else in the bucket
// still resolves, and the key the backend sees is the key that was authorized.
func TestPathParity_UnrelatedObjectAllowed(t *testing.T) {
	server, be := parityServer(t, guardrailPolicy)
	rr := serveSigned(t, server, http.MethodGet, "/owner-bucket/public/data")

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, []string{"GetObject owner-bucket/public/data"}, be.calls)
}

// What StripSlashes is there for: PUT /bucket/ is CreateBucket, not a rejected
// object write.
func TestPathParity_TrailingSlashCreateBucket(t *testing.T) {
	server, be := parityServer(t, guardrailPolicy)
	rr := serveSigned(t, server, http.MethodPut, "/new-bucket/")

	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Equal(t, []string{"CreateBucket new-bucket/"}, be.calls)
}

// Without the resolving middleware the auth middleware has no subject to
// authorize, and must fail closed rather than fall through as ListAllMyBuckets.
func TestPathParity_MissingTargetFailsClosed(t *testing.T) {
	server, _ := parityServer(t, guardrailPolicy)

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler must not run without a resolved request target")
	})
	rr := httptest.NewRecorder()
	server.sigV4AuthMiddleware(next).ServeHTTP(rr, signedReq(t, http.MethodGet, "/owner-bucket/secret/data", keyOwner))

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	assert.Contains(t, rr.Body.String(), "InternalError")
}
