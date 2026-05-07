// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package s3

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mulgadc/predastore/auth"
	"github.com/mulgadc/predastore/backend"
	"github.com/stretchr/testify/assert"
)

// --- bucketAccessAllowed unit tests ---

func TestBucketAccessAllowed(t *testing.T) {
	owned := &backend.BucketMetadata{Name: "owner-bucket", AccountID: "000000000001"}
	publicBucket := &backend.BucketMetadata{Name: "public-bucket", AccountID: "000000000001", Public: true}
	configBucket := &backend.BucketMetadata{Name: "predastore", AccountID: "000000000000"}

	tests := []struct {
		name            string
		caller          string
		meta            *backend.BucketMetadata
		skipPolicyCheck bool
		want            bool
	}{
		{"same account", "000000000001", owned, false, true},
		{"cross account", "000000000002", owned, false, false},
		{"cross account against config bucket", "000000000002", configBucket, false, false},
		{"public bucket cross-account read", "000000000002", publicBucket, false, true},
		{"public bucket anonymous", "", publicBucket, false, true},
		{"skip policy check service account", "", owned, true, true},
		{"skip policy check on config bucket", "", configBucket, true, true},
		{"nil metadata fails closed", "000000000001", nil, false, false},
		{"empty owner account never matches", "", &backend.BucketMetadata{AccountID: ""}, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bucketAccessAllowed(tt.caller, tt.meta, tt.skipPolicyCheck)
			assert.Equal(t, tt.want, got)
		})
	}
}

// --- HTTP middleware integration tests ---

// stubCredProvider returns canned CredentialResults keyed by access key.
type stubCredProvider struct {
	creds map[string]*CredentialResult
}

func (p *stubCredProvider) LookupCredentials(accessKeyID string) (*CredentialResult, error) {
	if r, ok := p.creds[accessKeyID]; ok {
		return r, nil
	}
	return nil, ErrKeyNotFound
}

func (p *stubCredProvider) Close() {}

// stubBackend implements backend.Backend with only GetBucketMetadata exercised.
// Other methods return ErrInternalError so any unexpected route invocation is
// obvious in test failures.
type stubBackend struct {
	buckets map[string]*backend.BucketMetadata
}

func (b *stubBackend) GetBucketMetadata(bucket string) (*backend.BucketMetadata, error) {
	if m, ok := b.buckets[bucket]; ok {
		return m, nil
	}
	return nil, backend.ErrNoSuchBucketError.WithResource(bucket)
}

func (b *stubBackend) GetObject(_ context.Context, _ *backend.GetObjectRequest) (*backend.GetObjectResponse, error) {
	return nil, errors.New("stubBackend.GetObject called unexpectedly")
}
func (b *stubBackend) HeadObject(_ context.Context, _, _ string) (*backend.HeadObjectResponse, error) {
	return nil, errors.New("stubBackend.HeadObject called unexpectedly")
}
func (b *stubBackend) PutObject(_ context.Context, _ *backend.PutObjectRequest) (*backend.PutObjectResponse, error) {
	return nil, errors.New("stubBackend.PutObject called unexpectedly")
}
func (b *stubBackend) DeleteObject(_ context.Context, _ *backend.DeleteObjectRequest) error {
	return errors.New("stubBackend.DeleteObject called unexpectedly")
}
func (b *stubBackend) CreateBucket(_ context.Context, _ *backend.CreateBucketRequest) (*backend.CreateBucketResponse, error) {
	return nil, errors.New("stubBackend.CreateBucket called unexpectedly")
}
func (b *stubBackend) DeleteBucket(_ context.Context, _ *backend.DeleteBucketRequest) error {
	return errors.New("stubBackend.DeleteBucket called unexpectedly")
}
func (b *stubBackend) HeadBucket(_ context.Context, _ *backend.HeadBucketRequest) (*backend.HeadBucketResponse, error) {
	return nil, errors.New("stubBackend.HeadBucket called unexpectedly")
}
func (b *stubBackend) ListBuckets(_ context.Context, accountID string) (*backend.ListBucketsResponse, error) {
	out := &backend.ListBucketsResponse{Owner: backend.OwnerInfo{ID: accountID}}
	for _, m := range b.buckets {
		if m.AccountID == accountID {
			out.Buckets = append(out.Buckets, backend.BucketInfo{Name: m.Name, Region: m.Region})
		}
	}
	return out, nil
}
func (b *stubBackend) ListObjects(_ context.Context, req *backend.ListObjectsRequest) (*backend.ListObjectsResponse, error) {
	if _, ok := b.buckets[req.Bucket]; !ok {
		return nil, backend.ErrNoSuchBucketError.WithResource(req.Bucket)
	}
	return &backend.ListObjectsResponse{Name: req.Bucket}, nil
}
func (b *stubBackend) CreateMultipartUpload(_ context.Context, _ *backend.CreateMultipartUploadRequest) (*backend.CreateMultipartUploadResponse, error) {
	return nil, errors.New("stubBackend.CreateMultipartUpload called unexpectedly")
}
func (b *stubBackend) UploadPart(_ context.Context, _ *backend.UploadPartRequest) (*backend.UploadPartResponse, error) {
	return nil, errors.New("stubBackend.UploadPart called unexpectedly")
}
func (b *stubBackend) CompleteMultipartUpload(_ context.Context, _ *backend.CompleteMultipartUploadRequest) (*backend.CompleteMultipartUploadResponse, error) {
	return nil, errors.New("stubBackend.CompleteMultipartUpload called unexpectedly")
}
func (b *stubBackend) AbortMultipartUpload(_ context.Context, _, _, _ string) error {
	return errors.New("stubBackend.AbortMultipartUpload called unexpectedly")
}
func (b *stubBackend) Type() string { return "stub" }
func (b *stubBackend) Close() error { return nil }

const (
	acctOwner = "000000000001"
	acctOther = "000000000002"
	acctSys   = "000000000000"

	keyOwner  = "AKIAOWNER"
	keyOther  = "AKIAOTHER"
	keyConfig = "AKIACONFIG"
	keyNoIAM  = "AKIANOPOLICY"
	secret    = "TESTSECRETKEY"
)

// allowAllPolicy is what spinifex's bootstrap AdministratorAccess looks like.
var allowAllPolicy = iamPolicyDocument{
	Version: "2012-10-17",
	Statement: []iamStatement{{
		Effect:   "Allow",
		Action:   iamStringOrArr{"s3:*"},
		Resource: iamStringOrArr{"*"},
	}},
}

func ownershipServer(t *testing.T) *HTTP2Server {
	t.Helper()
	cfg := &Config{
		Region: "ap-southeast-2",
		Buckets: []S3_Buckets{{
			Name:      "predastore",
			Region:    "ap-southeast-2",
			Type:      "distributed",
			Public:    false,
			AccountID: acctSys,
		}},
	}
	be := &stubBackend{buckets: map[string]*backend.BucketMetadata{
		"owner-bucket":  {Name: "owner-bucket", Region: "ap-southeast-2", AccountID: acctOwner},
		"public-bucket": {Name: "public-bucket", Region: "ap-southeast-2", AccountID: acctOwner, Public: true},
	}}
	credProv := &stubCredProvider{creds: map[string]*CredentialResult{
		keyOwner:  {SecretAccessKey: secret, AccountID: acctOwner, PolicyDocuments: []iamPolicyDocument{allowAllPolicy}},
		keyOther:  {SecretAccessKey: secret, AccountID: acctOther, PolicyDocuments: []iamPolicyDocument{allowAllPolicy}},
		keyConfig: {SecretAccessKey: secret, AccountID: acctSys, SkipPolicyCheck: true},
		keyNoIAM:  {SecretAccessKey: secret, AccountID: acctOwner /* no policies */},
	}}
	return NewHTTP2ServerWithBackend(cfg, be, credProv)
}

func signedReq(t *testing.T, method, path, accessKey string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	timestamp := time.Now().UTC().Format(auth.TimeFormat)
	if err := auth.GenerateAuthHeaderReq(accessKey, secret, timestamp, "ap-southeast-2", "s3", req); err != nil {
		t.Fatalf("sign request: %v", err)
	}
	return req
}

// runMiddleware drives only the auth+ownership middleware so backend route
// failures (which we don't mock fully) do not contaminate status assertions.
func runMiddleware(t *testing.T, server *HTTP2Server, req *http.Request) (status int, nextCalled bool, body string) {
	t.Helper()
	rr := httptest.NewRecorder()
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalled = true
		w.WriteHeader(http.StatusOK)
	})
	server.sigV4AuthMiddleware(next).ServeHTTP(rr, req)
	b, _ := io.ReadAll(rr.Body)
	return rr.Code, nextCalled, string(b)
}

func TestOwnership_OwnerAllowed(t *testing.T) {
	server := ownershipServer(t)
	status, nextCalled, _ := runMiddleware(t, server, signedReq(t, http.MethodGet, "/owner-bucket", keyOwner))
	assert.True(t, nextCalled)
	assert.Equal(t, http.StatusOK, status)
}

// The reproducer: a non-owner with AdministratorAccess must be denied on
// another account's bucket, even though their IAM policy says "s3:* on *".
func TestOwnership_CrossAccountDenied(t *testing.T) {
	server := ownershipServer(t)
	status, nextCalled, body := runMiddleware(t, server, signedReq(t, http.MethodGet, "/owner-bucket", keyOther))
	assert.False(t, nextCalled, "cross-account caller must not reach handler")
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "AccessDenied")
}

// Config-defined bucket: non-owner is denied via the synthesised metadata
// fallback. This covers Gap 2 from the plan — without the config AccountID,
// the middleware would have nothing to compare against.
func TestOwnership_CrossAccountDeniedOnConfigBucket(t *testing.T) {
	server := ownershipServer(t)
	status, nextCalled, body := runMiddleware(t, server, signedReq(t, http.MethodGet, "/predastore", keyOther))
	assert.False(t, nextCalled)
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "AccessDenied")
}

func TestOwnership_PublicBucketCrossAccountAllowed(t *testing.T) {
	server := ownershipServer(t)
	status, nextCalled, _ := runMiddleware(t, server, signedReq(t, http.MethodGet, "/public-bucket", keyOther))
	assert.True(t, nextCalled, "public bucket should let cross-account read through")
	assert.Equal(t, http.StatusOK, status)
}

// Service account: SkipPolicyCheck=true bypasses both IAM and ownership
// even on a foreign-owned config bucket. This is the documented escape hatch
// for the spinifex daemon.
func TestOwnership_ServiceAccountBypass(t *testing.T) {
	server := ownershipServer(t)
	status, nextCalled, _ := runMiddleware(t, server, signedReq(t, http.MethodGet, "/owner-bucket", keyConfig))
	assert.True(t, nextCalled)
	assert.Equal(t, http.StatusOK, status)
}

// Owner with no IAM policy: IAM check runs first and denies, so the request
// is rejected before ownership is consulted at all.
func TestOwnership_OwnerWithoutIAMPolicyDenied(t *testing.T) {
	server := ownershipServer(t)
	status, nextCalled, body := runMiddleware(t, server, signedReq(t, http.MethodGet, "/owner-bucket", keyNoIAM))
	assert.False(t, nextCalled)
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "AccessDenied")
}

// CreateBucket (PUT /{bucket}) skips the ownership check because there is no
// existing owner to compare against.
func TestOwnership_CreateBucketSkipsOwnershipCheck(t *testing.T) {
	server := ownershipServer(t)
	// "fresh-bucket" does not exist in stubBackend or config — without the skip,
	// resolveBucketMetadata would still succeed (returning nil), but a PUT must
	// reach the route handler regardless.
	req := signedReq(t, http.MethodPut, "/fresh-bucket", keyOther)
	status, nextCalled, _ := runMiddleware(t, server, req)
	assert.True(t, nextCalled)
	assert.Equal(t, http.StatusOK, status)
}

// ListAllMyBuckets has no bucket component so the ownership check is a no-op,
// and the handler runs with the caller's AccountID, returning only their
// buckets. With our stub, account 000000000002 owns nothing.
func TestOwnership_ListAllMyBucketsScopedByAccount(t *testing.T) {
	server := ownershipServer(t)
	req := signedReq(t, http.MethodGet, "/", keyOther)
	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
	body, _ := io.ReadAll(rr.Body)
	// The other account owns no buckets; XML body should contain no <Bucket> entry.
	assert.NotContains(t, string(body), "<Name>owner-bucket</Name>")
	assert.NotContains(t, string(body), "<Name>predastore</Name>")
}
