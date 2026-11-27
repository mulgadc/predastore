package gate

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
)

// A config entry that declares policy rules must be enforced: the entry below
// grants only s3:GetObject on one bucket, so its own account's bucket refuses
// a PUT, and the granted bucket serves a GET.
func TestConfigAuthPolicy_ScopedEntryEnforced(t *testing.T) {
	server := newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta: newFakeMeta(t,
			model.BucketMetadata{Name: "other-bucket", Region: "ap-southeast-2", AccountID: "000000000002"},
			model.BucketMetadata{Name: "backup", Region: "ap-southeast-2", AccountID: "000000000002"},
		),
		Auth: []auth.Entry{{
			AccessKeyID:     "AKIASCOPE",
			SecretAccessKey: secret,
			AccountID:       "000000000002",
			Policy:          []auth.PolicyRule{{Bucket: "backup", Actions: []string{"s3:GetObject"}}},
		}},
	})

	status, nextCalled, body := runMiddleware(t, server, signedReq(t, http.MethodPut, "/other-bucket/x", "AKIASCOPE"))
	assert.False(t, nextCalled, "a PUT outside the declared policy must not reach the handler")
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "AccessDenied")

	status, nextCalled, _ = runMiddleware(t, server, signedReq(t, http.MethodGet, "/backup/keys.db", "AKIASCOPE"))
	assert.True(t, nextCalled, "the declared action on the declared bucket must pass")
	assert.Equal(t, http.StatusOK, status)
}

// A scoped key copying between buckets its policy does not both cover is
// decided by the source: both buckets are the entry's own account, so
// ownership passes everywhere, and the denial is purely the policy's.
func TestConfigAuthPolicy_CopySourcePolicyIsolated(t *testing.T) {
	server := newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta: newFakeMeta(t,
			model.BucketMetadata{Name: "backup", Region: "ap-southeast-2", AccountID: "000000000002"},
			model.BucketMetadata{Name: "other-bucket", Region: "ap-southeast-2", AccountID: "000000000002"},
		),
		Auth: []auth.Entry{{
			AccessKeyID:     "AKIASCOPE",
			SecretAccessKey: secret,
			AccountID:       "000000000002",
			Policy:          []auth.PolicyRule{{Bucket: "backup", Actions: []string{"s3:GetObject", "s3:PutObject"}}},
		}},
	})

	// Destination granted, source not: the source read must be refused.
	req := httptest.NewRequest(http.MethodPut, "/backup/stolen", nil)
	req.Header.Set("X-Amz-Copy-Source", "/other-bucket/secret")
	signTestReq(t, req, nil, "AKIASCOPE", secret, "ap-southeast-2", "s3")
	status, nextCalled, body := runMiddleware(t, server, req)
	assert.False(t, nextCalled, "a copy whose source the policy does not grant must not proceed")
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "AccessDenied")

	// Source and destination both granted: the copy passes the gate.
	req = httptest.NewRequest(http.MethodPut, "/backup/copy", nil)
	req.Header.Set("X-Amz-Copy-Source", "/backup/keys.db")
	signTestReq(t, req, nil, "AKIASCOPE", secret, "ap-southeast-2", "s3")
	status, nextCalled, _ = runMiddleware(t, server, req)
	assert.True(t, nextCalled, "a fully granted copy must pass")
	assert.Equal(t, http.StatusOK, status)
}

// UploadPartCopy reaches the same source decision as CopyObject: the
// destination write is granted, the source read is not, and the part copy is
// refused on the source alone.
func TestConfigAuthPolicy_UploadPartCopySourcePolicyIsolated(t *testing.T) {
	server := newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta: newFakeMeta(t,
			model.BucketMetadata{Name: "backup", Region: "ap-southeast-2", AccountID: "000000000002"},
			model.BucketMetadata{Name: "other-bucket", Region: "ap-southeast-2", AccountID: "000000000002"},
		),
		Auth: []auth.Entry{{
			AccessKeyID:     "AKIASCOPE",
			SecretAccessKey: secret,
			AccountID:       "000000000002",
			Policy:          []auth.PolicyRule{{Bucket: "backup", Actions: []string{"s3:GetObject", "s3:PutObject"}}},
		}},
	})

	req := httptest.NewRequest(http.MethodPut, "/backup/part?partNumber=1&uploadId=up1", nil)
	req.Header.Set("X-Amz-Copy-Source", "/other-bucket/secret")
	signTestReq(t, req, nil, "AKIASCOPE", secret, "ap-southeast-2", "s3")
	status, nextCalled, body := runMiddleware(t, server, req)
	assert.False(t, nextCalled, "a part copy whose source the policy does not grant must not proceed")
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "AccessDenied")

	req = httptest.NewRequest(http.MethodPut, "/backup/part?partNumber=1&uploadId=up1", nil)
	req.Header.Set("X-Amz-Copy-Source", "/backup/keys.db")
	signTestReq(t, req, nil, "AKIASCOPE", secret, "ap-southeast-2", "s3")
	status, nextCalled, _ = runMiddleware(t, server, req)
	assert.True(t, nextCalled, "a fully granted part copy must pass")
	assert.Equal(t, http.StatusOK, status)
}

// Ownership is a second, independent gate: with a wildcard policy that grants
// every action, a private foreign-owned destination and source are still
// refused, and the refusal is the ownership check's alone.
func TestConfigAuthPolicy_OwnershipIsIndependentOfPolicy(t *testing.T) {
	server := newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta: newFakeMeta(t,
			model.BucketMetadata{Name: "backup", Region: "ap-southeast-2", AccountID: "000000000002"},
			model.BucketMetadata{Name: "foreign", Region: "ap-southeast-2", AccountID: "000000000009"},
		),
		Auth: []auth.Entry{{
			AccessKeyID:     "AKIAWILD",
			SecretAccessKey: secret,
			AccountID:       "000000000002",
			Policy:          []auth.PolicyRule{{Bucket: "*", Actions: []string{"s3:*"}}},
		}},
	})

	status, nextCalled, body := runMiddleware(t, server, signedReq(t, http.MethodPut, "/foreign/x", "AKIAWILD"))
	assert.False(t, nextCalled, "a wildcard policy must not override foreign ownership")
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "AccessDenied")

	req := httptest.NewRequest(http.MethodPut, "/backup/stolen", nil)
	req.Header.Set("X-Amz-Copy-Source", "/foreign/secret")
	signTestReq(t, req, nil, "AKIAWILD", secret, "ap-southeast-2", "s3")
	status, nextCalled, body = runMiddleware(t, server, req)
	assert.False(t, nextCalled, "a wildcard policy must not read a private foreign source")
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "AccessDenied")

	req = httptest.NewRequest(http.MethodPut, "/backup/part?partNumber=1&uploadId=up1", nil)
	req.Header.Set("X-Amz-Copy-Source", "/foreign/secret")
	signTestReq(t, req, nil, "AKIAWILD", secret, "ap-southeast-2", "s3")
	status, nextCalled, body = runMiddleware(t, server, req)
	assert.False(t, nextCalled, "a part copy must not read a private foreign source either")
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "AccessDenied")
}

// The listing contract follows IAM: a scoped key's ListBuckets is evaluated
// against the wildcard resource like any credential's, so named-bucket rules
// cannot list, and the owner override stays a policy-less-entry feature.
func TestConfigAuthPolicy_ListBucketsContract(t *testing.T) {
	meta := newFakeMeta(t,
		model.BucketMetadata{Name: "mine-a", Region: "ap-southeast-2", AccountID: "000000000002"},
		model.BucketMetadata{Name: "mine-b", Region: "ap-southeast-2", AccountID: "000000000002"},
		model.BucketMetadata{Name: "theirs", Region: "ap-southeast-2", AccountID: "000000000003"},
	)
	scoped := newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta:   meta,
		Auth: []auth.Entry{{
			AccessKeyID:     "AKIASCOPE",
			SecretAccessKey: secret,
			AccountID:       "000000000002",
			Policy:          []auth.PolicyRule{{Bucket: "mine-a", Actions: []string{"s3:*"}}},
		}},
	})

	rr := httptest.NewRecorder()
	scoped.ServeHTTP(rr, signedReq(t, http.MethodGet, "/", "AKIASCOPE"))
	assert.Equal(t, http.StatusForbidden, rr.Code,
		"a bucket-scoped policy cannot grant the store-wide ListAllMyBuckets action")

	// A wildcard grant authorizes the listing, and the owner override is
	// refused to it: the response names only the caller's own buckets.
	wildcard := newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta:   meta,
		Auth: []auth.Entry{{
			AccessKeyID:     "AKIAWILD",
			SecretAccessKey: secret,
			AccountID:       "000000000002",
			Policy:          []auth.PolicyRule{{Bucket: "*", Actions: []string{"s3:*"}}},
		}},
	})

	req := signedReq(t, http.MethodGet, "/", "AKIAWILD")
	req.Header.Set(handlers.OwnerAccountHeader, "000000000003")
	rr = httptest.NewRecorder()
	wildcard.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "mine-a", "a wildcard grant lists the caller's own buckets")
	assert.NotContains(t, rr.Body.String(), "theirs", "the override must not name another account for a scoped key")

	unrestricted := newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta:   meta,
		Auth: []auth.Entry{{
			AccessKeyID:     "AKIAROOT",
			SecretAccessKey: secret,
			AccountID:       "000000000002",
		}},
	})

	req = signedReq(t, http.MethodGet, "/", "AKIAROOT")
	req.Header.Set(handlers.OwnerAccountHeader, "000000000003")
	rr = httptest.NewRecorder()
	unrestricted.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "theirs", "the override names another account's buckets")
	assert.NotContains(t, rr.Body.String(), "mine-a", "the caller's own buckets stay out of an overridden listing")
}
