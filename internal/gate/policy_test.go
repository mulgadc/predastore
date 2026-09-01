package gate

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/iampolicy"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/stretchr/testify/assert"
)

// --- s3Action tests ---

func TestS3Action(t *testing.T) {
	tests := []struct {
		method string
		target string
		bucket string
		key    string
		want   string
	}{
		{"GET", "/", "", "", "s3:ListAllMyBuckets"},
		{"GET", "/my-bucket", "my-bucket", "", "s3:ListBucket"},
		{"GET", "/my-bucket/key.txt", "my-bucket", "key.txt", "s3:GetObject"},
		{"HEAD", "/my-bucket/key.txt", "my-bucket", "key.txt", "s3:GetObject"},
		{"HEAD", "/my-bucket", "my-bucket", "", "s3:ListBucket"},
		{"PUT", "/my-bucket", "my-bucket", "", "s3:CreateBucket"},
		{"PUT", "/my-bucket/key.txt", "my-bucket", "key.txt", "s3:PutObject"},
		{"POST", "/my-bucket/key.txt?uploads", "my-bucket", "key.txt", "s3:PutObject"},
		{"DELETE", "/my-bucket", "my-bucket", "", "s3:DeleteBucket"},
		{"DELETE", "/my-bucket/key.txt", "my-bucket", "key.txt", "s3:DeleteObject"},
		{"PATCH", "/my-bucket/key.txt", "my-bucket", "key.txt", ""},
		// The batch delete is a POST that names no key. Mapping it to
		// s3:PutObject with the other POSTs would let a principal holding only
		// write access empty a bucket.
		{"POST", "/my-bucket?delete", "my-bucket", "", "s3:DeleteObject"},
		{"POST", "/my-bucket?delete=", "my-bucket", "", "s3:DeleteObject"},
	}

	for _, tt := range tests {
		r := httptest.NewRequest(tt.method, tt.target, nil)
		got := s3Action(r, tt.bucket, tt.key)
		assert.Equal(t, tt.want, got, "s3Action(%q %q, %q, %q)", tt.method, tt.target, tt.bucket, tt.key)
	}
}

// --- s3Resource tests ---

func TestS3Resource(t *testing.T) {
	tests := []struct {
		bucket string
		key    string
		want   string
	}{
		{"", "", "arn:aws:s3:::*"},
		{"my-bucket", "", "arn:aws:s3:::my-bucket"},
		{"my-bucket", "key.txt", "arn:aws:s3:::my-bucket/key.txt"},
		{"my-bucket", "path/to/key.txt", "arn:aws:s3:::my-bucket/path/to/key.txt"},
	}

	for _, tt := range tests {
		got := s3Resource(tt.bucket, tt.key)
		assert.Equal(t, tt.want, got, "s3Resource(%q, %q)", tt.bucket, tt.key)
	}
}

// --- S3 access evaluation via the shared iampolicy.Evaluate ---
//
// The wildcard matcher and the deny-wins algorithm now live in pkg/iampolicy and
// are unit-tested there; these cases pin the S3-flavoured behaviour end to end.

func doc(effect, action, resource string) iampolicy.PolicyDocument {
	return iampolicy.PolicyDocument{
		Version: "2012-10-17",
		Statement: []iampolicy.Statement{
			{Effect: effect, Action: iampolicy.StringOrArr{action}, Resource: iampolicy.StringOrArr{resource}},
		},
	}
}

// allowed reports whether the S3 action on resource is permitted.
func allowed(action, resource string, policies []iampolicy.PolicyDocument) bool {
	return iampolicy.EvaluateWithKeys(action, resource, policies, nil) == iampolicy.Allow
}

// The batch delete is authorized as a delete of every key in the bucket. Write
// access alone must not reach it, and delete access on one key must not carry
// the whole batch.
func TestEvaluateS3Access_BulkDelete(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/my-bucket?delete=", nil)
	action := s3Action(r, "my-bucket", "")
	resource := s3Resource("my-bucket", "*")

	assert.Equal(t, "s3:DeleteObject", action)
	assert.Equal(t, "arn:aws:s3:::my-bucket/*", resource)

	writeOnly := []iampolicy.PolicyDocument{doc("Allow", "s3:PutObject", "arn:aws:s3:::my-bucket/*")}
	assert.False(t, allowed(action, resource, writeOnly))

	oneKey := []iampolicy.PolicyDocument{doc("Allow", "s3:DeleteObject", "arn:aws:s3:::my-bucket/one.txt")}
	assert.False(t, allowed(action, resource, oneKey))

	bucketWide := []iampolicy.PolicyDocument{doc("Allow", "s3:DeleteObject", "arn:aws:s3:::my-bucket/*")}
	assert.True(t, allowed(action, resource, bucketWide))
}

func TestEvaluateS3Access_DefaultDeny(t *testing.T) {
	assert.False(t, allowed("s3:GetObject", "*", nil))
	assert.False(t, allowed("s3:GetObject", "*", []iampolicy.PolicyDocument{}))
}

func TestEvaluateS3Access_ExplicitAllow(t *testing.T) {
	policies := []iampolicy.PolicyDocument{
		doc("Allow", "s3:GetObject", "*"),
	}
	assert.True(t, allowed("s3:GetObject", "*", policies))
}

func TestEvaluateS3Access_ExplicitDenyWins(t *testing.T) {
	policies := []iampolicy.PolicyDocument{
		doc("Allow", "s3:*", "*"),
		doc("Deny", "s3:DeleteObject", "*"),
	}
	assert.False(t, allowed("s3:DeleteObject", "*", policies))
	assert.True(t, allowed("s3:GetObject", "*", policies))
}

func TestEvaluateS3Access_WildcardAll(t *testing.T) {
	policies := []iampolicy.PolicyDocument{
		doc("Allow", "*", "*"),
	}
	assert.True(t, allowed("s3:GetObject", "*", policies))
	assert.True(t, allowed("s3:PutObject", "*", policies))
	assert.True(t, allowed("s3:DeleteBucket", "*", policies))
}

func TestEvaluateS3Access_ServiceWildcard(t *testing.T) {
	policies := []iampolicy.PolicyDocument{
		doc("Allow", "s3:*", "*"),
	}
	assert.True(t, allowed("s3:GetObject", "*", policies))
	assert.True(t, allowed("s3:PutObject", "*", policies))
}

func TestEvaluateS3Access_ResourceScoped(t *testing.T) {
	policies := []iampolicy.PolicyDocument{
		doc("Allow", "s3:GetObject", "arn:aws:s3:::my-bucket/*"),
	}
	assert.True(t, allowed("s3:GetObject", "arn:aws:s3:::my-bucket/key.txt", policies))
	assert.False(t, allowed("s3:GetObject", "arn:aws:s3:::other-bucket/key.txt", policies))
}

func TestEvaluateS3Access_NoMatchingAction(t *testing.T) {
	policies := []iampolicy.PolicyDocument{
		doc("Allow", "s3:GetObject", "*"),
	}
	assert.False(t, allowed("s3:PutObject", "*", policies))
}

func TestEvaluateS3Access_PrefixWildcard(t *testing.T) {
	policies := []iampolicy.PolicyDocument{
		doc("Allow", "s3:Get*", "*"),
	}
	assert.True(t, allowed("s3:GetObject", "*", policies))
	assert.False(t, allowed("s3:PutObject", "*", policies))
}

func TestEvaluateS3Access_CaseInsensitiveAction(t *testing.T) {
	policies := []iampolicy.PolicyDocument{
		doc("Allow", "S3:GetObject", "*"),
	}
	assert.True(t, allowed("s3:GetObject", "*", policies))
}

// --- conditionKeys tests ---

func TestConditionKeys_PopulatesEverySupportedKey(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/my-bucket?prefix=home/alice/", nil)
	r.RemoteAddr = "10.4.1.9:52344"
	r.TLS = &tls.ConnectionState{}
	cred := &auth.CredentialResult{AccountID: "000000000001", UserName: "alice", PrincipalType: "user"}

	keys := conditionKeys(r, "s3:ListBucket", cred)

	assert.Equal(t, iampolicy.ConditionKeys{
		iampolicy.KeySourceIP:         "10.4.1.9",
		iampolicy.KeySecureTransport:  "true",
		iampolicy.KeyUsername:         "alice",
		iampolicy.KeyPrincipalAccount: "000000000001",
		iampolicy.KeyS3Prefix:         "home/alice/",
	}, keys)
}

// RoleSessionName is chosen by the caller of AssumeRole and lands in UserName
// for a session, so aws:username must stay absent — otherwise anyone permitted
// to assume the role satisfies the condition just by naming their session.
func TestConditionKeys_OmitsUsernameForAssumedRole(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/my-bucket/obj", nil)
	r.RemoteAddr = "10.4.1.9:52344"
	cred := &auth.CredentialResult{
		AccountID:     "000000000001",
		UserName:      "alice",
		PrincipalType: "assumed-role",
	}

	keys := conditionKeys(r, "s3:GetObject", cred)

	assert.NotContains(t, keys, iampolicy.KeyUsername)
	assert.Equal(t, "000000000001", keys[iampolicy.KeyPrincipalAccount])
}

// An empty value would compare as a real value that matches nothing, which on a
// Deny widens access instead of narrowing it. Absent is the only safe reading.
func TestConditionKeys_OmitsEmptyValues(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/my-bucket/obj", nil)
	r.RemoteAddr = ""

	keys := conditionKeys(r, "s3:GetObject", &auth.CredentialResult{PrincipalType: "user"})

	assert.NotContains(t, keys, iampolicy.KeySourceIP)
	assert.NotContains(t, keys, iampolicy.KeyUsername)
	assert.NotContains(t, keys, iampolicy.KeyPrincipalAccount)
}

// s3:prefix exists only for a bucket listing; on any other action the key must
// be absent, so a condition on it evaluates false rather than matching "".
func TestConditionKeys_PrefixOnlyForListBucket(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/my-bucket/obj?prefix=home/", nil)
	r.RemoteAddr = "10.4.1.9:52344"
	cred := &auth.CredentialResult{AccountID: "000000000001", UserName: "alice"}

	keys := conditionKeys(r, "s3:GetObject", cred)

	assert.NotContains(t, keys, iampolicy.KeyS3Prefix)
	assert.Equal(t, "false", keys[iampolicy.KeySecureTransport])
}

func TestConditionKeys_SourceIPForms(t *testing.T) {
	tests := map[string]string{
		"10.4.1.9:52344":    "10.4.1.9",
		"[2001:db8::1]:443": "2001:db8::1",
		// No port to strip — pass the address through rather than dropping it.
		"10.4.1.9": "10.4.1.9",
	}
	for remoteAddr, want := range tests {
		t.Run(remoteAddr, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/my-bucket/obj", nil)
			r.RemoteAddr = remoteAddr

			keys := conditionKeys(r, "s3:GetObject", &auth.CredentialResult{})

			assert.Equal(t, want, keys[iampolicy.KeySourceIP])
		})
	}
}
