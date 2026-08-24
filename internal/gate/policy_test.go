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
		bucket string
		key    string
		want   string
	}{
		{"GET", "", "", "s3:ListAllMyBuckets"},
		{"GET", "my-bucket", "", "s3:ListBucket"},
		{"GET", "my-bucket", "key.txt", "s3:GetObject"},
		{"HEAD", "my-bucket", "key.txt", "s3:GetObject"},
		{"HEAD", "my-bucket", "", "s3:ListBucket"},
		{"PUT", "my-bucket", "", "s3:CreateBucket"},
		{"PUT", "my-bucket", "key.txt", "s3:PutObject"},
		{"POST", "my-bucket", "key.txt", "s3:PutObject"},
		{"DELETE", "my-bucket", "", "s3:DeleteBucket"},
		{"DELETE", "my-bucket", "key.txt", "s3:DeleteObject"},
		{"PATCH", "my-bucket", "key.txt", ""},
	}

	for _, tt := range tests {
		got := s3Action(tt.method, tt.bucket, tt.key)
		assert.Equal(t, tt.want, got, "s3Action(%q, %q, %q)", tt.method, tt.bucket, tt.key)
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
	return iampolicy.Evaluate(action, resource, policies) == iampolicy.Allow
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
	cred := &auth.CredentialResult{AccountID: "000000000001", UserName: "alice"}

	keys := conditionKeys(r, actionListBucket, cred)

	assert.Equal(t, iampolicy.ConditionKeys{
		iampolicy.KeySourceIP:         "10.4.1.9",
		iampolicy.KeySecureTransport:  "true",
		iampolicy.KeyUsername:         "alice",
		iampolicy.KeyPrincipalAccount: "000000000001",
		iampolicy.KeyS3Prefix:         "home/alice/",
	}, keys)
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

func TestClientIP(t *testing.T) {
	assert.Equal(t, "10.4.1.9", clientIP("10.4.1.9:52344"))
	assert.Equal(t, "2001:db8::1", clientIP("[2001:db8::1]:443"))
	// No port to strip — pass the address through rather than dropping it.
	assert.Equal(t, "10.4.1.9", clientIP("10.4.1.9"))
}
