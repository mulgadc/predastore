package s3

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// --- s3Action tests ---

func TestS3Action(t *testing.T) {
	tests := []struct {
		method string
		path   string
		want   string
	}{
		{"GET", "/", "s3:ListAllMyBuckets"},
		{"GET", "/my-bucket", "s3:ListBucket"},
		{"GET", "/my-bucket/key.txt", "s3:GetObject"},
		{"HEAD", "/my-bucket/key.txt", "s3:GetObject"},
		{"HEAD", "/my-bucket", "s3:ListBucket"},
		{"PUT", "/my-bucket", "s3:CreateBucket"},
		{"PUT", "/my-bucket/key.txt", "s3:PutObject"},
		{"POST", "/my-bucket/key.txt", "s3:PutObject"},
		{"DELETE", "/my-bucket", "s3:DeleteBucket"},
		{"DELETE", "/my-bucket/key.txt", "s3:DeleteObject"},
		{"PATCH", "/my-bucket/key.txt", ""},
	}

	for _, tt := range tests {
		got := s3Action(tt.method, tt.path)
		assert.Equal(t, tt.want, got, "s3Action(%q, %q)", tt.method, tt.path)
	}
}

// --- s3Resource tests ---

func TestS3Resource(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"/", "*"},
		{"/my-bucket", "arn:aws:s3:::my-bucket"},
		{"/my-bucket/key.txt", "arn:aws:s3:::my-bucket/key.txt"},
		{"/my-bucket/path/to/key.txt", "arn:aws:s3:::my-bucket/path/to/key.txt"},
	}

	for _, tt := range tests {
		got := s3Resource(tt.path)
		assert.Equal(t, tt.want, got, "s3Resource(%q)", tt.path)
	}
}

// --- evaluateS3Access tests ---

func doc(effect, action, resource string) iamPolicyDocument {
	return iamPolicyDocument{
		Version: "2012-10-17",
		Statement: []iamStatement{
			{Effect: effect, Action: iamStringOrArr{action}, Resource: iamStringOrArr{resource}},
		},
	}
}

func TestEvaluateS3Access_DefaultDeny(t *testing.T) {
	assert.False(t, evaluateS3Access("s3:GetObject", "*", nil))
	assert.False(t, evaluateS3Access("s3:GetObject", "*", []iamPolicyDocument{}))
}

func TestEvaluateS3Access_ExplicitAllow(t *testing.T) {
	policies := []iamPolicyDocument{
		doc("Allow", "s3:GetObject", "*"),
	}
	assert.True(t, evaluateS3Access("s3:GetObject", "*", policies))
}

func TestEvaluateS3Access_ExplicitDenyWins(t *testing.T) {
	policies := []iamPolicyDocument{
		doc("Allow", "s3:*", "*"),
		doc("Deny", "s3:DeleteObject", "*"),
	}
	assert.False(t, evaluateS3Access("s3:DeleteObject", "*", policies))
	assert.True(t, evaluateS3Access("s3:GetObject", "*", policies))
}

func TestEvaluateS3Access_WildcardAll(t *testing.T) {
	policies := []iamPolicyDocument{
		doc("Allow", "*", "*"),
	}
	assert.True(t, evaluateS3Access("s3:GetObject", "*", policies))
	assert.True(t, evaluateS3Access("s3:PutObject", "*", policies))
	assert.True(t, evaluateS3Access("s3:DeleteBucket", "*", policies))
}

func TestEvaluateS3Access_ServiceWildcard(t *testing.T) {
	policies := []iamPolicyDocument{
		doc("Allow", "s3:*", "*"),
	}
	assert.True(t, evaluateS3Access("s3:GetObject", "*", policies))
	assert.True(t, evaluateS3Access("s3:PutObject", "*", policies))
}

func TestEvaluateS3Access_ResourceScoped(t *testing.T) {
	policies := []iamPolicyDocument{
		doc("Allow", "s3:GetObject", "arn:aws:s3:::my-bucket/*"),
	}
	assert.True(t, evaluateS3Access("s3:GetObject", "arn:aws:s3:::my-bucket/key.txt", policies))
	assert.False(t, evaluateS3Access("s3:GetObject", "arn:aws:s3:::other-bucket/key.txt", policies))
}

func TestEvaluateS3Access_NoMatchingAction(t *testing.T) {
	policies := []iamPolicyDocument{
		doc("Allow", "s3:GetObject", "*"),
	}
	assert.False(t, evaluateS3Access("s3:PutObject", "*", policies))
}

func TestEvaluateS3Access_PrefixWildcard(t *testing.T) {
	policies := []iamPolicyDocument{
		doc("Allow", "s3:Get*", "*"),
	}
	assert.True(t, evaluateS3Access("s3:GetObject", "*", policies))
	assert.False(t, evaluateS3Access("s3:PutObject", "*", policies))
}

func TestEvaluateS3Access_CaseInsensitive(t *testing.T) {
	policies := []iamPolicyDocument{
		doc("Allow", "S3:GetObject", "*"),
	}
	assert.True(t, evaluateS3Access("s3:GetObject", "*", policies))
}

// --- matchWildcardPattern tests ---

func TestMatchWildcardPattern(t *testing.T) {
	tests := []struct {
		pattern string
		value   string
		want    bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"s3:*", "s3:GetObject", true},
		{"s3:*", "ec2:RunInstances", false},
		{"s3:Get*", "s3:GetObject", true},
		{"s3:Get*", "s3:PutObject", false},
		{"s3:GetObject", "s3:GetObject", true},
		{"s3:GetObject", "s3:PutObject", false},
		{"S3:GetObject", "s3:GetObject", true},
	}

	for _, tt := range tests {
		got := matchWildcardPattern(tt.pattern, tt.value)
		assert.Equal(t, tt.want, got, "matchWildcardPattern(%q, %q)", tt.pattern, tt.value)
	}
}
