package iampolicy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMatchWildcard exercises the case-sensitive infix matcher directly. Case
// folding is the caller's job (via matchesAny), so mixed-case here must NOT match.
func TestMatchWildcard(t *testing.T) {
	tests := []struct {
		pattern string
		value   string
		want    bool
	}{
		// Global wildcard.
		{"*", "anything", true},
		{"*", "", true},

		// Service wildcard (case-sensitive at this level).
		{"ec2:*", "ec2:RunInstances", true},
		{"ec2:*", "ec2:DescribeInstances", true},
		{"ec2:*", "s3:GetObject", false},
		{"EC2:*", "ec2:RunInstances", false},

		// Prefix wildcard.
		{"s3:Get*", "s3:GetObject", true},
		{"s3:Get*", "s3:GetBucketPolicy", true},
		{"s3:Get*", "s3:PutObject", false},

		// Exact match (case-sensitive).
		{"ec2:RunInstances", "ec2:RunInstances", true},
		{"ec2:RunInstances", "ec2:StopInstances", false},
		{"ec2:RunInstances", "EC2:RunInstances", false},

		// Infix wildcards (AWS IAM-style — required for iam:PassRole ARN matching).
		{"arn:aws:iam::*:role/app-*", "arn:aws:iam::123456789012:role/app-foo", true},
		{"arn:aws:iam::*:role/app-*", "arn:aws:iam::999999999999:role/app-bar", true},
		{"arn:aws:iam::*:role/*", "arn:aws:iam::123456789012:role/anything", true},
		{"arn:aws:iam::123456789012:role/app-*", "arn:aws:iam::123456789012:role/app-foo", true},
		{"arn:aws:iam::*:role/app-*", "arn:aws:iam::123456789012:role/admin-foo", false},
		{"arn:aws:iam::*:role/app-*", "arn:aws:iam::123456789012:user/app-foo", false},
		{"arn:aws:iam::*:role/app-*", "arn:aws:iam::123456789012:role/app-", true},
		{"a*b*c", "axxbyyc", true},
		{"a*b*c", "axxbyy", false},

		// S3 suffix ARNs, case-sensitive.
		{"arn:aws:s3:::my-bucket/*", "arn:aws:s3:::my-bucket/key.txt", true},
		{"arn:aws:s3:::my-bucket/*", "arn:aws:s3:::other-bucket/key.txt", false},
		{"arn:aws:s3:::MyBucket", "arn:aws:s3:::MyBucket", true},
		{"arn:aws:s3:::MyBucket", "arn:aws:s3:::mybucket", false},

		// Edge cases.
		{"", "", true},
		{"", "something", false},
	}

	for _, tt := range tests {
		got := MatchWildcard(tt.pattern, tt.value)
		assert.Equal(t, tt.want, got, "MatchWildcard(%q, %q)", tt.pattern, tt.value)
	}
}

// TestMatchesAny covers the case-fold flag: actions fold (true), resources are
// exact-case (false).
func TestMatchesAny(t *testing.T) {
	// Case-insensitive (actions).
	assert.True(t, matchesAny([]string{"EC2:*"}, "ec2:RunInstances", true))
	assert.True(t, matchesAny([]string{"ec2:runinstances"}, "ec2:RunInstances", true))
	assert.True(t, matchesAny([]string{"S3:get*"}, "s3:GetObject", true))
	assert.False(t, matchesAny([]string{"s3:*"}, "ec2:RunInstances", true))

	// Case-sensitive (resources).
	assert.True(t, matchesAny([]string{"arn:aws:s3:::MyBucket"}, "arn:aws:s3:::MyBucket", false))
	assert.False(t, matchesAny([]string{"arn:aws:s3:::MyBucket"}, "arn:aws:s3:::mybucket", false))

	// Any-of-many.
	assert.True(t, matchesAny([]string{"s3:Get*", "s3:Put*"}, "s3:PutObject", true))
	assert.False(t, matchesAny([]string{"s3:Get*", "s3:Put*"}, "s3:DeleteObject", true))

	// Empty pattern set never matches.
	assert.False(t, matchesAny(nil, "s3:GetObject", true))
}
