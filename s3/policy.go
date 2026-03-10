package s3

import (
	"net/http"
	"strings"
)

// s3ActionMap maps HTTP method + path pattern to IAM S3 actions.
func s3Action(method, path string) string {
	hasKey := false
	// Strip bucket name — if there's content after /{bucket}/, it's an object key
	cleanPath := path
	if len(cleanPath) > 0 && cleanPath[0] == '/' {
		cleanPath = cleanPath[1:]
	}
	if idx := strings.IndexByte(cleanPath, '/'); idx >= 0 && idx < len(cleanPath)-1 {
		hasKey = true
	}

	switch method {
	case http.MethodGet:
		if cleanPath == "" || !strings.Contains(cleanPath, "/") {
			// Root path = ListAllMyBuckets, bucket path = ListBucket
			if cleanPath == "" {
				return "s3:ListAllMyBuckets"
			}
			return "s3:ListBucket"
		}
		return "s3:GetObject"
	case http.MethodHead:
		if hasKey {
			return "s3:GetObject"
		}
		return "s3:ListBucket"
	case http.MethodPut:
		if hasKey {
			return "s3:PutObject"
		}
		return "s3:CreateBucket"
	case http.MethodPost:
		return "s3:PutObject" // multipart uploads
	case http.MethodDelete:
		if hasKey {
			return "s3:DeleteObject"
		}
		return "s3:DeleteBucket"
	default:
		return ""
	}
}

// s3Resource builds the ARN for the resource being accessed.
func s3Resource(path string) string {
	cleanPath := path
	if len(cleanPath) > 0 && cleanPath[0] == '/' {
		cleanPath = cleanPath[1:]
	}

	if cleanPath == "" {
		return "*"
	}

	// For bucket-level operations: arn:aws:s3:::bucket-name
	// For object-level operations: arn:aws:s3:::bucket-name/key
	return "arn:aws:s3:::" + cleanPath
}

// evaluateS3Access checks whether the given S3 action is allowed by the policy documents.
// Follows AWS evaluation order: explicit Deny wins, then explicit Allow, then implicit Deny.
func evaluateS3Access(action, resource string, policies []iamPolicyDocument) bool {
	hasAllow := false
	for i := range policies {
		for j := range policies[i].Statement {
			stmt := &policies[i].Statement[j]

			if !matchesAnyPattern(stmt.Action, action) {
				continue
			}
			if !matchesAnyPattern(stmt.Resource, resource) {
				continue
			}
			switch stmt.Effect {
			case "Deny":
				return false
			case "Allow":
				hasAllow = true
			}
		}
	}
	return hasAllow
}

// matchesAnyPattern returns true if any pattern matches the given value.
func matchesAnyPattern(patterns []string, value string) bool {
	for _, p := range patterns {
		if matchWildcardPattern(p, value) {
			return true
		}
	}
	return false
}

// matchWildcardPattern performs simple wildcard matching where "*" can appear
// at the end of a pattern as a suffix wildcard, or alone to match everything.
func matchWildcardPattern(pattern, value string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.ToLower(pattern[:len(pattern)-1])
		return strings.HasPrefix(strings.ToLower(value), prefix)
	}
	return strings.EqualFold(pattern, value)
}
