package s3

import (
	"net/http"
	"strings"

	"github.com/mulgadc/predastore/backend"
)

// bucketAccessAllowed enforces the S3 default-deny ownership invariant on top
// of an already-allowed IAM policy decision. Same-account callers pass; the
// public flag opens read-only anonymous access (GET/HEAD only — never writes);
// SkipPolicyCheck callers (config-defined service accounts) bypass entirely.
// Cross-account writes require a resource-based grant (bucket policy / ACL)
// which is not yet wired up here.
func bucketAccessAllowed(method, callerAccountID string, meta *backend.BucketMetadata, skipPolicyCheck bool) bool {
	if skipPolicyCheck {
		return true
	}
	if meta == nil {
		return false
	}
	if meta.AccountID != "" && callerAccountID == meta.AccountID {
		return true
	}
	if meta.Public && (method == http.MethodGet || method == http.MethodHead) {
		return true
	}
	// TODO: bucket-policy / ACL evaluation for cross-account grants.
	return false
}

// parseS3Path splits an S3 request URL path into bucket and object key. Returns
// ("", "") for the root (ListAllMyBuckets) and (bucket, "") for bucket-level
// requests. A trailing slash after the bucket is treated as bucket-only.
func parseS3Path(path string) (bucket, key string) {
	cleanPath := strings.TrimPrefix(path, "/")
	if cleanPath == "" {
		return "", ""
	}
	if before, after, ok := strings.Cut(cleanPath, "/"); ok {
		return before, after
	}
	return cleanPath, ""
}

// s3Action maps HTTP method + path to the corresponding IAM S3 action.
func s3Action(method, path string) string {
	bucket, key := parseS3Path(path)
	hasKey := key != ""

	switch method {
	case http.MethodGet:
		if bucket == "" {
			return "s3:ListAllMyBuckets"
		}
		if hasKey {
			return "s3:GetObject"
		}
		return "s3:ListBucket"
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
	cleanPath := strings.TrimPrefix(path, "/")
	if cleanPath == "" {
		// ListAllMyBuckets — use ARN wildcard so policies with
		// Resource: "arn:aws:s3:::*" match correctly.
		return "arn:aws:s3:::*"
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
			stmt := &policies[i].Statement[j] //nolint:gosec // G602: j is bounded by range, no out-of-bounds risk

			// Actions are case-insensitive per AWS IAM spec.
			if !matchesAnyPattern(stmt.Action, action, true) {
				continue
			}
			// Resource ARNs are case-sensitive per AWS IAM spec.
			if !matchesAnyPattern(stmt.Resource, resource, false) {
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
func matchesAnyPattern(patterns []string, value string, caseInsensitive bool) bool {
	for _, p := range patterns {
		if matchWildcardPattern(p, value, caseInsensitive) {
			return true
		}
	}
	return false
}

// matchWildcardPattern performs simple wildcard matching where "*" can appear
// at the end of a pattern as a suffix wildcard, or alone to match everything.
// When caseInsensitive is true, matching ignores case (used for IAM actions).
// When false, matching is exact (used for resource ARNs).
func matchWildcardPattern(pattern, value string, caseInsensitive bool) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		prefix := pattern[:len(pattern)-1]
		if caseInsensitive {
			return strings.HasPrefix(strings.ToLower(value), strings.ToLower(prefix))
		}
		return strings.HasPrefix(value, prefix)
	}
	if caseInsensitive {
		return strings.EqualFold(pattern, value)
	}
	return pattern == value
}
