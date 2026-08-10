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

// s3Action maps an HTTP method and the resolved bucket/key to the IAM S3 action.
func s3Action(method, bucket, key string) string {
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

// s3Resource builds the ARN for the resolved bucket/key.
func s3Resource(bucket, key string) string {
	if bucket == "" {
		// ListAllMyBuckets — use ARN wildcard so policies with
		// Resource: "arn:aws:s3:::*" match correctly.
		return "arn:aws:s3:::*"
	}
	if key == "" {
		return "arn:aws:s3:::" + bucket
	}
	return "arn:aws:s3:::" + bucket + "/" + key
}
