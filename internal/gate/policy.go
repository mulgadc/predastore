package gate

import (
	"net"
	"net/http"
	"strconv"

	"github.com/mulgadc/bluebottle/pkg/iampolicy"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/model"
)

// bucketAccessAllowed enforces the S3 default-deny ownership invariant on top
// of an already-allowed IAM policy decision. Same-account callers pass; the
// public flag opens read-only anonymous access (GET/HEAD only — never writes);
// SkipPolicyCheck callers (config-defined service accounts) bypass entirely.
// Cross-account writes require a resource-based grant (bucket policy / ACL)
// which is not yet wired up here.
func bucketAccessAllowed(method, callerAccountID string, meta *model.BucketMetadata, skipPolicyCheck bool) bool {
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

// isBulkDelete reports whether a request is POST /{bucket}?delete, the batch
// delete. It addresses no key, so without this it would take the multipart
// mapping below and a principal holding only s3:PutObject could empty a bucket.
func isBulkDelete(r *http.Request, key string) bool {
	return r.Method == http.MethodPost && key == "" && r.URL.Query().Has("delete")
}

// s3Action maps a request and the resolved bucket/key to the IAM S3 action.
// The request is read rather than the method alone because S3 distinguishes
// some operations by sub-resource rather than by method.
func s3Action(r *http.Request, bucket, key string) string {
	hasKey := key != ""

	switch r.Method {
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
		if isBulkDelete(r, key) {
			return "s3:DeleteObject"
		}
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

// conditionKeys resolves the IAM condition context keys for one S3 request.
// s3:prefix is set only for a bucket listing, matching AWS: on any other action
// the key is absent, which evaluates a condition on it false.
//
// Every key is omitted rather than set empty when unknown: an empty value reads
// as a real value that matches nothing, and on a Deny that silently widens
// access instead of narrowing it.
func conditionKeys(r *http.Request, action string, cred *auth.CredentialResult) iampolicy.ConditionKeys {
	// A RemoteAddr with no port is passed through rather than dropped.
	sourceIP := r.RemoteAddr
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		sourceIP = host
	}
	keys := iampolicy.ConditionKeys{
		iampolicy.KeySecureTransport: strconv.FormatBool(r.TLS != nil),
	}
	if sourceIP != "" {
		keys[iampolicy.KeySourceIP] = sourceIP
	}
	// aws:username is user-only in AWS. A role session's UserName is the
	// caller-chosen RoleSessionName, so gating authorization on it here would
	// let any principal that may assume the role satisfy the condition at will.
	if cred.IsIAMUser() && cred.UserName != "" {
		keys[iampolicy.KeyUsername] = cred.UserName
	}
	// aws:userid is safe for both principal types: neither an IAM user's unique
	// ID nor the role ID and session name STS minted is caller-chosen.
	if cred.UserID != "" {
		keys[iampolicy.KeyUserID] = cred.UserID
	}
	if cred.AccountID != "" {
		keys[iampolicy.KeyPrincipalAccount] = cred.AccountID
	}
	if action == "s3:ListBucket" {
		keys[iampolicy.KeyS3Prefix] = r.URL.Query().Get("prefix")
	}
	return keys
}
