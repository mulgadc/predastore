package auth

import "context"

// Entry is one config-defined service account. The root package parses the
// on-disk form and converts it to this.
type Entry struct {
	AccessKeyID     string
	SecretAccessKey string
	AccountID       string
	Policy          []PolicyRule
}

// Action						Meaning
// s3:ListBucket				List objects in a bucket
// s3:GetObject					Download (read) an object
// s3:PutObject					Upload or overwrite an object
// s3:DeleteObject				Delete an object
// s3:ListAllMyBuckets			List all buckets visible to a user
// s3:GetBucketAcl / PutAcl		(TODO) Manage access control for buckets

// PolicyRule grants a config-defined account a set of actions on a bucket.
type PolicyRule struct {
	Bucket  string   // Can be "*" or bucket name
	Actions []string // Like "s3:GetObject", "s3:PutObject", or "s3:*"
}

// IAMConfig configures IAM authentication via NATS KV.
type IAMConfig struct {
	NATSUrl          string
	NATSToken        string
	MasterKeyPath    string
	AccessKeysBucket string
}

// contextKey namespaces the values the auth middleware attaches to a request.
type contextKey string

const (
	// ContextKeyAccessKeyID is the context key for the authenticated user's access key ID.
	ContextKeyAccessKeyID contextKey = "accessKeyID"
	// ContextKeyAccountID is the context key for the authenticated user's account ID.
	ContextKeyAccountID contextKey = "accountID"
	// ContextKeyServiceAccount marks the caller as a config-defined service
	// account, the same credentials that carry SkipPolicyCheck.
	ContextKeyServiceAccount contextKey = "serviceAccount"
)

// AccessKeyID returns the authenticated access key attached to ctx, or "" when
// the request was not authenticated.
func AccessKeyID(ctx context.Context) string {
	v, _ := ctx.Value(ContextKeyAccessKeyID).(string)
	return v
}

// AccountID returns the authenticated account attached to ctx, or "" when the
// request was not authenticated.
func AccountID(ctx context.Context) string {
	v, _ := ctx.Value(ContextKeyAccountID).(string)
	return v
}

// IsServiceAccount reports whether ctx carries a config-defined service
// credential. It defaults to false, so an unauthenticated request or one that
// never passed through the auth middleware is never treated as trusted.
func IsServiceAccount(ctx context.Context) bool {
	v, _ := ctx.Value(ContextKeyServiceAccount).(bool)
	return v
}
