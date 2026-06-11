package s3

import (
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/nats-io/nats.go"
)

// ErrKeyNotFound is returned when an access key does not exist in the provider.
var ErrKeyNotFound = errors.New("access key not found")

// CredentialResult is the result of a credential lookup.
type CredentialResult struct {
	SecretAccessKey string
	AccountID       string
	UserName        string
	// SkipPolicyCheck marks the caller as a trusted config-based service
	// account. It bypasses both the IAM policy check AND the bucket-ownership
	// check, granting unrestricted access to every bucket regardless of owner.
	// Adding an [[auth]] entry to predastore.toml therefore grants god-mode.
	SkipPolicyCheck bool
	PolicyDocuments []iamPolicyDocument // resolved policies (only for NATS-sourced credentials)
}

// CredentialProvider looks up credentials by access key ID.
type CredentialProvider interface {
	LookupCredentials(accessKeyID string) (*CredentialResult, error)
	Close()
}

// --- IAM types (subset replicated from spinifex/handlers/iam for NATS KV reads) ---

// iamAccessKey mirrors the spinifex IAM AccessKey stored in NATS KV.
type iamAccessKey struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"` // AES-256-GCM encrypted, base64-encoded
	UserName        string `json:"user_name"`
	AccountID       string `json:"account_id"`
	Status          string `json:"status"`
	CreatedAt       string `json:"created_at"`
}

// sessionCredential mirrors the spinifex STS SessionCredential stored in the
// spinifex-iam-session-credentials KV bucket. Records are write-once at mint
// and expire within hours. Only the fields predastore needs to resolve a
// session are replicated.
type sessionCredential struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretEncrypted string `json:"secret_encrypted"` // AES-256-GCM, base64 (handlers_iam.EncryptSecret format)
	AccountID       string `json:"account_id"`
	PrincipalType   string `json:"principal_type"` // "user" | "assumed-role" | ""
	SessionName     string `json:"session_name"`
	// UnderlyingRoleARN identifies the assumed role for "assumed-role" sessions;
	// SessionName carries the instance ID, not the role, so the role name is
	// parsed from this ARN. Empty for "user" sessions.
	UnderlyingRoleARN string    `json:"underlying_role_arn"`
	ExpiresAt         time.Time `json:"expires_at"`
}

// iamUser mirrors the spinifex IAM User stored in NATS KV.
type iamUser struct {
	UserName         string   `json:"user_name"`
	AccountID        string   `json:"account_id"`
	AttachedPolicies []string `json:"attached_policies"` // policy ARNs
}

// iamRole mirrors the spinifex IAM Role stored in NATS KV (spinifex-iam-roles).
// Only the fields predastore needs to resolve an assumed-role session's
// permissions are replicated. The role's assume_role_policy_document is the
// trust policy (who may assume the role), not a permission policy, so it is
// deliberately omitted here.
type iamRole struct {
	RoleName         string   `json:"role_name"`
	AccountID        string   `json:"account_id"`
	AttachedPolicies []string `json:"attached_policies"` // managed policy ARNs
}

// iamPolicy mirrors the spinifex IAM Policy stored in NATS KV.
type iamPolicy struct {
	PolicyName     string `json:"policy_name"`
	PolicyDocument string `json:"policy_document"` // JSON string
}

// iamPolicyDocument is a parsed IAM policy JSON structure.
type iamPolicyDocument struct {
	Version   string         `json:"Version"`
	Statement []iamStatement `json:"Statement"`
}

// iamStatement is a single statement within a policy document.
type iamStatement struct {
	Sid      string         `json:"Sid,omitempty"`
	Effect   string         `json:"Effect"`
	Action   iamStringOrArr `json:"Action"`
	Resource iamStringOrArr `json:"Resource"`
}

// iamStringOrArr handles JSON fields that can be either a string or an array of strings.
type iamStringOrArr []string

func (s *iamStringOrArr) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*s = nil
		return nil
	}
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*s = []string{single}
		return nil
	}
	var arr []string
	if err := json.Unmarshal(data, &arr); err != nil {
		return err
	}
	*s = arr
	return nil
}

// --- ConfigProvider ---

// ConfigProvider looks up credentials from the static config Auth entries.
type ConfigProvider struct {
	entries []AuthEntry
}

var _ CredentialProvider = (*ConfigProvider)(nil)

// NewConfigProvider creates a provider backed by static config entries.
func NewConfigProvider(entries []AuthEntry) *ConfigProvider {
	return &ConfigProvider{entries: entries}
}

func (p *ConfigProvider) LookupCredentials(accessKeyID string) (*CredentialResult, error) {
	for _, auth := range p.entries {
		if auth.AccessKeyID == accessKeyID {
			return &CredentialResult{
				SecretAccessKey: auth.SecretAccessKey,
				AccountID:       auth.AccountID,
				SkipPolicyCheck: true,
			}, nil
		}
	}
	return nil, ErrKeyNotFound
}

func (p *ConfigProvider) Close() {}

// --- NATSIAMProvider ---

const (
	kvBucketUsers    = "spinifex-iam-users"
	kvBucketRoles    = "spinifex-iam-roles"
	kvBucketPolicies = "spinifex-iam-policies"

	// kvBucketSessionCredentials holds STS-minted ASIA session records. It is a
	// separate bucket from the AKIA access keys and is opened lazily on its own
	// readiness flag so a missing session bucket never disables AKIA auth.
	//nolint:gosec // G101: bucket name, not a credential value
	kvBucketSessionCredentials = "spinifex-iam-session-credentials"

	// sessionAccessKeyIDPrefix is the AWS prefix for STS temporary credentials.
	// Long-lived IAM keys use "AKIA"; the two namespaces live in disjoint
	// buckets so a prefix-first dispatch cannot be confused.
	sessionAccessKeyIDPrefix = "ASIA"

	// principalTypeUser marks a session minted by GetSessionToken for an IAM
	// user (SessionName == user name), as opposed to an assumed-role session.
	principalTypeUser = "user"

	// principalTypeAssumedRole marks a session minted by AssumeRole /
	// AssumeRoleForInstance; the caller's permissions come from the underlying
	// role's attached managed policies (resolved via underlying_role_arn). An
	// empty principal_type is treated as assumed-role for backward compatibility.
	principalTypeAssumedRole = "assumed-role"

	cacheTTL = 60 * time.Second
)

type cachedCredential struct {
	result    *CredentialResult
	expiresAt time.Time
}

// NATSIAMProvider looks up credentials from NATS KV and decrypts secrets.
// Buckets are lazily initialized to handle the bootstrap case where predastore
// starts before the spinifex daemon creates IAM KV buckets.
type NATSIAMProvider struct {
	conn       *nats.Conn
	js         nats.JetStreamContext
	gcm        cipher.AEAD
	bucketName string // access keys bucket name

	mu    sync.RWMutex
	cache map[string]*cachedCredential

	// Lazy-initialized KV buckets — nil until spinifex daemon creates them.
	accessKeysBucket nats.KeyValue
	usersBucket      nats.KeyValue
	rolesBucket      nats.KeyValue
	policiesBucket   nats.KeyValue
	bucketsReady     bool

	// Session-credentials bucket has its own readiness flag: it is opened
	// independently of the AKIA buckets so either path can come up alone.
	sessionsBucket nats.KeyValue
	sessionsReady  bool

	watcher   nats.KeyWatcher
	done      chan struct{}
	closeOnce sync.Once
}

var _ CredentialProvider = (*NATSIAMProvider)(nil)

// NewNATSIAMProvider creates a provider that looks up IAM credentials from NATS KV.
// The provider connects to NATS eagerly but opens KV buckets lazily — this allows
// predastore to start before the spinifex daemon creates the IAM buckets during bootstrap.
func NewNATSIAMProvider(cfg *IAMConfig) (*NATSIAMProvider, error) {
	if cfg.NATSUrl == "" {
		return nil, fmt.Errorf("iam.nats_url is required")
	}
	if cfg.MasterKeyPath == "" {
		return nil, fmt.Errorf("iam.master_key_path is required")
	}

	// Load and validate master key. The IAM master key is shared across
	// services on the host via group ownership (e.g. /etc/spinifex/master.key
	// at root:spinifex 0640), so use the shared loader rather than the strict
	// 0600 loader used for the per-node fragment encryption key.
	key, err := masterkey.LoadShared(cfg.MasterKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load master key: %w", err)
	}
	gcm := key.AEAD

	// Connect to NATS
	opts := []nats.Option{nats.Name("predastore-iam")}
	if cfg.NATSToken != "" {
		opts = append(opts, nats.Token(cfg.NATSToken))
	}

	conn, err := nats.Connect(cfg.NATSUrl, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("get JetStream context: %w", err)
	}

	bucketName := cfg.AccessKeysBucket
	if bucketName == "" {
		bucketName = "spinifex-iam-access-keys"
	}

	p := &NATSIAMProvider{
		conn:       conn,
		js:         js,
		gcm:        gcm,
		bucketName: bucketName,
		cache:      make(map[string]*cachedCredential),
		done:       make(chan struct{}),
	}

	// Try to open KV buckets now. If they don't exist yet (spinifex daemon hasn't
	// bootstrapped), we'll retry on each lookup until they appear.
	if err := p.ensureBuckets(); err != nil {
		slog.Warn("IAM KV buckets not available yet — IAM auth will activate once "+
			"spinifex daemon creates them (config-based auth works immediately)",
			"error", err)
	}

	slog.Info("NATS IAM provider initialized", "nats_url", cfg.NATSUrl, "bucket", bucketName,
		"bucketsReady", p.bucketsReady)
	return p, nil
}

// ensureBuckets attempts to open the four IAM KV buckets and start the watcher.
// Returns nil if all buckets are ready, or an error describing what's missing.
// Safe to call multiple times — no-ops once buckets are ready.
func (p *NATSIAMProvider) ensureBuckets() error {
	if p.bucketsReady {
		return nil
	}
	if p.js == nil {
		return fmt.Errorf("JetStream context not available")
	}

	akBucket, err := p.js.KeyValue(p.bucketName)
	if err != nil {
		return fmt.Errorf("open access keys bucket %q: %w", p.bucketName, err)
	}

	usersBucket, err := p.js.KeyValue(kvBucketUsers)
	if err != nil {
		return fmt.Errorf("open users bucket: %w", err)
	}

	rolesBucket, err := p.js.KeyValue(kvBucketRoles)
	if err != nil {
		return fmt.Errorf("open roles bucket: %w", err)
	}

	policiesBucket, err := p.js.KeyValue(kvBucketPolicies)
	if err != nil {
		return fmt.Errorf("open policies bucket: %w", err)
	}

	p.accessKeysBucket = akBucket
	p.usersBucket = usersBucket
	p.rolesBucket = rolesBucket
	p.policiesBucket = policiesBucket
	p.bucketsReady = true

	// Start KV watcher for cache invalidation
	watcher, err := akBucket.WatchAll()
	if err != nil {
		slog.Error("Failed to start NATS KV watcher — cache invalidation will not work, "+
			"credential changes will only take effect after cache TTL expiry",
			"error", err, "ttl", cacheTTL)
	} else {
		p.watcher = watcher
		go p.watchChanges()
	}

	slog.Info("IAM KV buckets now available — IAM authentication is active")
	return nil
}

// ensureSessionsBucket lazily opens the session-credentials KV bucket. It is
// deliberately decoupled from ensureBuckets: a missing session bucket must not
// disable AKIA auth, and an unbootstrapped AKIA path must not block sessions.
// The caller must hold p.mu.
func (p *NATSIAMProvider) ensureSessionsBucket() error {
	if p.sessionsReady {
		return nil
	}
	if p.js == nil {
		return fmt.Errorf("JetStream context not available")
	}

	bucket, err := p.js.KeyValue(kvBucketSessionCredentials)
	if err != nil {
		return fmt.Errorf("open session credentials bucket: %w", err)
	}

	p.sessionsBucket = bucket
	p.sessionsReady = true
	slog.Info("STS session-credentials bucket now available — ASIA session auth is active")
	return nil
}

// lookupSessionCredentials resolves an ASIA STS session credential: fetch the
// record, check expiry, decrypt the secret, and resolve the caller's policies.
// The request's SigV4 signature (over the decrypted secret) is what
// authenticates the caller — identical to the AKIA path, plus an expiry check.
// Session lookups are never cached so expiry is re-checked on every request.
func (p *NATSIAMProvider) lookupSessionCredentials(accessKeyID string) (*CredentialResult, error) {
	// Lazily open the session bucket. A not-yet-created bucket is the bootstrap
	// case — surface it as key-not-found (403) so AKIA auth is unaffected; any
	// other infra error propagates so the caller returns 500, not a misleading 403.
	p.mu.Lock()
	if !p.sessionsReady {
		if err := p.ensureSessionsBucket(); err != nil {
			p.mu.Unlock()
			if errors.Is(err, nats.ErrBucketNotFound) || errors.Is(err, nats.ErrStreamNotFound) {
				return nil, fmt.Errorf("%w: %s (session bucket not yet created)", ErrKeyNotFound, accessKeyID)
			}
			return nil, fmt.Errorf("session credential lookup unavailable: %w", err)
		}
	}
	bucket := p.sessionsBucket
	p.mu.Unlock()

	entry, err := bucket.Get(accessKeyID)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, accessKeyID)
		}
		return nil, fmt.Errorf("NATS KV lookup failed for session key %s: %w", accessKeyID, err)
	}

	var cred sessionCredential
	if err := json.Unmarshal(entry.Value(), &cred); err != nil {
		return nil, fmt.Errorf("unmarshal session credential: %w", err)
	}

	// Expired records are still readable until the STS janitor reaps them; reject
	// them here. Mapped to key-not-found (403 InvalidAccessKeyId) so the console
	// classifies it as a stale credential and logs out cleanly.
	if time.Now().UTC().After(cred.ExpiresAt) {
		slog.Warn("Authentication attempt with expired session credential",
			"accessKeyID", accessKeyID, "accountID", cred.AccountID, "expiresAt", cred.ExpiresAt)
		return nil, fmt.Errorf("%w: %s (session expired)", ErrKeyNotFound, accessKeyID)
	}

	// An empty AccountID would silently fail the bucket-ownership check for every
	// request; reject at the boundary with a clear diagnostic (parity with AKIA).
	if cred.AccountID == "" {
		slog.Error("Session credential has empty account_id — refusing to authenticate",
			"accessKeyID", accessKeyID, "sessionName", cred.SessionName)
		return nil, fmt.Errorf("session credential %s has empty account_id", accessKeyID)
	}

	secret, err := p.decrypt(cred.SecretEncrypted)
	if err != nil {
		return nil, fmt.Errorf("decrypt session secret: %w", err)
	}

	// User sessions (GetSessionToken) resolve straight back to the IAM user
	// (SessionName == user name); assumed-role sessions (AssumeRole /
	// AssumeRoleForInstance) resolve the underlying role's attached managed
	// policies. Both feed the same policy + ownership evaluation downstream. An
	// empty principal_type is treated as assumed-role for backward compat; any
	// other value fails closed (no policies → implicit deny → 403 AccessDenied).
	var policies []iamPolicyDocument
	switch cred.PrincipalType {
	case principalTypeUser:
		if err := p.ensureIAMBucketsForSession(accessKeyID); err != nil {
			return nil, err
		}
		policies, err = p.resolveUserPolicies(cred.AccountID, cred.SessionName)
		if err != nil {
			return nil, mapSessionPrincipalError(accessKeyID, err)
		}
	case principalTypeAssumedRole, "":
		arnAccount, roleName := parseRoleNameFromARN(cred.UnderlyingRoleARN)
		switch {
		case roleName == "":
			// A malformed or absent underlying_role_arn (e.g. a legacy record
			// predating the field) cannot identify a role — fail closed
			// (implicit deny), today's safe behaviour, never a server error.
			slog.Warn("Assumed-role session has no resolvable role ARN — failing closed (implicit deny)",
				"accessKeyID", accessKeyID, "accountID", cred.AccountID,
				"sessionName", cred.SessionName, "underlyingRoleARN", cred.UnderlyingRoleARN)
		case arnAccount != "" && arnAccount != cred.AccountID:
			// Spinifex rejects cross-account assume at mint, so a mismatch here
			// means a corrupt/misfiled record — fail closed (D2, defence-in-depth).
			slog.Error("Assumed-role session ARN account disagrees with session account — failing closed",
				"accessKeyID", accessKeyID, "sessionAccountID", cred.AccountID,
				"arnAccountID", arnAccount, "underlyingRoleARN", cred.UnderlyingRoleARN)
		default:
			if err := p.ensureIAMBucketsForSession(accessKeyID); err != nil {
				return nil, err
			}
			policies, err = p.resolveRolePolicies(cred.AccountID, roleName)
			if err != nil {
				return nil, mapSessionPrincipalError(accessKeyID, err)
			}
		}
	default:
		slog.Warn("Unrecognised session principal_type — failing closed (implicit deny)",
			"accessKeyID", accessKeyID, "principalType", cred.PrincipalType,
			"accountID", cred.AccountID, "sessionName", cred.SessionName)
	}

	return &CredentialResult{
		SecretAccessKey: secret,
		AccountID:       cred.AccountID,
		UserName:        cred.SessionName,
		SkipPolicyCheck: false,
		PolicyDocuments: policies,
	}, nil
}

func (p *NATSIAMProvider) watchChanges() {
	for {
		select {
		case entry, ok := <-p.watcher.Updates():
			if !ok {
				slog.Error("NATS KV watcher channel closed unexpectedly — " +
					"cache invalidation is disabled, cached credentials may become stale")
				p.mu.Lock()
				p.cache = make(map[string]*cachedCredential)
				p.mu.Unlock()
				return
			}
			if entry == nil {
				continue // initial nil sentinel
			}
			// Invalidate cache for this access key
			p.mu.Lock()
			delete(p.cache, entry.Key())
			p.mu.Unlock()
			slog.Debug("Cache invalidated for access key", "key", entry.Key())
		case <-p.done:
			return
		}
	}
}

func (p *NATSIAMProvider) LookupCredentials(accessKeyID string) (*CredentialResult, error) {
	// STS session credentials live in a separate bucket and follow a distinct
	// resolution path (expiry check, no caching). Dispatch on the AWS access-key
	// prefix before the AKIA cache check.
	if strings.HasPrefix(accessKeyID, sessionAccessKeyIDPrefix) {
		return p.lookupSessionCredentials(accessKeyID)
	}

	// Check cache
	p.mu.RLock()
	if cached, ok := p.cache[accessKeyID]; ok && time.Now().Before(cached.expiresAt) {
		p.mu.RUnlock()
		return cached.result, nil
	}
	p.mu.RUnlock()

	// Lazy bucket init: if buckets aren't ready yet, try to open them.
	// This handles the bootstrap case where predastore starts before the
	// spinifex daemon creates IAM KV buckets.
	p.mu.Lock()
	if !p.bucketsReady {
		if err := p.ensureBuckets(); err != nil {
			p.mu.Unlock()
			// Distinguish "buckets don't exist yet" (bootstrap) from NATS infra errors.
			// Bucket/stream-not-found means spinifex daemon hasn't created them yet — treat
			// as key-not-found so ChainProvider falls back to config.
			// Any other error (NATS down, auth failure) must propagate so callers
			// return 500 instead of a misleading 403.
			if errors.Is(err, nats.ErrBucketNotFound) || errors.Is(err, nats.ErrStreamNotFound) {
				return nil, fmt.Errorf("%w: %s (IAM buckets not yet created)", ErrKeyNotFound, accessKeyID)
			}
			return nil, fmt.Errorf("IAM lookup unavailable: %w", err)
		}
	}
	p.mu.Unlock()

	// Lookup access key in NATS KV
	entry, err := p.accessKeysBucket.Get(accessKeyID)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, accessKeyID)
		}
		return nil, fmt.Errorf("NATS KV lookup failed for access key %s: %w", accessKeyID, err)
	}

	var ak iamAccessKey
	if err := json.Unmarshal(entry.Value(), &ak); err != nil {
		return nil, fmt.Errorf("unmarshal access key: %w", err)
	}

	if ak.Status != "Active" {
		slog.Warn("Authentication attempt with inactive access key",
			"accessKeyID", accessKeyID,
			"accountID", ak.AccountID,
			"userName", ak.UserName,
			"status", ak.Status)
		return nil, fmt.Errorf("access key %s is inactive (status: %s)", accessKeyID, ak.Status)
	}

	// Reject malformed credentials at the boundary — an empty AccountID would
	// silently propagate into the bucket-ownership check and fail closed for
	// every authenticated request without any clear diagnostic.
	if ak.AccountID == "" {
		slog.Error("Access key has empty account_id — refusing to authenticate",
			"accessKeyID", accessKeyID, "userName", ak.UserName)
		return nil, fmt.Errorf("access key %s has empty account_id", accessKeyID)
	}

	// Decrypt the secret
	secret, err := p.decrypt(ak.SecretAccessKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt secret: %w", err)
	}

	// Resolve user policies
	policies, err := p.resolveUserPolicies(ak.AccountID, ak.UserName)
	if err != nil {
		return nil, fmt.Errorf("resolve policies: %w", err)
	}

	result := &CredentialResult{
		SecretAccessKey: secret,
		AccountID:       ak.AccountID,
		UserName:        ak.UserName,
		SkipPolicyCheck: false,
		PolicyDocuments: policies,
	}

	// Cache the result
	p.mu.Lock()
	p.cache[accessKeyID] = &cachedCredential{
		result:    result,
		expiresAt: time.Now().Add(cacheTTL),
	}
	p.mu.Unlock()

	return result, nil
}

// ensureIAMBucketsForSession opens the users/roles/policies buckets needed to
// resolve a session principal's permissions. Bootstrap-safe (mirroring the AKIA
// path): a not-yet-created bucket surfaces as ErrKeyNotFound (403) so a session
// arriving before any AKIA request never dereferences a nil bucket; any other
// infra error propagates so the caller returns 500, not a misleading 403.
func (p *NATSIAMProvider) ensureIAMBucketsForSession(accessKeyID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.ensureBuckets(); err != nil {
		if errors.Is(err, nats.ErrBucketNotFound) || errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("%w: %s (IAM buckets not yet created)", ErrKeyNotFound, accessKeyID)
		}
		return fmt.Errorf("session credential lookup unavailable: %w", err)
	}
	return nil
}

// mapSessionPrincipalError classifies a policy-resolution failure for a session
// principal. A deleted IAM user/role or detached policy surfaces as a KV
// not-found: a stale-principal authentication failure (403), not a server
// fault. Any other error is an infrastructure fault (500). Shared by the user
// and assumed-role arms so both report identical semantics.
func mapSessionPrincipalError(accessKeyID string, err error) error {
	if errors.Is(err, nats.ErrKeyNotFound) {
		return fmt.Errorf("%w: %s (session principal no longer exists)", ErrKeyNotFound, accessKeyID)
	}
	return fmt.Errorf("resolve session policies: %w", err)
}

func (p *NATSIAMProvider) resolveUserPolicies(accountID, userName string) ([]iamPolicyDocument, error) {
	kvKey := accountID + "." + userName
	entry, err := p.usersBucket.Get(kvKey)
	if err != nil {
		return nil, fmt.Errorf("lookup user %s: %w", kvKey, err)
	}

	var user iamUser
	if err := json.Unmarshal(entry.Value(), &user); err != nil {
		return nil, fmt.Errorf("unmarshal user: %w", err)
	}

	return p.resolveManagedPolicies(accountID, user.AttachedPolicies)
}

// resolveRolePolicies resolves an assumed-role session's permissions: load the
// role record from rolesBucket and resolve its attached managed policies. It
// mirrors resolveUserPolicies — spinifex roles only ever carry managed policies
// (no inline role policies exist), so the per-policy resolution is identical.
func (p *NATSIAMProvider) resolveRolePolicies(accountID, roleName string) ([]iamPolicyDocument, error) {
	kvKey := accountID + "." + roleName
	entry, err := p.rolesBucket.Get(kvKey)
	if err != nil {
		return nil, fmt.Errorf("lookup role %s: %w", kvKey, err)
	}

	var role iamRole
	if err := json.Unmarshal(entry.Value(), &role); err != nil {
		return nil, fmt.Errorf("unmarshal role: %w", err)
	}

	return p.resolveManagedPolicies(accountID, role.AttachedPolicies)
}

// resolveManagedPolicies resolves a list of managed-policy ARNs into parsed
// policy documents from policiesBucket. Shared by the user and role resolvers:
// both attach only managed policies, so the per-ARN resolution is identical.
func (p *NATSIAMProvider) resolveManagedPolicies(accountID string, arns []string) ([]iamPolicyDocument, error) {
	var docs []iamPolicyDocument
	for _, arn := range arns {
		policyName := extractPolicyName(arn)
		if policyName == "" {
			slog.Warn("Skipping unparseable policy ARN", "arn", arn, "accountID", accountID)
			continue
		}

		policyKey := accountID + "." + policyName
		pEntry, err := p.policiesBucket.Get(policyKey)
		if err != nil {
			return nil, fmt.Errorf("lookup policy %s: %w", policyKey, err)
		}

		var policy iamPolicy
		if err := json.Unmarshal(pEntry.Value(), &policy); err != nil {
			return nil, fmt.Errorf("unmarshal policy: %w", err)
		}

		var doc iamPolicyDocument
		if err := json.Unmarshal([]byte(policy.PolicyDocument), &doc); err != nil {
			return nil, fmt.Errorf("parse policy document %s: %w", policyName, err)
		}
		docs = append(docs, doc)
	}

	return docs, nil
}

// extractPolicyName extracts the policy name from an ARN like
// "arn:aws:iam::000000000001:policy/AdministratorAccess"
func extractPolicyName(arn string) string {
	parts := strings.SplitN(arn, ":policy", 2)
	if len(parts) != 2 || parts[1] == "" {
		return ""
	}
	segments := strings.Split(parts[1], "/")
	return segments[len(segments)-1]
}

// parseRoleNameFromARN extracts the account ID and role name from an IAM role
// ARN of the form arn:aws:iam::<accountID>:role/<path>/<name> (path optional),
// mirroring spinifex's parseRoleARN. The name is the segment after the final
// "/", so nested paths (role/some/path/Name → Name) are handled. Any malformed
// ARN returns empty strings so the caller fails closed (implicit deny).
func parseRoleNameFromARN(arn string) (accountID, name string) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" || parts[1] != "aws" || parts[2] != "iam" || parts[3] != "" {
		return "", ""
	}
	const prefix = "role/"
	resource := parts[5]
	if !strings.HasPrefix(resource, prefix) {
		return "", ""
	}
	pathAndName := resource[len(prefix):]
	if slash := strings.LastIndex(pathAndName, "/"); slash >= 0 {
		name = pathAndName[slash+1:]
	} else {
		name = pathAndName
	}
	if name == "" {
		return "", ""
	}
	return parts[4], name
}

func (p *NATSIAMProvider) decrypt(ciphertext string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("base64 decode: %w", err)
	}

	nonceSize := p.gcm.NonceSize()
	if len(data) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	nonce, sealed := data[:nonceSize], data[nonceSize:]
	plaintext, err := p.gcm.Open(nil, nonce, sealed, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt: %w", err)
	}
	return string(plaintext), nil
}

func (p *NATSIAMProvider) Close() {
	p.closeOnce.Do(func() {
		close(p.done)
		if p.watcher != nil {
			if err := p.watcher.Stop(); err != nil {
				slog.Warn("Failed to stop NATS KV watcher during cleanup", "error", err)
			}
		}
		if p.conn != nil {
			p.conn.Close()
		}
	})
}

// --- ChainProvider ---

// ChainProvider checks config first (service accounts with SkipPolicyCheck),
// then falls back to NATS IAM for user credentials. Config entries take priority
// because the system root key exists in both config and NATS KV but the NATS
// copy has no policies attached (implicit deny), while config entries get full access.
type ChainProvider struct {
	config CredentialProvider
	iam    CredentialProvider
}

var _ CredentialProvider = (*ChainProvider)(nil)

// NewChainProvider creates a provider that tries config first, then NATS IAM.
func NewChainProvider(iam, config CredentialProvider) *ChainProvider {
	return &ChainProvider{config: config, iam: iam}
}

func (p *ChainProvider) LookupCredentials(accessKeyID string) (*CredentialResult, error) {
	// Config entries are trusted service accounts — check first.
	result, err := p.config.LookupCredentials(accessKeyID)
	if err == nil {
		return result, nil
	}
	// Only fall through on "key not found" — propagate unexpected config errors.
	if !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}

	// Not in config — try NATS IAM for user / session credentials.
	result, err = p.iam.LookupCredentials(accessKeyID)
	if err == nil {
		return result, nil
	}

	// Distinguish "key not found anywhere" from infrastructure errors.
	if !errors.Is(err, ErrKeyNotFound) {
		slog.Warn("NATS IAM lookup failed",
			"accessKeyID", accessKeyID, "error", err)
		return nil, err
	}

	return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, accessKeyID)
}

func (p *ChainProvider) Close() {
	p.iam.Close()
	p.config.Close()
}
