// Package auth resolves S3 access keys to credentials and the IAM policies
// attached to them. It is the gate's identity layer: the config-defined
// service accounts, the NATS-backed IAM directory, and the chain that prefers
// one over the other.
package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	iamarn "github.com/mulgadc/bluebottle/pkg/auth"
	"github.com/mulgadc/bluebottle/pkg/iampolicy"
	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ErrKeyNotFound is returned when an access key does not exist in the provider.
var ErrKeyNotFound = errors.New("access key not found")

// ErrPrincipalConfig is returned when a principal's stored IAM records are
// unusable — a malformed or foreign-account attached policy ARN. It is a
// permanent config fault, so it denies (403) rather than inviting an SDK retry.
var ErrPrincipalConfig = errors.New("principal IAM configuration is invalid")

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
	PolicyDocuments []iampolicy.PolicyDocument // resolved policies (only for NATS-sourced credentials)
	// PrincipalType is "user" or "assumed-role", mirroring the session record.
	// Empty for config-based credentials, which skip policy evaluation entirely.
	PrincipalType string
}

// IsIAMUser reports whether the credential resolves to a real IAM user rather
// than a role session. aws:username is user-only: a session's UserName is the
// caller-chosen RoleSessionName and must never gate authorization.
func (c *CredentialResult) IsIAMUser() bool {
	return c.PrincipalType == principalTypeUser
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
	UserName         string            `json:"user_name"`
	AccountID        string            `json:"account_id"`
	AttachedPolicies []string          `json:"attached_policies"` // policy ARNs
	Groups           []string          `json:"groups"`            // group NAMES the user belongs to (≤10)
	InlinePolicies   map[string]string `json:"inline_policies"`   // policyName → document JSON
}

// iamRole mirrors the spinifex IAM Role stored in NATS KV (spinifex-iam-roles).
// Only the fields predastore needs to resolve an assumed-role session's
// permissions are replicated. The role's assume_role_policy_document is the
// trust policy (who may assume the role), not a permission policy, so it is
// deliberately omitted here.
type iamRole struct {
	RoleName         string            `json:"role_name"`
	AccountID        string            `json:"account_id"`
	AttachedPolicies []string          `json:"attached_policies"` // managed policy ARNs
	InlinePolicies   map[string]string `json:"inline_policies"`   // policyName → document JSON
}

// iamGroup mirrors the spinifex IAM Group stored in NATS KV (spinifex-iam-groups).
// Membership is canonical on iamUser.Groups, so the group's member list is not
// replicated here — only the two grant-source fields predastore needs to resolve a
// member's inherited permissions, exactly like iamRole.
type iamGroup struct {
	GroupName        string            `json:"group_name"`
	AccountID        string            `json:"account_id"`
	AttachedPolicies []string          `json:"attached_policies"` // managed policy ARNs
	InlinePolicies   map[string]string `json:"inline_policies"`   // policyName → document JSON
}

// iamPolicy mirrors the spinifex IAM Policy stored in NATS KV.
type iamPolicy struct {
	PolicyName     string `json:"policy_name"`
	PolicyDocument string `json:"policy_document"` // JSON string
}

// --- ConfigProvider ---

// ConfigProvider looks up credentials from the static config Auth entries.
type ConfigProvider struct {
	entries []Entry
}

var _ CredentialProvider = (*ConfigProvider)(nil)

// NewConfigProvider creates a provider backed by static config entries.
func NewConfigProvider(entries []Entry) *ConfigProvider {
	return &ConfigProvider{entries: entries}
}

func (p *ConfigProvider) LookupCredentials(accessKeyID string) (*CredentialResult, error) {
	for _, entry := range p.entries {
		if entry.AccessKeyID == accessKeyID {
			return &CredentialResult{
				SecretAccessKey: entry.SecretAccessKey,
				AccountID:       entry.AccountID,
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

	// kvBucketGroups holds IAM group records. It is opened lazily on its own
	// readiness flag (like the session bucket) so a missing groups bucket — a
	// predastore-ahead-of-spinifex rollout window — never disables the
	// direct-grant IAM path; group resolution is simply skipped until it appears.
	kvBucketGroups = "spinifex-iam-groups"

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
//
// The CredentialProvider contract carries no context: the S3 middleware calls
// LookupCredentials in-process off a SigV4 request and does not pass one through.
// LookupCredentials therefore binds context.Background() for its KV work, which
// leaves the jetstream package's own 5s API timeout in force — the same wait the
// legacy KV API applied. Every helper below it takes the context as its leading
// parameter, so the day the contract gains one only that binding line changes.
type NATSIAMProvider struct {
	conn       *nats.Conn
	js         jetstream.JetStream
	key        *masterkey.Key
	bucketName string // access keys bucket name

	mu    sync.RWMutex
	cache map[string]*cachedCredential

	// Lazy-initialized KV buckets — nil until spinifex daemon creates them.
	accessKeysBucket jetstream.KeyValue
	usersBucket      jetstream.KeyValue
	rolesBucket      jetstream.KeyValue
	policiesBucket   jetstream.KeyValue
	bucketsReady     bool

	// Session-credentials bucket has its own readiness flag: it is opened
	// independently of the AKIA buckets so either path can come up alone.
	sessionsBucket jetstream.KeyValue
	sessionsReady  bool

	// Groups bucket has its own readiness flag for the same reason: a missing
	// groups bucket must not disable direct-grant IAM auth for users who never
	// use groups. Group resolution is an additive layer on the user path.
	groupsBucket jetstream.KeyValue
	groupsReady  bool

	watcher   jetstream.KeyWatcher
	done      chan struct{}
	closeOnce sync.Once
}

var _ CredentialProvider = (*NATSIAMProvider)(nil)

// NewNATSIAMProvider creates a provider that looks up IAM credentials from NATS KV.
// natsIAMOptions builds the IAM connection's NATS options.
//
// MaxReconnects is unlimited on purpose. nats.go defaults to 60 attempts at 2s,
// so a NATS restart lasting over ~2 minutes closes this connection for good and
// every credential lookup fails until predastore itself is restarted.
func natsIAMOptions(cfg *IAMConfig) []nats.Option {
	opts := []nats.Option{
		nats.Name("predastore-iam"),
		nats.ReconnectWait(time.Second),
		nats.MaxReconnects(-1),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			slog.Warn("IAM NATS disconnected; credential lookups fail until it returns", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			slog.Info("IAM NATS reconnected", "url", nc.ConnectedUrl())
		}),
	}
	if cfg.NATSToken != "" {
		opts = append(opts, nats.Token(cfg.NATSToken))
	}
	return opts
}

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

	conn, err := nats.Connect(cfg.NATSUrl, natsIAMOptions(cfg)...)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}

	js, err := jetstream.New(conn)
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
		key:        key,
		bucketName: bucketName,
		cache:      make(map[string]*cachedCredential),
		done:       make(chan struct{}),
	}

	// Try to open KV buckets now. If they don't exist yet (spinifex daemon hasn't
	// bootstrapped), we'll retry on each lookup until they appear. This runs at
	// startup with no request in flight, so it opens buckets against Background.
	if err := p.ensureBuckets(context.Background()); err != nil {
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
func (p *NATSIAMProvider) ensureBuckets(ctx context.Context) error {
	if p.bucketsReady {
		return nil
	}
	if p.js == nil {
		return fmt.Errorf("JetStream context not available")
	}

	akBucket, err := p.js.KeyValue(ctx, p.bucketName)
	if err != nil {
		return fmt.Errorf("open access keys bucket %q: %w", p.bucketName, err)
	}

	usersBucket, err := p.js.KeyValue(ctx, kvBucketUsers)
	if err != nil {
		return fmt.Errorf("open users bucket: %w", err)
	}

	rolesBucket, err := p.js.KeyValue(ctx, kvBucketRoles)
	if err != nil {
		return fmt.Errorf("open roles bucket: %w", err)
	}

	policiesBucket, err := p.js.KeyValue(ctx, kvBucketPolicies)
	if err != nil {
		return fmt.Errorf("open policies bucket: %w", err)
	}

	p.accessKeysBucket = akBucket
	p.usersBucket = usersBucket
	p.rolesBucket = rolesBucket
	p.policiesBucket = policiesBucket
	p.bucketsReady = true

	// Start KV watcher for cache invalidation. ensureBuckets is only ever called
	// with Background, so the watcher lives for the process, not a single request.
	watcher, err := akBucket.WatchAll(ctx)
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
func (p *NATSIAMProvider) ensureSessionsBucket(ctx context.Context) error {
	if p.sessionsReady {
		return nil
	}
	if p.js == nil {
		return fmt.Errorf("JetStream context not available")
	}

	bucket, err := p.js.KeyValue(ctx, kvBucketSessionCredentials)
	if err != nil {
		return fmt.Errorf("open session credentials bucket: %w", err)
	}

	p.sessionsBucket = bucket
	p.sessionsReady = true
	slog.Info("STS session-credentials bucket now available — ASIA session auth is active")
	return nil
}

// ensureGroupsBucket lazily opens the IAM groups KV bucket. Like the session
// bucket it is decoupled from ensureBuckets: a missing groups bucket must not
// disable direct-grant IAM auth. The caller must hold p.mu.
func (p *NATSIAMProvider) ensureGroupsBucket(ctx context.Context) error {
	if p.groupsReady {
		return nil
	}
	if p.js == nil {
		return fmt.Errorf("JetStream context not available")
	}

	bucket, err := p.js.KeyValue(ctx, kvBucketGroups)
	if err != nil {
		return fmt.Errorf("open groups bucket: %w", err)
	}

	p.groupsBucket = bucket
	p.groupsReady = true
	slog.Info("IAM groups bucket now available — group-inherited S3 permissions are active")
	return nil
}

// lookupSessionCredentials resolves an ASIA STS session credential: fetch the
// record, check expiry, decrypt the secret, and resolve the caller's policies.
// The request's SigV4 signature (over the decrypted secret) is what
// authenticates the caller — identical to the AKIA path, plus an expiry check.
// Session lookups are never cached so expiry is re-checked on every request.
func (p *NATSIAMProvider) lookupSessionCredentials(ctx context.Context, accessKeyID string) (*CredentialResult, error) {
	// Lazily open the session bucket. A not-yet-created bucket is the bootstrap
	// case — surface it as key-not-found (403) so AKIA auth is unaffected; any
	// other infra error propagates so the caller returns 500, not a misleading 403.
	p.mu.Lock()
	if !p.sessionsReady {
		if err := p.ensureSessionsBucket(ctx); err != nil {
			p.mu.Unlock()
			if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
				return nil, fmt.Errorf("%w: %s (session bucket not yet created)", ErrKeyNotFound, accessKeyID)
			}
			return nil, fmt.Errorf("session credential lookup unavailable: %w", err)
		}
	}
	bucket := p.sessionsBucket
	p.mu.Unlock()

	entry, err := bucket.Get(ctx, accessKeyID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
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

	secret, err := p.key.DecryptBase64(cred.SecretEncrypted)
	if err != nil {
		return nil, fmt.Errorf("decrypt session secret: %w", err)
	}

	// User sessions (GetSessionToken) resolve straight back to the IAM user
	// (SessionName == user name); assumed-role sessions (AssumeRole /
	// AssumeRoleForInstance) resolve the underlying role's attached managed
	// policies. Both feed the same policy + ownership evaluation downstream. An
	// empty principal_type is treated as assumed-role for backward compat; any
	// other value fails closed (no policies → implicit deny → 403 AccessDenied).
	var policies []iampolicy.PolicyDocument
	switch cred.PrincipalType {
	case principalTypeUser:
		if err := p.ensureIAMBucketsForSession(ctx, accessKeyID); err != nil {
			return nil, err
		}
		policies, err = p.resolveUserPolicies(ctx, cred.AccountID, cred.SessionName)
		if err != nil {
			return nil, mapSessionPrincipalError(accessKeyID, err)
		}
	case principalTypeAssumedRole, "":
		arnAccount, roleName, arnErr := iamarn.ParseRoleARN(cred.UnderlyingRoleARN)
		switch {
		case arnErr != nil:
			// A malformed or absent underlying_role_arn (e.g. a legacy record
			// predating the field) cannot identify a role — fail closed
			// (implicit deny), today's safe behaviour, never a server error.
			slog.Warn("Assumed-role session has no resolvable role ARN — failing closed (implicit deny)",
				"accessKeyID", accessKeyID, "accountID", cred.AccountID,
				"sessionName", cred.SessionName, "underlyingRoleARN", cred.UnderlyingRoleARN, "err", arnErr)
		case arnAccount != cred.AccountID:
			// Spinifex rejects cross-account assume at mint, so a mismatch here
			// means a corrupt/misfiled record — fail closed (defence-in-depth
			// against resolving a same-named role in the session's own account).
			slog.Error("Assumed-role session ARN account disagrees with session account — failing closed",
				"accessKeyID", accessKeyID, "sessionAccountID", cred.AccountID,
				"arnAccountID", arnAccount, "underlyingRoleARN", cred.UnderlyingRoleARN)
		default:
			if err := p.ensureIAMBucketsForSession(ctx, accessKeyID); err != nil {
				return nil, err
			}
			policies, err = p.resolveRolePolicies(ctx, cred.AccountID, roleName)
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
		PrincipalType:   cred.PrincipalType,
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
	// The CredentialProvider contract passes no context, so bind Background here
	// and thread it into every KV op below (see the type doc for the rationale).
	ctx := context.Background()

	// STS session credentials live in a separate bucket and follow a distinct
	// resolution path (expiry check, no caching). Dispatch on the AWS access-key
	// prefix before the AKIA cache check.
	if strings.HasPrefix(accessKeyID, sessionAccessKeyIDPrefix) {
		return p.lookupSessionCredentials(ctx, accessKeyID)
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
		if err := p.ensureBuckets(ctx); err != nil {
			p.mu.Unlock()
			// Distinguish "buckets don't exist yet" (bootstrap) from NATS infra errors.
			// Bucket/stream-not-found means spinifex daemon hasn't created them yet — treat
			// as key-not-found so ChainProvider falls back to config.
			// Any other error (NATS down, auth failure) must propagate so callers
			// return 500 instead of a misleading 403.
			if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
				return nil, fmt.Errorf("%w: %s (IAM buckets not yet created)", ErrKeyNotFound, accessKeyID)
			}
			return nil, fmt.Errorf("IAM lookup unavailable: %w", err)
		}
	}
	p.mu.Unlock()

	// Lookup access key in NATS KV
	entry, err := p.accessKeysBucket.Get(ctx, accessKeyID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
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
	secret, err := p.key.DecryptBase64(ak.SecretAccessKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt secret: %w", err)
	}

	// Resolve user policies
	policies, err := p.resolveUserPolicies(ctx, ak.AccountID, ak.UserName)
	if err != nil {
		return nil, fmt.Errorf("resolve policies: %w", err)
	}

	result := &CredentialResult{
		SecretAccessKey: secret,
		AccountID:       ak.AccountID,
		UserName:        ak.UserName,
		SkipPolicyCheck: false,
		PolicyDocuments: policies,
		PrincipalType:   principalTypeUser,
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
func (p *NATSIAMProvider) ensureIAMBucketsForSession(ctx context.Context, accessKeyID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.ensureBuckets(ctx); err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
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
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("%w: %s (session principal no longer exists)", ErrKeyNotFound, accessKeyID)
	}
	return fmt.Errorf("resolve session policies: %w", err)
}

func (p *NATSIAMProvider) resolveUserPolicies(ctx context.Context, accountID, userName string) ([]iampolicy.PolicyDocument, error) {
	kvKey := accountID + "." + userName
	entry, err := p.usersBucket.Get(ctx, kvKey)
	if err != nil {
		return nil, fmt.Errorf("lookup user %s: %w", kvKey, err)
	}

	var user iamUser
	if err := json.Unmarshal(entry.Value(), &user); err != nil {
		return nil, fmt.Errorf("unmarshal user: %w", err)
	}

	docs, err := p.resolveManagedPolicies(ctx, accountID, "user "+userName, user.AttachedPolicies)
	if err != nil {
		return nil, err
	}

	// User-own inline policies. Shares the group/role parse helper so a malformed
	// document fails closed identically; keeps the S3 decision in lockstep with
	// spinifex's GetUserPolicies user-inline loop.
	inline, err := resolveInlinePolicies(user.InlinePolicies, "user "+userName)
	if err != nil {
		return nil, err
	}
	docs = append(docs, inline...)

	// Group-inherited policies (managed + inline). Appended to the same slice so
	// iampolicy.Evaluate combines direct, group-managed, and group-inline grants
	// under deny-wins. The common no-group user pays no extra lock or KV round-trip.
	if len(user.Groups) > 0 {
		groupDocs, err := p.resolveGroupPolicies(ctx, accountID, userName, user.Groups)
		if err != nil {
			return nil, err
		}
		docs = append(docs, groupDocs...)
	}

	return docs, nil
}

// resolveGroupPolicies resolves the managed and inline policies inherited from a
// user's groups. It mirrors spinifex's GetUserPolicies group loop: a missing
// group is skipped (a deleted group is inert), an unresolvable/malformed group
// policy fails closed, an absent groups bucket skips all group resolution, and a
// groups-bucket infra fault fails closed.
func (p *NATSIAMProvider) resolveGroupPolicies(ctx context.Context, accountID, userName string, groups []string) ([]iampolicy.PolicyDocument, error) {
	// Lazily open the groups bucket. A not-yet-created bucket means groups-v1 is
	// not deployed on the spinifex side, so no group records exist anywhere and
	// there is nothing (no Allow and no Deny) to resolve — skip safely. Any other
	// open error is an infra fault: fail closed rather than risk dropping a Deny.
	p.mu.Lock()
	if !p.groupsReady {
		if err := p.ensureGroupsBucket(ctx); err != nil {
			p.mu.Unlock()
			if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
				slog.Warn("Groups bucket not available — skipping group-inherited policies "+
					"(group grants will not apply until spinifex creates the bucket)",
					"accountID", accountID, "user", userName)
				return nil, nil
			}
			return nil, fmt.Errorf("open groups bucket: %w", err)
		}
	}
	bucket := p.groupsBucket
	p.mu.Unlock()

	var docs []iampolicy.PolicyDocument
	for _, groupName := range groups {
		gEntry, err := bucket.Get(ctx, accountID+"."+groupName)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				// Membership to a deleted group is inert (spinifex refuses to
				// delete a non-empty group), so this is a benign racing-delete
				// remnant. Skip it; a deleted group carries no grant to drop.
				slog.Warn("resolveGroupPolicies: member references missing group; skipping",
					"accountID", accountID, "user", userName, "group", groupName)
				continue
			}
			return nil, fmt.Errorf("lookup group %s.%s: %w", accountID, groupName, err)
		}

		var group iamGroup
		if err := json.Unmarshal(gEntry.Value(), &group); err != nil {
			return nil, fmt.Errorf("unmarshal group %s: %w", groupName, err)
		}

		managed, err := p.resolveManagedPolicies(ctx, accountID, "group "+groupName, group.AttachedPolicies)
		if err != nil {
			return nil, err // fail closed (mirrors direct-policy handling)
		}
		docs = append(docs, managed...)

		inline, err := resolveInlinePolicies(group.InlinePolicies, "group "+groupName)
		if err != nil {
			return nil, err
		}
		docs = append(docs, inline...)
	}
	return docs, nil
}

// resolveRolePolicies resolves an assumed-role session's permissions: load the
// role record from rolesBucket and resolve its attached managed policies plus
// any embedded inline policies.
func (p *NATSIAMProvider) resolveRolePolicies(ctx context.Context, accountID, roleName string) ([]iampolicy.PolicyDocument, error) {
	kvKey := accountID + "." + roleName
	entry, err := p.rolesBucket.Get(ctx, kvKey)
	if err != nil {
		return nil, fmt.Errorf("lookup role %s: %w", kvKey, err)
	}

	var role iamRole
	if err := json.Unmarshal(entry.Value(), &role); err != nil {
		return nil, fmt.Errorf("unmarshal role: %w", err)
	}

	docs, err := p.resolveManagedPolicies(ctx, accountID, "role "+roleName, role.AttachedPolicies)
	if err != nil {
		return nil, err
	}
	inline, err := resolveInlinePolicies(role.InlinePolicies, "role "+roleName)
	if err != nil {
		return nil, err
	}
	return append(docs, inline...), nil
}

// resolveInlinePolicies parses a principal's inline-policy map (policyName →
// document JSON) into policy documents. A malformed document fails closed so
// role- and group-inherited parsing cannot diverge; label identifies the owning
// principal in error messages (e.g. "group Admins", "role InstanceRole").
func resolveInlinePolicies(inline map[string]string, label string) ([]iampolicy.PolicyDocument, error) {
	var docs []iampolicy.PolicyDocument
	for name, raw := range inline {
		var doc iampolicy.PolicyDocument
		if err := json.Unmarshal([]byte(raw), &doc); err != nil {
			return nil, fmt.Errorf("parse %s inline policy %s: %w", label, name, err)
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

// resolveManagedPolicies resolves a list of managed-policy ARNs into parsed
// policy documents from policiesBucket. label names the principal holding the
// attachment ("group Admins") so a fault points at the record to fix.
func (p *NATSIAMProvider) resolveManagedPolicies(ctx context.Context, accountID, label string, arns []string) ([]iampolicy.PolicyDocument, error) {
	var docs []iampolicy.PolicyDocument
	for _, arn := range arns {
		// AWS-managed policies have no document in this stack: resolve them to no
		// grant, matching spinifex's deny for an unmodeled managed policy.
		if iamarn.IsAWSManagedPolicyARN(arn) {
			slog.Debug("Skipping AWS-managed policy ARN", "arn", arn, "accountID", accountID, "principal", label)
			continue
		}

		arnAccount, policyName, err := iamarn.ParsePolicyARN(arn)
		if err != nil {
			slog.Error("Attached policy ARN is unparseable — failing closed",
				"accountID", accountID, "principal", label, "arn", arn, "err", err)
			return nil, fmt.Errorf("%w: %s attached policy ARN %q: %w", ErrPrincipalConfig, label, arn, err)
		}
		// Scoping a foreign ARN's name to this account would load an unrelated
		// same-named policy; there is no correct grant to return for it.
		if arnAccount != accountID {
			slog.Error("Attached policy ARN names a foreign account — failing closed",
				"accountID", accountID, "principal", label, "arn", arn, "arnAccountID", arnAccount)
			return nil, fmt.Errorf("%w: %s attached policy ARN %q is not in account %s", ErrPrincipalConfig, label, arn, accountID)
		}

		policyKey := accountID + "." + policyName
		pEntry, err := p.policiesBucket.Get(ctx, policyKey)
		if err != nil {
			return nil, fmt.Errorf("lookup policy %s: %w", policyKey, err)
		}

		var policy iamPolicy
		if err := json.Unmarshal(pEntry.Value(), &policy); err != nil {
			return nil, fmt.Errorf("unmarshal policy: %w", err)
		}

		var doc iampolicy.PolicyDocument
		if err := json.Unmarshal([]byte(policy.PolicyDocument), &doc); err != nil {
			return nil, fmt.Errorf("parse policy document %s: %w", policyName, err)
		}
		docs = append(docs, doc)
	}

	return docs, nil
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
