package s3

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// ErrKeyNotFound is returned when an access key does not exist in the provider.
var ErrKeyNotFound = errors.New("access key not found")

// CredentialResult is the result of a credential lookup.
type CredentialResult struct {
	SecretAccessKey string
	AccountID       string
	UserName        string
	SkipPolicyCheck bool                // true for config-based entries (implicit full access)
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

// iamUser mirrors the spinifex IAM User stored in NATS KV.
type iamUser struct {
	UserName         string   `json:"user_name"`
	AccountID        string   `json:"account_id"`
	AttachedPolicies []string `json:"attached_policies"` // policy ARNs
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
	kvBucketPolicies = "spinifex-iam-policies"

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
	policiesBucket   nats.KeyValue
	bucketsReady     bool

	watcher   nats.KeyWatcher
	done      chan struct{}
	closeOnce sync.Once
}

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

	// Load and validate master key
	masterKey, err := loadMasterKey(cfg.MasterKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load master key: %w", err)
	}

	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

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

// ensureBuckets attempts to open the three IAM KV buckets and start the watcher.
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

	policiesBucket, err := p.js.KeyValue(kvBucketPolicies)
	if err != nil {
		return fmt.Errorf("open policies bucket: %w", err)
	}

	p.accessKeysBucket = akBucket
	p.usersBucket = usersBucket
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

func (p *NATSIAMProvider) resolveUserPolicies(accountID, userName string) ([]iamPolicyDocument, error) {
	// Look up user
	kvKey := accountID + "." + userName
	entry, err := p.usersBucket.Get(kvKey)
	if err != nil {
		return nil, fmt.Errorf("lookup user %s: %w", kvKey, err)
	}

	var user iamUser
	if err := json.Unmarshal(entry.Value(), &user); err != nil {
		return nil, fmt.Errorf("unmarshal user: %w", err)
	}

	// Resolve each attached policy
	var docs []iamPolicyDocument
	for _, arn := range user.AttachedPolicies {
		policyName := extractPolicyName(arn)
		if policyName == "" {
			slog.Warn("Skipping unparseable policy ARN",
				"arn", arn,
				"accountID", accountID,
				"userName", userName)
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

	// Not in config — try NATS IAM for user credentials.
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

// --- Helpers ---

func loadMasterKey(path string) ([]byte, error) {
	key, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read master key: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("master key must be 32 bytes, got %d", len(key))
	}
	return key, nil
}
