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

// --- IAM types (subset replicated from hive/handlers/iam for NATS KV reads) ---

// iamAccessKey mirrors the hive IAM AccessKey stored in NATS KV.
type iamAccessKey struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"` // AES-256-GCM encrypted, base64-encoded
	UserName        string `json:"user_name"`
	AccountID       string `json:"account_id"`
	Status          string `json:"status"`
	CreatedAt       string `json:"created_at"`
}

// iamUser mirrors the hive IAM User stored in NATS KV.
type iamUser struct {
	UserName         string   `json:"user_name"`
	AccountID        string   `json:"account_id"`
	AttachedPolicies []string `json:"attached_policies"` // policy ARNs
}

// iamPolicy mirrors the hive IAM Policy stored in NATS KV.
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
	return nil, errors.New("access key not found")
}

func (p *ConfigProvider) Close() {}

// --- NATSIAMProvider ---

const (
	kvBucketUsers    = "hive-iam-users"
	kvBucketPolicies = "hive-iam-policies"

	cacheTTL = 60 * time.Second
)

type cachedCredential struct {
	result    *CredentialResult
	expiresAt time.Time
}

// NATSIAMProvider looks up credentials from NATS KV and decrypts secrets.
type NATSIAMProvider struct {
	conn             *nats.Conn
	accessKeysBucket nats.KeyValue
	usersBucket      nats.KeyValue
	policiesBucket   nats.KeyValue
	gcm              cipher.AEAD

	mu    sync.RWMutex
	cache map[string]*cachedCredential

	watcher nats.KeyWatcher
	done    chan struct{}
}

// NewNATSIAMProvider creates a provider that looks up IAM credentials from NATS KV.
func NewNATSIAMProvider(cfg *IAMConfig) (*NATSIAMProvider, error) {
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
		bucketName = "hive-iam-access-keys"
	}

	akBucket, err := js.KeyValue(bucketName)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open access keys bucket %q: %w", bucketName, err)
	}

	usersBucket, err := js.KeyValue(kvBucketUsers)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open users bucket: %w", err)
	}

	policiesBucket, err := js.KeyValue(kvBucketPolicies)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("open policies bucket: %w", err)
	}

	p := &NATSIAMProvider{
		conn:             conn,
		accessKeysBucket: akBucket,
		usersBucket:      usersBucket,
		policiesBucket:   policiesBucket,
		gcm:              gcm,
		cache:            make(map[string]*cachedCredential),
		done:             make(chan struct{}),
	}

	// Start KV watcher for cache invalidation
	watcher, err := akBucket.WatchAll()
	if err != nil {
		slog.Warn("Failed to start NATS KV watcher for cache invalidation", "error", err)
	} else {
		p.watcher = watcher
		go p.watchChanges()
	}

	slog.Info("NATS IAM provider initialized", "nats_url", cfg.NATSUrl, "bucket", bucketName)
	return p, nil
}

func (p *NATSIAMProvider) watchChanges() {
	for {
		select {
		case entry, ok := <-p.watcher.Updates():
			if !ok {
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

	// Lookup access key in NATS KV
	entry, err := p.accessKeysBucket.Get(accessKeyID)
	if err != nil {
		return nil, fmt.Errorf("access key not found: %w", err)
	}

	var ak iamAccessKey
	if err := json.Unmarshal(entry.Value(), &ak); err != nil {
		return nil, fmt.Errorf("unmarshal access key: %w", err)
	}

	if ak.Status != "Active" {
		return nil, errors.New("access key is inactive")
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
	close(p.done)
	if p.watcher != nil {
		if err := p.watcher.Stop(); err != nil {
			slog.Debug("Failed to stop NATS KV watcher", "error", err)
		}
	}
	p.conn.Close()
}

// --- ChainProvider ---

// ChainProvider tries NATS first, then falls back to config.
type ChainProvider struct {
	primary  CredentialProvider
	fallback CredentialProvider
}

// NewChainProvider creates a provider that tries primary first, then fallback.
func NewChainProvider(primary, fallback CredentialProvider) *ChainProvider {
	return &ChainProvider{primary: primary, fallback: fallback}
}

func (p *ChainProvider) LookupCredentials(accessKeyID string) (*CredentialResult, error) {
	result, err := p.primary.LookupCredentials(accessKeyID)
	if err == nil {
		return result, nil
	}
	return p.fallback.LookupCredentials(accessKeyID)
}

func (p *ChainProvider) Close() {
	p.primary.Close()
	p.fallback.Close()
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
