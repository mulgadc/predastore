package s3

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- ConfigProvider tests ---

func TestConfigProvider_Found(t *testing.T) {
	p := NewConfigProvider([]AuthEntry{
		{AccessKeyID: "AK1", SecretAccessKey: "SK1", AccountID: "acct-1"},
		{AccessKeyID: "AK2", SecretAccessKey: "SK2", AccountID: "acct-2"},
	})

	result, err := p.LookupCredentials("AK2")
	require.NoError(t, err)
	assert.Equal(t, "SK2", result.SecretAccessKey)
	assert.Equal(t, "acct-2", result.AccountID)
	assert.True(t, result.SkipPolicyCheck, "config entries should skip policy check")
}

func TestConfigProvider_NotFound(t *testing.T) {
	p := NewConfigProvider([]AuthEntry{
		{AccessKeyID: "AK1", SecretAccessKey: "SK1"},
	})

	_, err := p.LookupCredentials("AK_MISSING")
	assert.Error(t, err)
}

func TestConfigProvider_Empty(t *testing.T) {
	p := NewConfigProvider(nil)
	_, err := p.LookupCredentials("AK1")
	assert.Error(t, err)
}

// --- ChainProvider tests ---

type mockProvider struct {
	result *CredentialResult
	err    error
}

func (m *mockProvider) LookupCredentials(_ string) (*CredentialResult, error) {
	return m.result, m.err
}

func (m *mockProvider) Close() {}

func TestChainProvider_PrimarySuccess(t *testing.T) {
	primary := &mockProvider{result: &CredentialResult{SecretAccessKey: "from-primary", AccountID: "acct-1"}}
	fallback := &mockProvider{result: &CredentialResult{SecretAccessKey: "from-fallback", AccountID: "acct-2"}}

	chain := NewChainProvider(primary, fallback)
	result, err := chain.LookupCredentials("AK1")
	require.NoError(t, err)
	assert.Equal(t, "from-primary", result.SecretAccessKey)
}

func TestChainProvider_FallbackOnNotFound(t *testing.T) {
	// ErrKeyNotFound from primary should fall back to config
	primary := &mockProvider{err: fmt.Errorf("%w: AK1", ErrKeyNotFound)}
	fallback := &mockProvider{result: &CredentialResult{SecretAccessKey: "from-fallback", AccountID: "acct-2", SkipPolicyCheck: true}}

	chain := NewChainProvider(primary, fallback)
	result, err := chain.LookupCredentials("AK1")
	require.NoError(t, err)
	assert.Equal(t, "from-fallback", result.SecretAccessKey)
	assert.True(t, result.SkipPolicyCheck)
}

func TestChainProvider_NoFallbackOnInfraError(t *testing.T) {
	// Non-ErrKeyNotFound errors (infra failures, inactive keys, etc.) must NOT fall back
	primary := &mockProvider{err: errors.New("NATS connection timeout")}
	fallback := &mockProvider{result: &CredentialResult{SecretAccessKey: "from-fallback", AccountID: "acct-2"}}

	chain := NewChainProvider(primary, fallback)
	_, err := chain.LookupCredentials("AK1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NATS connection timeout")
}

func TestChainProvider_NoFallbackOnInactiveKey(t *testing.T) {
	// Inactive key errors must NOT fall back to config
	primary := &mockProvider{err: fmt.Errorf("access key AK1 is inactive (status: Inactive)")}
	fallback := &mockProvider{result: &CredentialResult{SecretAccessKey: "from-fallback", AccountID: "acct-2", SkipPolicyCheck: true}}

	chain := NewChainProvider(primary, fallback)
	_, err := chain.LookupCredentials("AK1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "inactive")
}

func TestChainProvider_BothNotFound(t *testing.T) {
	primary := &mockProvider{err: ErrKeyNotFound}
	fallback := &mockProvider{err: ErrKeyNotFound}

	chain := NewChainProvider(primary, fallback)
	_, err := chain.LookupCredentials("AK1")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// --- extractPolicyName tests ---

func TestExtractPolicyName(t *testing.T) {
	tests := []struct {
		arn  string
		want string
	}{
		{"arn:aws:iam::000000000001:policy/AdministratorAccess", "AdministratorAccess"},
		{"arn:aws:iam::000000000001:policy/path/to/MyPolicy", "MyPolicy"},
		{"arn:aws:iam::000000000001:policy/", ""},
		{"invalid-arn", ""},
		{"", ""},
	}

	for _, tt := range tests {
		got := extractPolicyName(tt.arn)
		assert.Equal(t, tt.want, got, "extractPolicyName(%q)", tt.arn)
	}
}

// --- loadMasterKey tests ---

func TestLoadMasterKey_Valid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "master.key")

	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)

	err = os.WriteFile(path, key, 0600)
	require.NoError(t, err)

	loaded, err := loadMasterKey(path)
	require.NoError(t, err)
	assert.Equal(t, key, loaded)
}

func TestLoadMasterKey_WrongSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "master.key")

	err := os.WriteFile(path, []byte("too-short"), 0600)
	require.NoError(t, err)

	_, err = loadMasterKey(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "32 bytes")
}

func TestLoadMasterKey_Missing(t *testing.T) {
	_, err := loadMasterKey("/nonexistent/master.key")
	assert.Error(t, err)
}

// --- decrypt tests ---

func TestDecrypt_Roundtrip(t *testing.T) {
	// Generate a key and encrypt a secret
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)

	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	gcm, err := cipher.NewGCM(block)
	require.NoError(t, err)

	plaintext := "my-super-secret-key-12345"
	nonce := make([]byte, gcm.NonceSize())
	_, err = rand.Read(nonce)
	require.NoError(t, err)

	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	encoded := base64.StdEncoding.EncodeToString(ciphertext)

	// Create provider with the same key
	p := &NATSIAMProvider{gcm: gcm}
	decrypted, err := p.decrypt(encoded)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)
}

func TestDecrypt_InvalidBase64(t *testing.T) {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)

	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	gcm, err := cipher.NewGCM(block)
	require.NoError(t, err)

	p := &NATSIAMProvider{gcm: gcm}
	_, err = p.decrypt("not-valid-base64!!!")
	assert.Error(t, err)
}

func TestDecrypt_TooShort(t *testing.T) {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)

	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	gcm, err := cipher.NewGCM(block)
	require.NoError(t, err)

	p := &NATSIAMProvider{gcm: gcm}
	// Encode just 2 bytes — too short for nonce
	_, err = p.decrypt(base64.StdEncoding.EncodeToString([]byte{0x01, 0x02}))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}

// --- iamStringOrArr JSON tests ---

func TestIamStringOrArr_UnmarshalString(t *testing.T) {
	var s iamStringOrArr
	err := json.Unmarshal([]byte(`"s3:GetObject"`), &s)
	require.NoError(t, err)
	assert.Equal(t, iamStringOrArr{"s3:GetObject"}, s)
}

func TestIamStringOrArr_UnmarshalArray(t *testing.T) {
	var s iamStringOrArr
	err := json.Unmarshal([]byte(`["s3:GetObject", "s3:PutObject"]`), &s)
	require.NoError(t, err)
	assert.Equal(t, iamStringOrArr{"s3:GetObject", "s3:PutObject"}, s)
}

func TestIamStringOrArr_UnmarshalNull(t *testing.T) {
	var s iamStringOrArr
	err := json.Unmarshal([]byte(`null`), &s)
	require.NoError(t, err)
	assert.Nil(t, s)
}

// --- ErrKeyNotFound sentinel tests ---

func TestErrKeyNotFound_IsDetectable(t *testing.T) {
	wrapped := fmt.Errorf("%w: AK123", ErrKeyNotFound)
	assert.True(t, errors.Is(wrapped, ErrKeyNotFound))

	unrelated := errors.New("NATS connection timeout")
	assert.False(t, errors.Is(unrelated, ErrKeyNotFound))
}

// --- IAMConfig validation tests ---

// --- Lazy bucket init tests ---

func TestNATSIAMProvider_LazyBucketsNotReady(t *testing.T) {
	// When buckets aren't ready, LookupCredentials should return ErrKeyNotFound
	// so ChainProvider falls back to config.
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)

	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	gcm, err := cipher.NewGCM(block)
	require.NoError(t, err)

	p := &NATSIAMProvider{
		gcm:          gcm,
		bucketName:   "hive-iam-access-keys",
		bucketsReady: false,
		cache:        make(map[string]*cachedCredential),
		done:         make(chan struct{}),
	}

	_, err = p.LookupCredentials("AKIAEXAMPLE")
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrKeyNotFound),
		"should return ErrKeyNotFound when buckets not ready so chain falls back to config")
}

// --- IAMConfig validation tests ---

func TestNewNATSIAMProvider_MissingNATSUrl(t *testing.T) {
	_, err := NewNATSIAMProvider(&IAMConfig{
		MasterKeyPath: "/tmp/master.key",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nats_url is required")
}

func TestNewNATSIAMProvider_MissingMasterKeyPath(t *testing.T) {
	_, err := NewNATSIAMProvider(&IAMConfig{
		NATSUrl: "nats://localhost:4222",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "master_key_path is required")
}
