package auth

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/iampolicy"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A legacy session record with an empty principal_type resolves as assumed-role
// everywhere else, so it must not read as an IAM user here either — that would
// hand aws:username the caller-chosen RoleSessionName.
func TestCredentialResult_IsIAMUser(t *testing.T) {
	for principalType, want := range map[string]bool{
		principalTypeUser:        true,
		principalTypeAssumedRole: false,
		"":                       false,
		"federated":              false,
	} {
		cred := &CredentialResult{UserName: "alice", PrincipalType: principalType}
		assert.Equal(t, want, cred.IsIAMUser(), "principalType %q", principalType)
	}
}

// --- ConfigProvider tests ---

func TestConfigProvider_Found(t *testing.T) {
	p := NewConfigProvider([]Entry{
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
	p := NewConfigProvider([]Entry{
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

func TestChainProvider_ConfigWins(t *testing.T) {
	// Config entries (service accounts) take priority over NATS IAM
	iam := &mockProvider{result: &CredentialResult{SecretAccessKey: "from-iam", AccountID: "acct-1"}}
	config := &mockProvider{result: &CredentialResult{SecretAccessKey: "from-config", AccountID: "acct-1", SkipPolicyCheck: true}}

	chain := NewChainProvider(iam, config)
	result, err := chain.LookupCredentials("AK1")
	require.NoError(t, err)
	assert.Equal(t, "from-config", result.SecretAccessKey)
	assert.True(t, result.SkipPolicyCheck, "config entries should skip policy check")
}

func TestChainProvider_IAMFallback(t *testing.T) {
	// Key not in config → NATS IAM resolves it with policies
	iam := &mockProvider{result: &CredentialResult{SecretAccessKey: "from-iam", AccountID: "acct-2"}}
	config := &mockProvider{err: ErrKeyNotFound}

	chain := NewChainProvider(iam, config)
	result, err := chain.LookupCredentials("AK1")
	require.NoError(t, err)
	assert.Equal(t, "from-iam", result.SecretAccessKey)
}

func TestChainProvider_IAMInfraError(t *testing.T) {
	// Config miss + NATS infra error must surface the error
	iam := &mockProvider{err: errors.New("NATS connection timeout")}
	config := &mockProvider{err: ErrKeyNotFound}

	chain := NewChainProvider(iam, config)
	_, err := chain.LookupCredentials("AK1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "NATS connection timeout")
}

func TestChainProvider_IAMInactiveKey(t *testing.T) {
	// Config miss + inactive key in NATS must surface the error
	iam := &mockProvider{err: fmt.Errorf("access key AK1 is inactive (status: Inactive)")}
	config := &mockProvider{err: ErrKeyNotFound}

	chain := NewChainProvider(iam, config)
	_, err := chain.LookupCredentials("AK1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "inactive")
}

func TestChainProvider_BothNotFound(t *testing.T) {
	iam := &mockProvider{err: ErrKeyNotFound}
	config := &mockProvider{err: ErrKeyNotFound}

	chain := NewChainProvider(iam, config)
	_, err := chain.LookupCredentials("AK1")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// Secret decryption (masterkey.Key.DecryptBase64) and the StringOrArr JSON forms are
// unit-tested in pkg/masterkey and pkg/iampolicy respectively; the provider just
// delegates to them.

// --- ErrKeyNotFound sentinel tests ---

func TestErrKeyNotFound_IsDetectable(t *testing.T) {
	wrapped := fmt.Errorf("%w: AK123", ErrKeyNotFound)
	assert.ErrorIs(t, wrapped, ErrKeyNotFound)

	unrelated := errors.New("NATS connection timeout")
	assert.NotErrorIs(t, unrelated, ErrKeyNotFound)
}

// --- IAMConfig validation tests ---

// --- Lazy bucket init tests ---

func TestNATSIAMProvider_LazyBucketsNotReady_InfraError(t *testing.T) {
	// When buckets aren't ready due to infrastructure issues (no JetStream context),
	// LookupCredentials should return an infrastructure error (NOT ErrKeyNotFound)
	// so the caller can return 500 instead of a misleading 403.
	p := &NATSIAMProvider{
		key:          loadTestKey(t),
		bucketName:   "spinifex-iam-access-keys",
		bucketsReady: false,
		cache:        make(map[string]*cachedCredential),
		done:         make(chan struct{}),
	}

	_, err := p.LookupCredentials("AKIAEXAMPLE")
	assert.Error(t, err)
	assert.NotErrorIs(t, err, ErrKeyNotFound,
		"infrastructure errors should NOT be mapped to ErrKeyNotFound")
	assert.Contains(t, err.Error(), "IAM lookup unavailable")
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

// applyNATSOptions resolves an option slice into the nats.Options it produces,
// so a test can assert on the settings rather than on a live connection.
func applyNATSOptions(t *testing.T, opts []nats.Option) nats.Options {
	t.Helper()
	var resolved nats.Options
	for _, opt := range opts {
		require.NoError(t, opt(&resolved))
	}
	return resolved
}

// TestNATSIAMOptions_ReconnectsForever guards the defect that took every
// authenticated S3 request out for 20 hours: with nats.go's default 60
// attempts at 2s, a NATS restart longer than ~2 minutes closed this connection
// permanently and no credential could be validated until predastore restarted.
func TestNATSIAMOptions_ReconnectsForever(t *testing.T) {
	resolved := applyNATSOptions(t, natsIAMOptions(&IAMConfig{NATSUrl: "nats://localhost:4222"}))

	assert.Equal(t, -1, resolved.MaxReconnect, "a bounded retry count silently ends IAM auth")
	assert.Equal(t, time.Second, resolved.ReconnectWait)
	assert.NotNil(t, resolved.DisconnectedErrCB, "an outage must be visible without waiting for a request to fail")
	assert.NotNil(t, resolved.ReconnectedCB)
}

func TestNATSIAMOptions_TokenOnlySetWhenConfigured(t *testing.T) {
	withToken := applyNATSOptions(t, natsIAMOptions(&IAMConfig{
		NATSUrl: "nats://localhost:4222", NATSToken: "secret",
	}))
	assert.Equal(t, "secret", withToken.Token)

	withoutToken := applyNATSOptions(t, natsIAMOptions(&IAMConfig{NATSUrl: "nats://localhost:4222"}))
	assert.Empty(t, withoutToken.Token)
}

// --- inline role policy resolution (resolveRolePolicies) ---

const inlineTestAccount = "000000000001"

// TestResolveRolePolicies_InlineAllow proves an inline Allow policy embedded in a
// role resolves and authorizes on the S3 data path, with no managed attachment.
func TestResolveRolePolicies_InlineAllow(t *testing.T) {
	roles := map[string][]byte{
		inlineTestAccount + ".InlineRole": mustMarshal(t, iamRole{
			RoleName:       "InlineRole",
			AccountID:      inlineTestAccount,
			InlinePolicies: map[string]string{"AllowS3": allowAllS3Policy},
		}),
	}
	p := &NATSIAMProvider{
		rolesBucket:    &fakeKV{data: roles},
		policiesBucket: &fakeKV{data: map[string][]byte{}},
	}

	docs, err := p.resolveRolePolicies(context.Background(), inlineTestAccount, "InlineRole")
	require.NoError(t, err)
	require.Len(t, docs, 1)
	assert.Equal(t, iampolicy.Allow, iampolicy.EvaluateWithKeys("s3:ListBucket", "arn:aws:s3:::any", docs, nil), "inline Allow must be honoured")
}

// TestResolveRolePolicies_InlineDenyOverridesManagedAllow proves inline and
// managed documents are evaluated together: an inline Deny overrides a managed
// Allow, the standard IAM deny-wins outcome.
func TestResolveRolePolicies_InlineDenyOverridesManagedAllow(t *testing.T) {
	roles := map[string][]byte{
		inlineTestAccount + ".DenyRole": mustMarshal(t, iamRole{
			RoleName:         "DenyRole",
			AccountID:        inlineTestAccount,
			AttachedPolicies: []string{"arn:aws:iam::" + inlineTestAccount + ":policy/AllowAll"},
			InlinePolicies:   map[string]string{"DenyS3": denyAllS3Policy},
		}),
	}
	policies := map[string][]byte{
		inlineTestAccount + ".AllowAll": mustMarshal(t, iamPolicy{
			PolicyName:     "AllowAll",
			PolicyDocument: allowAllS3Policy,
		}),
	}
	p := &NATSIAMProvider{
		rolesBucket:    &fakeKV{data: roles},
		policiesBucket: &fakeKV{data: policies},
	}

	docs, err := p.resolveRolePolicies(context.Background(), inlineTestAccount, "DenyRole")
	require.NoError(t, err)
	require.Len(t, docs, 2, "managed Allow and inline Deny must both resolve")
	assert.Equal(t, iampolicy.Deny, iampolicy.EvaluateWithKeys("s3:ListBucket", "arn:aws:s3:::any", docs, nil), "inline Deny must override managed Allow")
}

// TestResolveRolePolicies_InlineMalformed proves a corrupt inline document fails
// closed rather than silently resolving to a partial set.
func TestResolveRolePolicies_InlineMalformed(t *testing.T) {
	roles := map[string][]byte{
		inlineTestAccount + ".BadRole": mustMarshal(t, iamRole{
			RoleName:       "BadRole",
			AccountID:      inlineTestAccount,
			InlinePolicies: map[string]string{"Broken": "{not json"},
		}),
	}
	p := &NATSIAMProvider{
		rolesBucket:    &fakeKV{data: roles},
		policiesBucket: &fakeKV{data: map[string][]byte{}},
	}

	_, err := p.resolveRolePolicies(context.Background(), inlineTestAccount, "BadRole")
	assert.Error(t, err, "a malformed inline document must fail closed")
}

// allowed reports whether the S3 action on resource is permitted by the
// resolved policies.
func allowed(action, resource string, policies []iampolicy.PolicyDocument) bool {
	return iampolicy.EvaluateWithKeys(action, resource, policies, nil) == iampolicy.Allow
}

// A Bool condition leaf is an AWS-routine shape. If it failed to unmarshal, the
// whole principal's policy load would fail and read as "no policies" — a total
// denial reported far from its cause.
func TestResolveRolePolicies_BoolConditionLeafResolves(t *testing.T) {
	const conditioned = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*",
	 "Resource":"*","Condition":{"Bool":{"aws:SecureTransport":true}}}]}`
	roles := map[string][]byte{
		inlineTestAccount + ".TLSRole": mustMarshal(t, iamRole{
			RoleName:       "TLSRole",
			AccountID:      inlineTestAccount,
			InlinePolicies: map[string]string{"TLSOnly": conditioned},
		}),
	}
	p := &NATSIAMProvider{
		rolesBucket:    &fakeKV{data: roles},
		policiesBucket: &fakeKV{data: map[string][]byte{}},
	}

	docs, err := p.resolveRolePolicies(context.Background(), inlineTestAccount, "TLSRole")
	require.NoError(t, err)
	require.Len(t, docs, 1)

	keys := iampolicy.ConditionKeys{iampolicy.KeySecureTransport: "true"}
	assert.Equal(t, iampolicy.Allow,
		iampolicy.EvaluateWithKeys("s3:ListBucket", "arn:aws:s3:::any", docs, keys))
	assert.Equal(t, iampolicy.Deny,
		iampolicy.EvaluateWithKeys("s3:ListBucket", "arn:aws:s3:::any", docs,
			iampolicy.ConditionKeys{iampolicy.KeySecureTransport: "false"}))
}
