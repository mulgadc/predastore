package auth

import (
	"testing"

	"github.com/mulgadc/bluebottle/pkg/iampolicy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfigProvider_HonoursPolicyRules pins that a config entry declaring
// policy rules is evaluated as a scoped credential, while an omitted policy
// keeps the unrestricted behaviour the system root key relies on.
func TestConfigProvider_HonoursPolicyRules(t *testing.T) {
	p, err := NewConfigProvider([]Entry{
		{AccessKeyID: "AKIAROOT", SecretAccessKey: "secret", AccountID: "000000000000"},
		{AccessKeyID: "AKIASCOPE", SecretAccessKey: "secret", AccountID: "000000000002",
			Policy: []PolicyRule{{Bucket: "backup", Actions: []string{"s3:GetObject"}}}},
	})
	require.NoError(t, err)

	root, err := p.LookupCredentials("AKIAROOT")
	require.NoError(t, err)
	assert.True(t, root.SkipPolicyCheck, "an entry without a policy stays unrestricted")
	assert.Empty(t, root.PolicyDocuments)

	scoped, err := p.LookupCredentials("AKIASCOPE")
	require.NoError(t, err)
	assert.False(t, scoped.SkipPolicyCheck, "a scoped entry must not bypass the policy check")
	require.Len(t, scoped.PolicyDocuments, 1)

	get := func(action, resource string) iampolicy.Decision {
		return iampolicy.EvaluateWithKeys(action, resource, scoped.PolicyDocuments, nil)
	}
	assert.Equal(t, iampolicy.Allow, get("s3:GetObject", "arn:aws:s3:::backup/keys.db"),
		"the granted action on the granted bucket must be allowed")
	assert.Equal(t, iampolicy.Allow, get("s3:GetObject", "arn:aws:s3:::backup"),
		"the bucket ARN itself is granted too")
	assert.NotEqual(t, iampolicy.Allow, get("s3:PutObject", "arn:aws:s3:::backup/keys.db"),
		"an ungranted action must be denied")
	assert.NotEqual(t, iampolicy.Allow, get("s3:GetObject", "arn:aws:s3:::other/keys.db"),
		"an ungranted bucket must be denied")
}

// TestConfigProvider_ExplicitEmptyPolicyDeniesAll pins the presence-tracked
// distinction: `policy = []` in the config means the account can do nothing,
// not that it is unrestricted like an omitted policy.
func TestConfigProvider_ExplicitEmptyPolicyDeniesAll(t *testing.T) {
	p, err := NewConfigProvider([]Entry{
		{AccessKeyID: "AKIAEMPTY", SecretAccessKey: "secret", AccountID: "000000000003",
			Policy: []PolicyRule{}},
	})
	require.NoError(t, err)

	cred, err := p.LookupCredentials("AKIAEMPTY")
	require.NoError(t, err)
	assert.False(t, cred.SkipPolicyCheck, "an explicitly empty policy must not bypass the check")
	assert.NotEqual(t, iampolicy.Allow, iampolicy.EvaluateWithKeys(
		"s3:GetObject", "arn:aws:s3:::any/key", cred.PolicyDocuments, nil),
		"an empty policy denies every action")
}

// TestConfigProvider_RejectsWildcardBucketSelectors pins startup validation:
// a bucket selector is `*` or a real bucket name, never a partial pattern the
// schema does not advertise.
func TestConfigProvider_RejectsWildcardBucketSelectors(t *testing.T) {
	_, err := NewConfigProvider([]Entry{
		{AccessKeyID: "AKIABAD", SecretAccessKey: "secret", AccountID: "000000000004",
			Policy: []PolicyRule{{Bucket: "back*", Actions: []string{"s3:*"}}}},
	})
	assert.Error(t, err, "a partial wildcard must fail at construction, not evaluate broadly")

	_, err = NewConfigProvider([]Entry{
		{AccessKeyID: "AKIASTAR", SecretAccessKey: "secret", AccountID: "000000000004",
			Policy: []PolicyRule{{Bucket: "*", Actions: []string{"s3:*"}}}},
	})
	assert.NoError(t, err, "the documented whole-store selector stays valid")
}

// TestConfigProvider_UnknownKeyFailsClosed pins the not-found path.
func TestConfigProvider_UnknownKeyFailsClosed(t *testing.T) {
	p, err := NewConfigProvider(nil)
	require.NoError(t, err)
	_, err = p.LookupCredentials("AKIANONE")
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// BenchmarkConfigProvider_LookupCredentials measures the per-request cost of
// a lookup, which the gate pays on every signed request.
func BenchmarkConfigProvider_LookupCredentials(b *testing.B) {
	entries := make([]Entry, 0, 9)
	entries = append(entries, Entry{AccessKeyID: "AKIASCOPE", SecretAccessKey: "secret", AccountID: "000000000002",
		Policy: []PolicyRule{{Bucket: "backup", Actions: []string{"s3:GetObject", "s3:PutObject", "s3:ListBucket"}}}})
	for i := range 8 {
		entries = append(entries, Entry{AccessKeyID: "AKIAOTHER" + string(rune('0'+i)), SecretAccessKey: "secret", AccountID: "000000000009"})
	}
	p, err := NewConfigProvider(entries)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.LookupCredentials("AKIASCOPE"); err != nil {
			b.Fatal(err)
		}
	}
}
