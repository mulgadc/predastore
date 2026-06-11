// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package s3

import (
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- fake nats.KeyValue (embeds the interface; only Get is exercised) ---

type fakeKVEntry struct {
	nats.KeyValueEntry

	val []byte
}

func (e fakeKVEntry) Value() []byte { return e.val }

type fakeKV struct {
	nats.KeyValue

	data   map[string][]byte
	getErr error // when set, Get returns this error (simulates a non-NotFound KV fault)
}

func (k *fakeKV) Get(key string) (nats.KeyValueEntry, error) {
	if k.getErr != nil {
		return nil, k.getErr
	}
	v, ok := k.data[key]
	if !ok {
		return nil, nats.ErrKeyNotFound
	}
	return fakeKVEntry{val: v}, nil
}

// --- session test helpers ---

const (
	testSessionAKID       = "ASIATESTSESSIONKEY01"
	testSessionAccount    = "000000000001"
	testSessionUser       = "alice"
	testSessionRole       = "InstanceRole"
	testSessionInstanceID = "i-0123456789abcdef0"
	testSessionRoleARN    = "arn:aws:iam::" + testSessionAccount + ":role/" + testSessionRole

	allowAllS3Policy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	denyAllS3Policy  = `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:*","Resource":"*"}]}`
)

// loadTestKey writes a random 32-byte master key to disk and loads it, mirroring
// how the real IAM master key is loaded at startup. The session path only needs
// the AEAD (to decrypt the session secret), exactly like the AKIA path.
func loadTestKey(t *testing.T) *masterkey.Key {
	t.Helper()
	raw := make([]byte, masterkey.MasterKeySize)
	_, err := rand.Read(raw)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "master.key")
	require.NoError(t, os.WriteFile(path, raw, 0o600))
	k, err := masterkey.LoadShared(path)
	require.NoError(t, err)
	return k
}

// encryptSessionSecret seals plaintext exactly as spinifex's
// handlers_iam.EncryptSecret does: base64(nonce + ciphertext + tag).
func encryptSessionSecret(t *testing.T, gcm cipher.AEAD, plaintext string) string {
	t.Helper()
	nonce := make([]byte, gcm.NonceSize())
	_, err := rand.Read(nonce)
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(gcm.Seal(nonce, nonce, []byte(plaintext), nil))
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// newSessionProvider builds a NATSIAMProvider wired to fake KV buckets, ready to
// serve the ASIA session path without a live NATS connection. The users/roles/
// policies buckets share one readiness flag (as in the real provider), so they
// are wired together whenever any IAM map is supplied.
func newSessionProvider(k *masterkey.Key, sessions, users, roles, policies map[string][]byte) *NATSIAMProvider {
	p := &NATSIAMProvider{
		gcm:            k.AEAD,
		cache:          make(map[string]*cachedCredential),
		done:           make(chan struct{}),
		sessionsBucket: &fakeKV{data: sessions},
		sessionsReady:  true,
	}
	if users != nil || roles != nil || policies != nil {
		p.usersBucket = &fakeKV{data: users}
		p.rolesBucket = &fakeKV{data: roles}
		p.policiesBucket = &fakeKV{data: policies}
		p.bucketsReady = true
	}
	return p
}

// userSessionFixture builds a valid "user" principal session: the encrypted
// secret plus the user+admin-policy KV records needed for resolveUserPolicies.
func userSessionFixture(t *testing.T, k *masterkey.Key, secret string, expiresAt time.Time) (sessions, users, policies map[string][]byte) {
	t.Helper()
	cred := sessionCredential{
		AccessKeyID:     testSessionAKID,
		SecretEncrypted: encryptSessionSecret(t, k.AEAD, secret),
		AccountID:       testSessionAccount,
		PrincipalType:   "user",
		SessionName:     testSessionUser,
		ExpiresAt:       expiresAt,
	}
	sessions = map[string][]byte{testSessionAKID: mustMarshal(t, cred)}
	users = map[string][]byte{
		testSessionAccount + "." + testSessionUser: mustMarshal(t, iamUser{
			UserName:         testSessionUser,
			AccountID:        testSessionAccount,
			AttachedPolicies: []string{"arn:aws:iam::" + testSessionAccount + ":policy/AdministratorAccess"},
		}),
	}
	policies = map[string][]byte{
		testSessionAccount + ".AdministratorAccess": mustMarshal(t, iamPolicy{
			PolicyName:     "AdministratorAccess",
			PolicyDocument: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`,
		}),
	}
	return sessions, users, policies
}

// assumedRoleSession builds an "assumed-role" principal session record for the
// given role ARN. SessionName carries the instance ID (not the role), exactly
// as the STS producer sets it; the secret is sealed as spinifex mints it.
func assumedRoleSession(t *testing.T, k *masterkey.Key, secret, roleARN string, expiresAt time.Time) map[string][]byte {
	t.Helper()
	cred := sessionCredential{
		AccessKeyID:       testSessionAKID,
		SecretEncrypted:   encryptSessionSecret(t, k.AEAD, secret),
		AccountID:         testSessionAccount,
		PrincipalType:     principalTypeAssumedRole,
		SessionName:       testSessionInstanceID,
		UnderlyingRoleARN: roleARN,
		ExpiresAt:         expiresAt,
	}
	return map[string][]byte{testSessionAKID: mustMarshal(t, cred)}
}

// roleWithPolicy builds the roles + policies KV records for testSessionRole
// attaching a single managed policy named policyName carrying policyDoc.
func roleWithPolicy(t *testing.T, policyName, policyDoc string) (roles, policies map[string][]byte) {
	t.Helper()
	roles = map[string][]byte{
		testSessionAccount + "." + testSessionRole: mustMarshal(t, iamRole{
			RoleName:         testSessionRole,
			AccountID:        testSessionAccount,
			AttachedPolicies: []string{"arn:aws:iam::" + testSessionAccount + ":policy/" + policyName},
		}),
	}
	policies = map[string][]byte{
		testSessionAccount + "." + policyName: mustMarshal(t, iamPolicy{
			PolicyName:     policyName,
			PolicyDocument: policyDoc,
		}),
	}
	return roles, policies
}

// --- unit tests: lookupSessionCredentials ---

func TestLookupSession_UserPrincipalResolves(t *testing.T) {
	k := loadTestKey(t)
	const secret = "session-secret-value"
	sessions, users, policies := userSessionFixture(t, k, secret, time.Now().UTC().Add(time.Hour))
	p := newSessionProvider(k, sessions, users, nil, policies)

	res, err := p.LookupCredentials(testSessionAKID)
	require.NoError(t, err)
	assert.Equal(t, secret, res.SecretAccessKey, "encrypted secret must round-trip back to the minted plaintext")
	assert.Equal(t, testSessionAccount, res.AccountID)
	assert.Equal(t, testSessionUser, res.UserName)
	assert.False(t, res.SkipPolicyCheck, "sessions never bypass policy/ownership checks")
	assert.Len(t, res.PolicyDocuments, 1, "user-session policies must resolve via resolveUserPolicies")
}

// TestLookupSession_AssumedRoleEmptyARN covers an assumed-role session whose
// underlying_role_arn is absent (e.g. a legacy record predating the field): the
// role cannot be identified, so it must fail closed (no policies → implicit
// deny) without touching any bucket — preserving the prior safe behaviour. The
// empty principal_type is treated as assumed-role for backward compatibility.
func TestLookupSession_AssumedRoleEmptyARN(t *testing.T) {
	k := loadTestKey(t)
	const secret = "role-secret"

	for _, principalType := range []string{principalTypeAssumedRole, ""} {
		t.Run("principal="+principalType, func(t *testing.T) {
			cred := sessionCredential{
				AccessKeyID:     testSessionAKID,
				SecretEncrypted: encryptSessionSecret(t, k.AEAD, secret),
				AccountID:       testSessionAccount,
				PrincipalType:   principalType,
				SessionName:     testSessionInstanceID,
				// UnderlyingRoleARN intentionally empty.
				ExpiresAt: time.Now().UTC().Add(time.Hour),
			}
			// No IAM buckets: an empty ARN must short-circuit before touching them.
			p := newSessionProvider(k, map[string][]byte{testSessionAKID: mustMarshal(t, cred)}, nil, nil, nil)

			res, err := p.LookupCredentials(testSessionAKID)
			require.NoError(t, err)
			assert.Equal(t, secret, res.SecretAccessKey)
			assert.Empty(t, res.PolicyDocuments, "an unresolvable role ARN resolves to no policies (implicit deny)")
			assert.False(t, res.SkipPolicyCheck)
		})
	}
}

// TestLookupSession_AssumedRoleResolvesPolicies is the core of this feature: an
// assumed-role session whose underlying role attaches an S3-allowing managed
// policy must resolve that policy document so the request can be authorized.
func TestLookupSession_AssumedRoleResolvesPolicies(t *testing.T) {
	k := loadTestKey(t)
	const secret = "role-secret-value"
	roles, policies := roleWithPolicy(t, "S3FullAccess", allowAllS3Policy)
	sessions := assumedRoleSession(t, k, secret, testSessionRoleARN, time.Now().UTC().Add(time.Hour))
	p := newSessionProvider(k, sessions, nil, roles, policies)

	res, err := p.LookupCredentials(testSessionAKID)
	require.NoError(t, err)
	assert.Equal(t, secret, res.SecretAccessKey, "the encrypted session secret must round-trip")
	assert.Equal(t, testSessionAccount, res.AccountID)
	assert.Equal(t, testSessionInstanceID, res.UserName, "UserName carries the session name (instance ID)")
	assert.False(t, res.SkipPolicyCheck, "sessions never bypass policy/ownership checks")
	require.Len(t, res.PolicyDocuments, 1, "the role's attached managed policy must resolve")
	assert.True(t,
		evaluateS3Access("s3:ListBucket", "arn:aws:s3:::session-bucket", res.PolicyDocuments),
		"the resolved role policy must authorize the allowed action")
}

// TestLookupSession_AssumedRoleNoAttachedPolicies: a role with zero attached
// policies resolves to an empty document set → implicit deny (not an error).
func TestLookupSession_AssumedRoleNoAttachedPolicies(t *testing.T) {
	k := loadTestKey(t)
	roles := map[string][]byte{
		testSessionAccount + "." + testSessionRole: mustMarshal(t, iamRole{
			RoleName:         testSessionRole,
			AccountID:        testSessionAccount,
			AttachedPolicies: nil,
		}),
	}
	sessions := assumedRoleSession(t, k, "secret", testSessionRoleARN, time.Now().UTC().Add(time.Hour))
	p := newSessionProvider(k, sessions, nil, roles, nil)

	res, err := p.LookupCredentials(testSessionAKID)
	require.NoError(t, err)
	assert.Empty(t, res.PolicyDocuments, "a role with no attached policies → implicit deny")
	assert.False(t, evaluateS3Access("s3:ListBucket", "arn:aws:s3:::session-bucket", res.PolicyDocuments))
}

// TestLookupSession_AssumedRoleAccountMismatch: an ARN whose embedded account
// disagrees with the session's account_id must fail closed rather than resolve a
// same-named role in the session's account.
func TestLookupSession_AssumedRoleAccountMismatch(t *testing.T) {
	k := loadTestKey(t)
	// The roles/policies exist in the session's account, but the ARN names a
	// different account — a corrupt record that must never resolve.
	roles, policies := roleWithPolicy(t, "S3FullAccess", allowAllS3Policy)
	foreignARN := "arn:aws:iam::999999999999:role/" + testSessionRole
	sessions := assumedRoleSession(t, k, "secret", foreignARN, time.Now().UTC().Add(time.Hour))
	p := newSessionProvider(k, sessions, nil, roles, policies)

	res, err := p.LookupCredentials(testSessionAKID)
	require.NoError(t, err)
	assert.Empty(t, res.PolicyDocuments, "ARN account ≠ session account must fail closed, never resolve")
}

// TestLookupSession_AssumedRoleEmptyAccountARN: an ARN with a blank account field
// (arn:aws:iam:::role/Name) is malformed — a real role ARN always carries an
// account — so it must fail closed rather than resolve the named role in the
// session's own account, even though the account does not visibly "disagree".
func TestLookupSession_AssumedRoleEmptyAccountARN(t *testing.T) {
	k := loadTestKey(t)
	// The role+policy exist in the session's account; only the ARN is corrupt.
	roles, policies := roleWithPolicy(t, "S3FullAccess", allowAllS3Policy)
	accountlessARN := "arn:aws:iam:::role/" + testSessionRole
	sessions := assumedRoleSession(t, k, "secret", accountlessARN, time.Now().UTC().Add(time.Hour))
	p := newSessionProvider(k, sessions, nil, roles, policies)

	res, err := p.LookupCredentials(testSessionAKID)
	require.NoError(t, err)
	assert.Empty(t, res.PolicyDocuments, "an account-less role ARN must fail closed, never resolve")
}

// TestLookupSession_AssumedRoleDeletedRole: a session referencing a role that no
// longer exists is a stale principal → 403 (ErrKeyNotFound), not a 500.
func TestLookupSession_AssumedRoleDeletedRole(t *testing.T) {
	k := loadTestKey(t)
	sessions := assumedRoleSession(t, k, "secret", testSessionRoleARN, time.Now().UTC().Add(time.Hour))
	// Empty (non-nil) roles bucket → the role Get returns ErrKeyNotFound.
	p := newSessionProvider(k, sessions, nil, map[string][]byte{}, map[string][]byte{})

	_, err := p.LookupCredentials(testSessionAKID)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrKeyNotFound, "a deleted role principal maps to 403, not 500")
}

// TestLookupSession_AssumedRoleKVError: a non-NotFound fault on the role lookup
// is infrastructure (→ 500), never a misleading ErrKeyNotFound (→ 403).
func TestLookupSession_AssumedRoleKVError(t *testing.T) {
	k := loadTestKey(t)
	sessions := assumedRoleSession(t, k, "secret", testSessionRoleARN, time.Now().UTC().Add(time.Hour))
	p := newSessionProvider(k, sessions, nil, map[string][]byte{}, map[string][]byte{})
	p.rolesBucket = &fakeKV{getErr: errors.New("nats: connection closed")}

	_, err := p.LookupCredentials(testSessionAKID)
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrKeyNotFound), "infra faults must not be mapped to ErrKeyNotFound")
	assert.Contains(t, err.Error(), "resolve session policies")
}

// TestLookupSession_AssumedRoleMissingPolicy: the role record exists and attaches
// a policy, but that managed policy has been detached/deleted — a stale principal
// → 403 (ErrKeyNotFound), not a 500.
func TestLookupSession_AssumedRoleMissingPolicy(t *testing.T) {
	k := loadTestKey(t)
	roles, _ := roleWithPolicy(t, "S3FullAccess", allowAllS3Policy)
	sessions := assumedRoleSession(t, k, "secret", testSessionRoleARN, time.Now().UTC().Add(time.Hour))
	// Empty (non-nil) policies bucket → the attached policy Get returns ErrKeyNotFound.
	p := newSessionProvider(k, sessions, nil, roles, map[string][]byte{})

	_, err := p.LookupCredentials(testSessionAKID)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrKeyNotFound, "a detached/deleted managed policy maps to 403, not 500")
}

// TestLookupSession_AssumedRolePolicyKVError: a non-NotFound fault on the policy
// lookup (role resolved fine) is infrastructure (→ 500), never a misleading
// ErrKeyNotFound (→ 403).
func TestLookupSession_AssumedRolePolicyKVError(t *testing.T) {
	k := loadTestKey(t)
	roles, _ := roleWithPolicy(t, "S3FullAccess", allowAllS3Policy)
	sessions := assumedRoleSession(t, k, "secret", testSessionRoleARN, time.Now().UTC().Add(time.Hour))
	p := newSessionProvider(k, sessions, nil, roles, map[string][]byte{})
	p.policiesBucket = &fakeKV{getErr: errors.New("nats: connection closed")}

	_, err := p.LookupCredentials(testSessionAKID)
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrKeyNotFound), "infra faults must not be mapped to ErrKeyNotFound")
	assert.Contains(t, err.Error(), "resolve session policies")
}

// TestLookupSession_AssumedRoleSkipsUnparseablePolicyARN: an unparseable entry in
// the role's attached policies is skipped (logged), not treated as an error — the
// remaining valid policies still resolve.
func TestLookupSession_AssumedRoleSkipsUnparseablePolicyARN(t *testing.T) {
	k := loadTestKey(t)
	roles := map[string][]byte{
		testSessionAccount + "." + testSessionRole: mustMarshal(t, iamRole{
			RoleName:  testSessionRole,
			AccountID: testSessionAccount,
			AttachedPolicies: []string{
				"not-an-arn",
				"arn:aws:iam::" + testSessionAccount + ":policy/S3FullAccess",
			},
		}),
	}
	policies := map[string][]byte{
		testSessionAccount + ".S3FullAccess": mustMarshal(t, iamPolicy{
			PolicyName:     "S3FullAccess",
			PolicyDocument: allowAllS3Policy,
		}),
	}
	sessions := assumedRoleSession(t, k, "secret", testSessionRoleARN, time.Now().UTC().Add(time.Hour))
	p := newSessionProvider(k, sessions, nil, roles, policies)

	res, err := p.LookupCredentials(testSessionAKID)
	require.NoError(t, err, "an unparseable ARN must be skipped, not error the whole lookup")
	require.Len(t, res.PolicyDocuments, 1, "only the one valid attached policy resolves")
	assert.True(t, evaluateS3Access("s3:ListBucket", "arn:aws:s3:::session-bucket", res.PolicyDocuments))
}

// TestLookupSession_UnrecognisedPrincipalType: a principal_type that is neither
// "user" nor "assumed-role" (nor empty) fails closed (implicit deny).
func TestLookupSession_UnrecognisedPrincipalType(t *testing.T) {
	k := loadTestKey(t)
	const secret = "weird-secret"
	cred := sessionCredential{
		AccessKeyID:     testSessionAKID,
		SecretEncrypted: encryptSessionSecret(t, k.AEAD, secret),
		AccountID:       testSessionAccount,
		PrincipalType:   "federated-user",
		SessionName:     "whoever",
		ExpiresAt:       time.Now().UTC().Add(time.Hour),
	}
	p := newSessionProvider(k, map[string][]byte{testSessionAKID: mustMarshal(t, cred)}, nil, nil, nil)

	res, err := p.LookupCredentials(testSessionAKID)
	require.NoError(t, err)
	assert.Empty(t, res.PolicyDocuments, "an unrecognised principal_type must fail closed (implicit deny)")
}

// --- unit tests: iamRole ---

func TestIamRole_UnmarshalsSpinifexJSON(t *testing.T) {
	// A representative spinifex Role record. Extra fields (trust policy, ARN,
	// tags) must be ignored — only the three mirror fields are read.
	raw := `{
		"role_name": "InstanceRole",
		"role_id": "AROAEXAMPLE",
		"account_id": "000000000001",
		"arn": "arn:aws:iam::000000000001:role/InstanceRole",
		"path": "/",
		"assume_role_policy_document": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"ec2.amazonaws.com\"},\"Action\":\"sts:AssumeRole\"}]}",
		"attached_policies": ["arn:aws:iam::000000000001:policy/S3FullAccess"],
		"tags": []
	}`
	var role iamRole
	require.NoError(t, json.Unmarshal([]byte(raw), &role))
	assert.Equal(t, "InstanceRole", role.RoleName)
	assert.Equal(t, "000000000001", role.AccountID)
	assert.Equal(t, []string{"arn:aws:iam::000000000001:policy/S3FullAccess"}, role.AttachedPolicies)
}

func TestLookupSession_Expired(t *testing.T) {
	k := loadTestKey(t)
	sessions, users, policies := userSessionFixture(t, k, "secret", time.Now().UTC().Add(-time.Minute))
	p := newSessionProvider(k, sessions, users, nil, policies)

	_, err := p.LookupCredentials(testSessionAKID)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrKeyNotFound, "expired session must be rejected")
}

func TestLookupSession_UnknownKey(t *testing.T) {
	k := loadTestKey(t)
	p := newSessionProvider(k, map[string][]byte{}, nil, nil, nil)

	_, err := p.LookupCredentials("ASIANOSUCHKEY00000000")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// TestLookupSession_NonASIAUnaffected proves the prefix dispatch: an AKIA key is
// never resolved via the session bucket even when a same-named record exists
// there — it falls through to the AKIA path (which here is uninitialised, so it
// surfaces an infra error rather than a successful session resolution).
func TestLookupSession_NonASIAUnaffected(t *testing.T) {
	k := loadTestKey(t)
	cred := sessionCredential{
		AccessKeyID:     "AKIAEXAMPLE",
		SecretEncrypted: encryptSessionSecret(t, k.AEAD, "secret"),
		AccountID:       testSessionAccount,
		PrincipalType:   "user",
		SessionName:     testSessionUser,
		ExpiresAt:       time.Now().UTC().Add(time.Hour),
	}
	p := newSessionProvider(k, map[string][]byte{"AKIAEXAMPLE": mustMarshal(t, cred)}, nil, nil, nil)
	// AKIA buckets are unbootstrapped (js is nil), so a (wrong) session resolution
	// would be the only way this could succeed.
	p.bucketsReady = false

	_, err := p.LookupCredentials("AKIAEXAMPLE")
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrKeyNotFound), "AKIA key must hit the AKIA path, not the session bucket")
	assert.Contains(t, err.Error(), "IAM lookup unavailable")
}

// TestLookupSession_NoJetStream_InfraError covers the non-bootstrap failure to
// open the session bucket: it must propagate as an infra error (→ 500), never a
// misleading ErrKeyNotFound (→ 403).
func TestLookupSession_NoJetStream_InfraError(t *testing.T) {
	k := loadTestKey(t)
	p := &NATSIAMProvider{
		gcm:           k.AEAD,
		cache:         make(map[string]*cachedCredential),
		done:          make(chan struct{}),
		sessionsReady: false, // force ensureSessionsBucket, which fails: js is nil
	}

	_, err := p.LookupCredentials(testSessionAKID)
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrKeyNotFound), "infra errors must not be mapped to ErrKeyNotFound")
	assert.Contains(t, err.Error(), "session credential lookup unavailable")
}

// --- middleware test: ASIA-signed request end to end through sigV4AuthMiddleware ---

func sessionMiddlewareServer(t *testing.T, p *NATSIAMProvider) *HTTP2Server {
	t.Helper()
	cfg := &Config{
		Region: "ap-southeast-2",
		Buckets: []S3_Buckets{{
			Name: "session-bucket", Region: "ap-southeast-2", Type: "distributed",
			Public: false, AccountID: testSessionAccount,
		}},
	}
	be := &stubBackend{buckets: map[string]*backend.BucketMetadata{
		"session-bucket": {Name: "session-bucket", Region: "ap-southeast-2", AccountID: testSessionAccount},
	}}
	return NewHTTP2ServerWithBackend(cfg, be, p)
}

func TestSigV4Middleware_SessionCredential(t *testing.T) {
	k := loadTestKey(t)
	const secret = "session-secret-value"
	sessions, users, policies := userSessionFixture(t, k, secret, time.Now().UTC().Add(time.Hour))
	server := sessionMiddlewareServer(t, newSessionProvider(k, sessions, users, nil, policies))

	t.Run("valid session passes auth", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/session-bucket", nil)
		signTestReq(t, req, nil, testSessionAKID, secret, "ap-southeast-2", "s3")
		status, nextCalled, _ := runMiddleware(t, server, req)
		assert.True(t, nextCalled, "valid ASIA session signature must pass through to the handler")
		assert.Equal(t, http.StatusOK, status)
	})

	t.Run("unknown session key rejected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/session-bucket", nil)
		signTestReq(t, req, nil, "ASIAUNKNOWNKEY000000", secret, "ap-southeast-2", "s3")
		status, nextCalled, _ := runMiddleware(t, server, req)
		assert.False(t, nextCalled)
		assert.Equal(t, http.StatusForbidden, status)
	})
}

func TestSigV4Middleware_SessionExpired(t *testing.T) {
	k := loadTestKey(t)
	const secret = "session-secret-value"
	sessions, users, policies := userSessionFixture(t, k, secret, time.Now().UTC().Add(-time.Minute))
	server := sessionMiddlewareServer(t, newSessionProvider(k, sessions, users, nil, policies))

	req := httptest.NewRequest(http.MethodGet, "/session-bucket", nil)
	signTestReq(t, req, nil, testSessionAKID, secret, "ap-southeast-2", "s3")
	status, nextCalled, _ := runMiddleware(t, server, req)
	assert.False(t, nextCalled, "expired session must not reach the handler")
	assert.Equal(t, http.StatusForbidden, status)
}

// TestSigV4Middleware_AssumedRole exercises the IMDS → S3 path end to end: an
// ASIA-signed request for an assumed-role session is authorized iff the
// underlying role's managed policy permits the action on the resource.
func TestSigV4Middleware_AssumedRole(t *testing.T) {
	k := loadTestKey(t)
	const secret = "role-secret-value"

	t.Run("role policy permits → 200", func(t *testing.T) {
		roles, policies := roleWithPolicy(t, "S3FullAccess", allowAllS3Policy)
		sessions := assumedRoleSession(t, k, secret, testSessionRoleARN, time.Now().UTC().Add(time.Hour))
		server := sessionMiddlewareServer(t, newSessionProvider(k, sessions, nil, roles, policies))

		req := httptest.NewRequest(http.MethodGet, "/session-bucket", nil)
		signTestReq(t, req, nil, testSessionAKID, secret, "ap-southeast-2", "s3")
		status, nextCalled, _ := runMiddleware(t, server, req)
		assert.True(t, nextCalled, "an allowed assumed-role request must pass through to the handler")
		assert.Equal(t, http.StatusOK, status)
	})

	t.Run("role policy denies → 403", func(t *testing.T) {
		roles, policies := roleWithPolicy(t, "S3Deny", denyAllS3Policy)
		sessions := assumedRoleSession(t, k, secret, testSessionRoleARN, time.Now().UTC().Add(time.Hour))
		server := sessionMiddlewareServer(t, newSessionProvider(k, sessions, nil, roles, policies))

		req := httptest.NewRequest(http.MethodGet, "/session-bucket", nil)
		signTestReq(t, req, nil, testSessionAKID, secret, "ap-southeast-2", "s3")
		status, nextCalled, _ := runMiddleware(t, server, req)
		assert.False(t, nextCalled, "a denied assumed-role request must not reach the handler")
		assert.Equal(t, http.StatusForbidden, status)
	})
}
