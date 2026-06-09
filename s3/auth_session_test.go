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

	data map[string][]byte
}

func (k *fakeKV) Get(key string) (nats.KeyValueEntry, error) {
	v, ok := k.data[key]
	if !ok {
		return nil, nats.ErrKeyNotFound
	}
	return fakeKVEntry{val: v}, nil
}

// --- session test helpers ---

const (
	testSessionAKID    = "ASIATESTSESSIONKEY01"
	testSessionAccount = "000000000001"
	testSessionUser    = "alice"
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
// serve the ASIA session path without a live NATS connection.
func newSessionProvider(k *masterkey.Key, sessions, users, policies map[string][]byte) *NATSIAMProvider {
	p := &NATSIAMProvider{
		gcm:            k.AEAD,
		cache:          make(map[string]*cachedCredential),
		done:           make(chan struct{}),
		sessionsBucket: &fakeKV{data: sessions},
		sessionsReady:  true,
	}
	if users != nil {
		p.usersBucket = &fakeKV{data: users}
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

// --- unit tests: lookupSessionCredentials ---

func TestLookupSession_UserPrincipalResolves(t *testing.T) {
	k := loadTestKey(t)
	const secret = "session-secret-value"
	sessions, users, policies := userSessionFixture(t, k, secret, time.Now().UTC().Add(time.Hour))
	p := newSessionProvider(k, sessions, users, policies)

	res, err := p.LookupCredentials(testSessionAKID)
	require.NoError(t, err)
	assert.Equal(t, secret, res.SecretAccessKey, "encrypted secret must round-trip back to the minted plaintext")
	assert.Equal(t, testSessionAccount, res.AccountID)
	assert.Equal(t, testSessionUser, res.UserName)
	assert.False(t, res.SkipPolicyCheck, "sessions never bypass policy/ownership checks")
	assert.Len(t, res.PolicyDocuments, 1, "user-session policies must resolve via resolveUserPolicies")
}

func TestLookupSession_AssumedRoleFailsClosed(t *testing.T) {
	k := loadTestKey(t)
	const secret = "role-secret"

	for _, principalType := range []string{"assumed-role", ""} {
		t.Run("principal="+principalType, func(t *testing.T) {
			cred := sessionCredential{
				AccessKeyID:     testSessionAKID,
				SecretEncrypted: encryptSessionSecret(t, k.AEAD, secret),
				AccountID:       testSessionAccount,
				PrincipalType:   principalType,
				SessionName:     "my-role-session",
				ExpiresAt:       time.Now().UTC().Add(time.Hour),
			}
			// No users/policies buckets: the assumed-role path must not touch them.
			p := newSessionProvider(k, map[string][]byte{testSessionAKID: mustMarshal(t, cred)}, nil, nil)

			res, err := p.LookupCredentials(testSessionAKID)
			require.NoError(t, err)
			assert.Equal(t, secret, res.SecretAccessKey)
			assert.Empty(t, res.PolicyDocuments, "assumed-role resolves to no policies (implicit deny)")
			assert.False(t, res.SkipPolicyCheck)
		})
	}
}

func TestLookupSession_Expired(t *testing.T) {
	k := loadTestKey(t)
	sessions, users, policies := userSessionFixture(t, k, "secret", time.Now().UTC().Add(-time.Minute))
	p := newSessionProvider(k, sessions, users, policies)

	_, err := p.LookupCredentials(testSessionAKID)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrKeyNotFound, "expired session must be rejected")
}

func TestLookupSession_UnknownKey(t *testing.T) {
	k := loadTestKey(t)
	p := newSessionProvider(k, map[string][]byte{}, nil, nil)

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
	p := newSessionProvider(k, map[string][]byte{"AKIAEXAMPLE": mustMarshal(t, cred)}, nil, nil)
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
	server := sessionMiddlewareServer(t, newSessionProvider(k, sessions, users, policies))

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
	server := sessionMiddlewareServer(t, newSessionProvider(k, sessions, users, policies))

	req := httptest.NewRequest(http.MethodGet, "/session-bucket", nil)
	signTestReq(t, req, nil, testSessionAKID, secret, "ap-southeast-2", "s3")
	status, nextCalled, _ := runMiddleware(t, server, req)
	assert.False(t, nextCalled, "expired session must not reach the handler")
	assert.Equal(t, http.StatusForbidden, status)
}
