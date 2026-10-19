package auth_test

// CURRENT-behaviour characterisation of how the S3 gate consumes Spinifex IAM
// state (access keys, users, policies, STS sessions read straight from the
// spinifex-iam-* KV buckets). Every test pins today's observable outcome so a
// later migration onto an IAM enforcement projection can prove what it kept
// and what it deliberately changed. Lines marked CURRENT: are outcomes that
// are insecure or diverge from AWS; they are pinned, not endorsed.

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/mulgadc/predastore/internal/gate"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	charPrivateBucket = "session-bucket"
	charPublicBucket  = "char-public-bucket"
	allowAllS3        = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	allowGetOnly      = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`
	allowThenDenyHead = `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"s3:*","Resource":"*"},` +
		`{"Effect":"Deny","Action":"s3:ListBucket","Resource":"*"}]}`
)

// charGate builds a gate with one private and one public config-defined
// bucket, both owned by the fixture account, over the given provider.
func charGate(t *testing.T, p auth.CredentialProvider) http.Handler {
	t.Helper()
	cert, key, _ := testcerts.Generate(t)
	s, err := gate.New(gate.Config{
		Region: sessionRegion,
		NodeID: 1,
		Buckets: []handlers.BucketConfig{
			{Name: charPrivateBucket, Region: sessionRegion, AccountID: auth.TestSessionAccount},
			{Name: charPublicBucket, Region: sessionRegion, AccountID: auth.TestSessionAccount, Public: true},
		},
		TLSCert:  cert,
		TLSKey:   key,
		Meta:     noMeta{},
		Blob:     noBlob{},
		CredProv: p,
	})
	require.NoError(t, err)
	return s
}

type s3Reply struct {
	Status  int    `xml:"-"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// charServe signs (when accessKey is set) and serves one request, returning
// the status and, for an error body, its S3 code and message.
func charServe(t *testing.T, h http.Handler, method, path, accessKey, secret, token string) s3Reply {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if accessKey != "" {
		const emptySHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		req.Header.Set("X-Amz-Content-Sha256", emptySHA)
		signer := v4.NewSigner(func(so *v4.SignerOptions) { so.DisableURIPathEscaping = true })
		require.NoError(t, signer.SignHTTP(context.Background(),
			aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secret, SessionToken: token},
			req, emptySHA, "s3", sessionRegion, time.Now().UTC()))
	}
	return record(t, h, req)
}

func record(t *testing.T, h http.Handler, req *http.Request) s3Reply {
	t.Helper()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	out := s3Reply{Status: rr.Code}
	if rr.Code >= 300 && rr.Body.Len() > 0 {
		require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &out), "body: %s", rr.Body.String())
	}
	return out
}

const (
	msgUnknownKey  = "The AWS Access Key Id you provided does not exist in our records"
	msgSigMismatch = "The request signature we calculated does not match the signature you provided. " +
		"Check your key and signing method."
	msgLookupFault = "An internal error occurred while validating credentials"
)

var (
	replyOK          = s3Reply{Status: http.StatusOK}
	replyUnknownKey  = s3Reply{Status: http.StatusForbidden, Code: "InvalidAccessKeyId", Message: msgUnknownKey}
	replySigMismatch = s3Reply{Status: http.StatusForbidden, Code: "SignatureDoesNotMatch", Message: msgSigMismatch}
	replyLookupFault = s3Reply{Status: http.StatusInternalServerError, Code: "InternalError", Message: msgLookupFault}
	replyDenied      = s3Reply{Status: http.StatusForbidden, Code: "AccessDenied", Message: "Access Denied"}
)

func headPrivate(t *testing.T, h http.Handler, akid, secret string) s3Reply {
	t.Helper()
	return charServe(t, h, http.MethodHead, "/"+charPrivateBucket, akid, secret, "")
}

// Pins CURRENT behaviour: an active long-lived IAM key with an S3 grant passes
// through header auth and through a presigned URL alike.
func TestCurrentBehaviour_PredastoreLongLived_ValidKey(t *testing.T) {
	f := auth.NewCharFixture(t, allowAllS3)
	h := charGate(t, f.Provider)

	assert.Equal(t, replyOK, headPrivate(t, h, auth.CharAKID, auth.CharSecret))

	req := httptest.NewRequest(http.MethodHead, "/"+charPrivateBucket+"?X-Amz-Expires=300", nil)
	signer := v4.NewSigner(func(so *v4.SignerOptions) { so.DisableURIPathEscaping = true })
	url, _, err := signer.PresignHTTP(context.Background(),
		aws.Credentials{AccessKeyID: auth.CharAKID, SecretAccessKey: auth.CharSecret},
		req, "UNSIGNED-PAYLOAD", "s3", sessionRegion, time.Now().UTC())
	require.NoError(t, err)
	assert.Equal(t, replyOK, record(t, h, httptest.NewRequest(http.MethodHead, url, nil)))
}

// CURRENT: Predastore never opens the IAM accounts bucket, so an account
// record marked SUSPENDED or TERMINATING still serves a valid IAM key.
func TestCurrentBehaviour_PredastoreAccountStatusIsNotConsulted(t *testing.T) {
	for _, status := range []string{"SUSPENDED", "TERMINATING"} {
		t.Run(status, func(t *testing.T) {
			f := auth.NewCharFixture(t, allowAllS3)
			f.SetAccountStatus(status)

			assert.Equal(t, replyOK, headPrivate(t, charGate(t, f.Provider), auth.CharAKID, auth.CharSecret))
		})
	}
}

// Pins CURRENT behaviour: how each invalid long-lived credential is reported,
// and so which failures a caller can tell apart (anti-enumeration). The order
// of checks is parse -> key lookup (status, account, decrypt, user, policies)
// -> signature verify -> policy -> ownership, so a lookup failure masks a bad
// signature. Malformed-envelope codes are pinned by TestSigV4AuthMiddleware.
func TestCurrentBehaviour_PredastoreLongLived_InvalidCredentials(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(f *auth.CharFixture)
		akid   string
		secret string
		want   s3Reply
	}{
		{name: "unknown key", akid: "AKIAUNKNOWNKEY000001", secret: auth.CharSecret, want: replyUnknownKey},
		// Lookup precedes verify: an unknown key never reaches signature checking.
		{name: "unknown key with a bad signature", akid: "AKIAUNKNOWNKEY000001", secret: "wrong", want: replyUnknownKey},
		// No prefix dispatch for long-lived keys: any non-ASIA ID goes to the AKIA bucket.
		{name: "unrecognised prefix", akid: "ZZZZUNKNOWNKEY000001", secret: auth.CharSecret, want: replyUnknownKey},
		{name: "known key, bad signature", akid: auth.CharAKID, secret: "wrong", want: replySigMismatch},
		// CURRENT: an Inactive key is a 500 InternalError, not 403 InvalidAccessKeyId
		// (AWS S3 answers 403 InvalidAccessKeyId for a deactivated key; unverified).
		// It is distinguishable from an unknown key, and SDKs retry the 500.
		{
			name:   "inactive key",
			mutate: func(f *auth.CharFixture) { f.SetKeyStatus("Inactive") },
			akid:   auth.CharAKID, secret: auth.CharSecret, want: replyLookupFault,
		},
		// CURRENT: status is checked before the signature, so a caller holding no
		// secret learns the key exists and is inactive.
		{
			name:   "inactive key with a bad signature",
			mutate: func(f *auth.CharFixture) { f.SetKeyStatus("Inactive") },
			akid:   auth.CharAKID, secret: "wrong", want: replyLookupFault,
		},
		{
			name:   "key record with empty account_id",
			mutate: func(f *auth.CharFixture) { f.SetKeyAccount("") },
			akid:   auth.CharAKID, secret: auth.CharSecret, want: replyLookupFault,
		},
		// CURRENT: a secret sealed under a different master key is a 500, where the
		// Spinifex gateway and Goanna report it as an invalid credential.
		{
			name:   "secret undecryptable with this master key",
			mutate: func(f *auth.CharFixture) { f.ResealKeyUnderForeignMasterKey() },
			akid:   auth.CharAKID, secret: auth.CharSecret, want: replyLookupFault,
		},
		// CURRENT: an orphaned key whose user record is gone is a 500, where the
		// session path maps the same stale principal to 403 InvalidAccessKeyId.
		{
			name:   "key whose IAM user record is deleted",
			mutate: func(f *auth.CharFixture) { f.DeleteUser() },
			akid:   auth.CharAKID, secret: auth.CharSecret, want: replyLookupFault,
		},
		// CURRENT: a user attaching a managed policy that no longer exists is a
		// 500 on the AKIA path (403 on the session path).
		{
			name:   "user attaches a deleted managed policy",
			mutate: func(f *auth.CharFixture) { f.DeletePolicy() },
			akid:   auth.CharAKID, secret: auth.CharSecret, want: replyLookupFault,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := auth.NewCharFixture(t, allowAllS3)
			if tt.mutate != nil {
				tt.mutate(f)
			}
			assert.Equal(t, tt.want, headPrivate(t, charGate(t, f.Provider), tt.akid, tt.secret))
		})
	}
}

// Pins CURRENT behaviour: STS session credentials (ASIA) are authenticated by
// SigV4 over the decrypted session secret plus an expiry check only.
func TestCurrentBehaviour_PredastoreSession_TokenAndExpiry(t *testing.T) {
	k := auth.LoadTestKey(t)
	const secret = "session-secret-value"

	live := func(t *testing.T) http.Handler {
		sessions, users, policies := auth.UserSessionFixture(t, k, secret, time.Now().UTC().Add(time.Hour))
		return charGate(t, auth.NewSessionProvider(k, sessions, users, nil, policies))
	}

	// CURRENT: X-Amz-Security-Token is never read. A session key and secret
	// with no token authenticate. AWS rejects this (403; InvalidAccessKeyId or
	// InvalidToken, exact code unverified); the Spinifex gateway verifies the
	// token against the record's session_token_hmac.
	t.Run("valid session, no security token", func(t *testing.T) {
		assert.Equal(t, replyOK,
			charServe(t, live(t), http.MethodHead, "/"+charPrivateBucket, auth.TestSessionAKID, secret, ""))
	})

	// CURRENT: any token value is accepted, because it is never compared.
	t.Run("valid session, forged security token", func(t *testing.T) {
		assert.Equal(t, replyOK,
			charServe(t, live(t), http.MethodHead, "/"+charPrivateBucket, auth.TestSessionAKID, secret, "forged-token"))
	})

	t.Run("valid session, bad signature", func(t *testing.T) {
		assert.Equal(t, replySigMismatch,
			charServe(t, live(t), http.MethodHead, "/"+charPrivateBucket, auth.TestSessionAKID, "wrong", "t"))
	})

	t.Run("unknown session key", func(t *testing.T) {
		assert.Equal(t, replyUnknownKey,
			charServe(t, live(t), http.MethodHead, "/"+charPrivateBucket, "ASIAUNKNOWNSESSION01", secret, "t"))
	})

	// CURRENT: an expired session reads as an unknown key. AWS S3 answers
	// 400 ExpiredToken (unverified); Goanna answers ExpiredToken.
	t.Run("expired session", func(t *testing.T) {
		sessions, users, policies := auth.UserSessionFixture(t, k, secret, time.Now().UTC().Add(-time.Minute))
		h := charGate(t, auth.NewSessionProvider(k, sessions, users, nil, policies))
		assert.Equal(t, replyUnknownKey,
			charServe(t, h, http.MethodHead, "/"+charPrivateBucket, auth.TestSessionAKID, secret, "t"))
	})

	// A session whose IAM user has since been deleted reads as an unknown key.
	t.Run("session principal deleted", func(t *testing.T) {
		sessions, _, policies := auth.UserSessionFixture(t, k, secret, time.Now().UTC().Add(time.Hour))
		h := charGate(t, auth.NewSessionProvider(k, sessions, map[string][]byte{}, nil, policies))
		assert.Equal(t, replyUnknownKey,
			charServe(t, h, http.MethodHead, "/"+charPrivateBucket, auth.TestSessionAKID, secret, "t"))
	})
}

// Pins CURRENT behaviour: long-lived credentials are cached for CacheTTL and
// revoked early only by a KV watcher event.
func TestCurrentBehaviour_PredastoreRevocation_KeyDeleted(t *testing.T) {
	assert.Equal(t, 60*time.Second, auth.CacheTTL)

	t.Run("watcher delivers the delete", func(t *testing.T) {
		f := auth.NewCharFixture(t, allowAllS3)
		h := charGate(t, f.Provider)
		require.Equal(t, replyOK, headPrivate(t, h, auth.CharAKID, auth.CharSecret))

		f.DeleteKey(true)
		require.Eventually(t, func() bool { return f.CachedEntries() == 0 }, 2*time.Second, 5*time.Millisecond)
		assert.Equal(t, replyUnknownKey, headPrivate(t, h, auth.CharAKID, auth.CharSecret))
	})

	// CURRENT: with no watcher event the deleted key keeps authenticating from
	// cache until the 60s TTL lapses. AWS documents IAM changes as eventually
	// consistent, with no stated bound.
	t.Run("watcher misses the delete", func(t *testing.T) {
		f := auth.NewCharFixture(t, allowAllS3)
		h := charGate(t, f.Provider)
		require.Equal(t, replyOK, headPrivate(t, h, auth.CharAKID, auth.CharSecret))

		f.DeleteKey(false)
		assert.Equal(t, replyOK, headPrivate(t, h, auth.CharAKID, auth.CharSecret))

		f.ExpireCache()
		assert.Equal(t, replyUnknownKey, headPrivate(t, h, auth.CharAKID, auth.CharSecret))
	})
}

// Pins CURRENT behaviour: an access key binds to its user by name, not by the
// user's unique ID, so a same-named replacement user inherits the old key.
func TestCurrentBehaviour_PredastoreRevocation_UserReplaced(t *testing.T) {
	// Spinifex refuses DeleteUser while keys exist (DeleteConflict, as AWS), so
	// this state needs an out-of-band or racing delete; the consumer is pinned.
	t.Run("replacement user has no policies", func(t *testing.T) {
		f := auth.NewCharFixture(t, allowAllS3)
		h := charGate(t, f.Provider)
		require.Equal(t, replyOK, headPrivate(t, h, auth.CharAKID, auth.CharSecret))

		f.RecreateUser("AIDACAROLREPLACED01")
		require.Eventually(t, func() bool { return f.CachedEntries() == 0 }, 2*time.Second, 5*time.Millisecond)
		assert.Equal(t, replyDenied, headPrivate(t, h, auth.CharAKID, auth.CharSecret))
	})

	// CURRENT: the old key authenticates as the new user and receives the new
	// user's grants. In AWS a recreated user is a new principal (new unique ID)
	// and no key of the old user can exist.
	t.Run("replacement user has a grant", func(t *testing.T) {
		f := auth.NewCharFixture(t, allowAllS3)
		h := charGate(t, f.Provider)
		require.Equal(t, replyOK, headPrivate(t, h, auth.CharAKID, auth.CharSecret))

		f.DeleteUser()
		require.Eventually(t, func() bool { return f.CachedEntries() == 0 }, 2*time.Second, 5*time.Millisecond)
		require.Equal(t, replyLookupFault, headPrivate(t, h, auth.CharAKID, auth.CharSecret))

		f.RecreateUser("AIDACAROLREPLACED01", auth.CharPolicyARN)
		assert.Equal(t, replyOK, headPrivate(t, h, auth.CharAKID, auth.CharSecret))
	})

	// Session credentials are never cached: deleting the record revokes the
	// very next request, with no watcher involved.
	t.Run("session record deleted", func(t *testing.T) {
		k := auth.LoadTestKey(t)
		const secret = "session-secret-value"
		sessions, users, policies := auth.UserSessionFixture(t, k, secret, time.Now().UTC().Add(time.Hour))
		h := charGate(t, auth.NewSessionProvider(k, sessions, users, nil, policies))
		require.Equal(t, replyOK,
			charServe(t, h, http.MethodHead, "/"+charPrivateBucket, auth.TestSessionAKID, secret, ""))

		delete(sessions, auth.TestSessionAKID)
		assert.Equal(t, replyUnknownKey,
			charServe(t, h, http.MethodHead, "/"+charPrivateBucket, auth.TestSessionAKID, secret, ""))
	})
}

// Pins CURRENT behaviour: unsigned requests are served only as public-bucket
// reads; no IAM state is consulted.
func TestCurrentBehaviour_PredastoreAnonymous_Access(t *testing.T) {
	h := charGate(t, auth.NewBootstrappingIAMProvider(t))

	assert.Equal(t, replyDenied, charServe(t, h, http.MethodHead, "/"+charPrivateBucket, "", "", ""))
	assert.Equal(t, replyDenied, charServe(t, h, http.MethodGet, "/", "", "", ""))
	assert.Equal(t, replyOK, charServe(t, h, http.MethodHead, "/"+charPublicBucket, "", "", ""))
	assert.Equal(t, replyDenied, charServe(t, h, http.MethodPut, "/"+charPublicBucket+"/obj", "", "", ""))
	// A signed request to a public bucket is judged on its credential and never
	// falls back to anonymous access.
	assert.Equal(t, replyUnknownKey,
		charServe(t, h, http.MethodHead, "/"+charPublicBucket, "AKIAUNKNOWNKEY000001", "s", ""))
}

// Pins CURRENT behaviour: what a request gets when IAM state cannot be read.
func TestCurrentBehaviour_PredastoreUnavailable_BackingState(t *testing.T) {
	t.Run("NATS unreachable, buckets never opened", func(t *testing.T) {
		h := charGate(t, auth.NewUnreachableIAMProvider(t))
		assert.Equal(t, replyLookupFault, headPrivate(t, h, auth.CharAKID, auth.CharSecret))
		assert.Equal(t, replyLookupFault, headPrivate(t, h, auth.TestSessionAKID, auth.CharSecret))
	})

	// The bootstrap window (Spinifex has not created the buckets) is reported as
	// an unknown key, not an outage.
	t.Run("IAM buckets not yet created", func(t *testing.T) {
		h := charGate(t, auth.NewBootstrappingIAMProvider(t))
		assert.Equal(t, replyUnknownKey, headPrivate(t, h, auth.CharAKID, auth.CharSecret))
		assert.Equal(t, replyUnknownKey, headPrivate(t, h, auth.TestSessionAKID, auth.CharSecret))
	})

	for _, bucket := range []string{"keys", "users", "policies"} {
		t.Run("KV fault on "+bucket+", uncached key", func(t *testing.T) {
			f := auth.NewCharFixture(t, allowAllS3)
			f.FailBucket(bucket, auth.ErrKVUnavailable)
			assert.Equal(t, replyLookupFault, headPrivate(t, charGate(t, f.Provider), auth.CharAKID, auth.CharSecret))
		})
	}

	t.Run("KV fault on sessions", func(t *testing.T) {
		f := auth.NewCharFixture(t, allowAllS3)
		f.FailBucket("sessions", auth.ErrKVUnavailable)
		assert.Equal(t, replyLookupFault, headPrivate(t, charGate(t, f.Provider), auth.TestSessionAKID, auth.CharSecret))
	})

	// CURRENT: a cached long-lived credential keeps authenticating through an
	// IAM store outage for up to CacheTTL; no freshness bound is enforced.
	t.Run("KV fault after the key is cached", func(t *testing.T) {
		f := auth.NewCharFixture(t, allowAllS3)
		h := charGate(t, f.Provider)
		require.Equal(t, replyOK, headPrivate(t, h, auth.CharAKID, auth.CharSecret))

		for _, bucket := range []string{"keys", "users", "policies"} {
			f.FailBucket(bucket, auth.ErrKVUnavailable)
		}
		assert.Equal(t, replyOK, headPrivate(t, h, auth.CharAKID, auth.CharSecret))

		f.ExpireCache()
		assert.Equal(t, replyLookupFault, headPrivate(t, h, auth.CharAKID, auth.CharSecret))
	})

	// An unreadable IAM master key stops the gate from starting at all.
	t.Run("master key unreadable at startup", func(t *testing.T) {
		cert, key, _ := testcerts.Generate(t)
		_, err := gate.New(gate.Config{
			Region: sessionRegion, NodeID: 1, TLSCert: cert, TLSKey: key,
			Meta: noMeta{}, Blob: noBlob{},
			IAM: &auth.IAMConfig{NATSUrl: "nats://127.0.0.1:1", MasterKeyPath: t.TempDir() + "/absent.key"},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "load master key")
	})
}

// Pins CURRENT behaviour: identity-policy evaluation for a long-lived key read
// from IAM state. There is no bucket policy to combine with: GET ?policy
// answers NoSuchBucketPolicy (TestBucketControlReadAnswersTheUnsetCode) and
// cross-account access is denied whatever the identity policy grants
// (TestOwnership_CrossAccountDenied). Role-session allow/deny is pinned by
// TestSigV4Middleware_AssumedRole; deny-wins across user/group/inline sources
// by TestResolveUserPolicies_*.
func TestCurrentBehaviour_PredastorePolicy_IdentityEvaluation(t *testing.T) {
	tests := []struct {
		name   string
		policy string
		want   s3Reply
	}{
		{"explicit allow", allowAllS3, replyOK},
		{"implicit deny (no matching statement)", allowGetOnly, replyDenied},
		{"explicit deny beats allow", allowThenDenyHead, replyDenied},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := auth.NewCharFixture(t, tt.policy)
			assert.Equal(t, tt.want, headPrivate(t, charGate(t, f.Provider), auth.CharAKID, auth.CharSecret))
		})
	}
}

// Pins CURRENT behaviour: a config-defined [[auth]] account wins over IAM for
// the same access key ID and skips every IAM and ownership check.
func TestCurrentBehaviour_PredastoreBootstrap_ConfigShadowsIAM(t *testing.T) {
	f := auth.NewCharFixture(t, allowAllS3)
	f.SetKeyStatus("Inactive")
	config := auth.NewConfigProvider([]auth.Entry{{
		AccessKeyID: auth.CharAKID, SecretAccessKey: auth.CharSecret, AccountID: "999999999999",
	}})
	h := charGate(t, auth.NewChainProvider(f.Provider, config))

	// CURRENT: deactivating the key in IAM does not revoke it here, and a
	// foreign-account config entry still reaches a bucket it does not own.
	assert.Equal(t, replyOK, headPrivate(t, h, auth.CharAKID, auth.CharSecret))
}

// CURRENT: a configured [[auth]] credential also bypasses an IAM account
// termination record, as it bypasses every other IAM read and policy check.
func TestCurrentBehaviour_PredastoreBootstrap_ConfigIgnoresAccountStatus(t *testing.T) {
	f := auth.NewCharFixture(t, allowAllS3)
	f.SetAccountStatus("TERMINATING")
	// IAM can no longer admit this key, so only the config entry can.
	f.SetKeyStatus("Inactive")
	config := auth.NewConfigProvider([]auth.Entry{{
		AccessKeyID: auth.CharAKID, SecretAccessKey: auth.CharSecret, AccountID: auth.TestSessionAccount,
	}})

	assert.Equal(t, replyOK, headPrivate(t, charGate(t, auth.NewChainProvider(f.Provider, config)), auth.CharAKID, auth.CharSecret))
}
