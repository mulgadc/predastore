package auth_test

// ASIA-signed requests driven end to end through the gateway's SigV4
// middleware against a real NATSIAMProvider over fake KV buckets. The test
// lives beside the provider because the session fixtures are its own; it is an
// external test package so it may import the gateway that imports auth.
//
// HEAD /{bucket} is the probe: it resolves entirely from the config-defined
// bucket cache, so an authorized request reaches the handler and returns 200
// without a state cluster behind it.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/mulgadc/predastore/internal/gateway"
	"github.com/mulgadc/predastore/internal/gateway/auth"
	"github.com/mulgadc/predastore/internal/gateway/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sessionRegion = "ap-southeast-2"

// sessionGateway builds a gateway whose only bucket is config-defined and owned
// by the session's account, so authorization is the only thing under test.
func sessionGateway(p auth.CredentialProvider) http.Handler {
	cfg := &gateway.Config{
		Region: sessionRegion,
		Buckets: []handlers.BucketConfig{{
			Name: "session-bucket", Region: sessionRegion, Type: "distributed",
			Public: false, AccountID: auth.TestSessionAccount,
		}},
	}
	return gateway.NewHandler(cfg, gateway.Clients{}, p)
}

// signSession signs a body-less request with the AWS SDK's SigV4 signer, as a
// client holding STS session credentials would.
func signSession(t *testing.T, req *http.Request, accessKey, secret string) {
	t.Helper()
	sum := sha256.Sum256(nil)
	payloadHash := hex.EncodeToString(sum[:])
	// The server recovers the signed payload hash from this header; the SDK doesn't set it.
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	signer := v4.NewSigner(func(so *v4.SignerOptions) { so.DisableURIPathEscaping = true })
	require.NoError(t, signer.SignHTTP(context.Background(),
		aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secret},
		req, payloadHash, "s3", sessionRegion, time.Now().UTC()))
}

// headBucket signs and serves HEAD /session-bucket, returning the status code.
func headBucket(t *testing.T, h http.Handler, accessKey, secret string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodHead, "/session-bucket", nil)
	signSession(t, req, accessKey, secret)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr.Code
}

func TestSigV4Middleware_SessionCredential(t *testing.T) {
	k := auth.LoadTestKey(t)
	const secret = "session-secret-value"
	sessions, users, policies := auth.UserSessionFixture(t, k, secret, time.Now().UTC().Add(time.Hour))
	h := sessionGateway(auth.NewSessionProvider(k, sessions, users, nil, policies))

	t.Run("valid session passes auth", func(t *testing.T) {
		assert.Equal(t, http.StatusOK, headBucket(t, h, auth.TestSessionAKID, secret),
			"valid ASIA session signature must pass through to the handler")
	})

	t.Run("unknown session key rejected", func(t *testing.T) {
		assert.Equal(t, http.StatusForbidden, headBucket(t, h, "ASIAUNKNOWNKEY000000", secret))
	})
}

func TestSigV4Middleware_SessionExpired(t *testing.T) {
	k := auth.LoadTestKey(t)
	const secret = "session-secret-value"
	sessions, users, policies := auth.UserSessionFixture(t, k, secret, time.Now().UTC().Add(-time.Minute))
	h := sessionGateway(auth.NewSessionProvider(k, sessions, users, nil, policies))

	assert.Equal(t, http.StatusForbidden, headBucket(t, h, auth.TestSessionAKID, secret),
		"expired session must not reach the handler")
}

// TestSigV4Middleware_AssumedRole exercises the IMDS → S3 path end to end: an
// ASIA-signed request for an assumed-role session is authorized iff the
// underlying role's managed policy permits the action on the resource.
func TestSigV4Middleware_AssumedRole(t *testing.T) {
	k := auth.LoadTestKey(t)
	const secret = "role-secret-value"

	t.Run("role policy permits → 200", func(t *testing.T) {
		roles, policies := auth.RoleWithPolicy(t, "S3FullAccess", auth.AllowAllS3Policy)
		sessions := auth.AssumedRoleSession(t, k, secret, auth.TestSessionRoleARN, time.Now().UTC().Add(time.Hour))
		h := sessionGateway(auth.NewSessionProvider(k, sessions, nil, roles, policies))

		assert.Equal(t, http.StatusOK, headBucket(t, h, auth.TestSessionAKID, secret),
			"an allowed assumed-role request must pass through to the handler")
	})

	t.Run("role policy denies → 403", func(t *testing.T) {
		roles, policies := auth.RoleWithPolicy(t, "S3Deny", auth.DenyAllS3Policy)
		sessions := auth.AssumedRoleSession(t, k, secret, auth.TestSessionRoleARN, time.Now().UTC().Add(time.Hour))
		h := sessionGateway(auth.NewSessionProvider(k, sessions, nil, roles, policies))

		assert.Equal(t, http.StatusForbidden, headBucket(t, h, auth.TestSessionAKID, secret),
			"a denied assumed-role request must not reach the handler")
	})
}
