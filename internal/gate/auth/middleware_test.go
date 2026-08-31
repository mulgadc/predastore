package auth_test

// ASIA-signed requests driven end to end through the gate's SigV4
// middleware against a real NATSIAMProvider over fake KV buckets. The test
// lives beside the provider because the session fixtures are its own; it is an
// external test package so it may import the gate that imports auth.
//
// HEAD /{bucket} is the probe: it resolves entirely from the config-defined
// bucket cache, so an authorized request reaches the handler and returns 200
// without a state cluster behind it.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sessionRegion = "ap-southeast-2"

// errNoCluster is what the stand-in clients below return. gate.New requires
// both, but a config-defined bucket resolves from the gate's own cache, so a
// HEAD against one never reaches state or a shard.
var errNoCluster = errors.New("no cluster")

type noMeta struct{}

func (noMeta) Get(context.Context, string) ([]byte, error)          { return nil, errNoCluster }
func (noMeta) Put(context.Context, string, []byte) error            { return errNoCluster }
func (noMeta) PutMax(context.Context, string, []byte, uint64) error { return errNoCluster }
func (noMeta) Delete(context.Context, string) error                 { return errNoCluster }
func (noMeta) ScanFrom(context.Context, string, string, int) ([]meta.Item, error) {
	return nil, errNoCluster
}
func (noMeta) Scan(context.Context, string, int) ([]meta.Item, error) {
	return nil, errNoCluster
}

type noBlob struct{}

func (noBlob) Put(context.Context, config.NodeID, blob.PutRequest, io.Reader) (*blob.PutResponse, error) {
	return nil, errNoCluster
}

func (noBlob) Get(context.Context, config.NodeID, blob.GetRequest) (io.ReadCloser, error) {
	return nil, errNoCluster
}

func (noBlob) Delete(context.Context, config.NodeID, blob.DeleteRequest) (*blob.DeleteResponse, error) {
	return nil, errNoCluster
}

func (noBlob) Stat(context.Context, config.NodeID, blob.StatRequest) (*blob.StatResponse, error) {
	return nil, errNoCluster
}

func (noBlob) Commit(context.Context, config.NodeID, blob.CommitRequest) error { return errNoCluster }
func (noBlob) Abort(context.Context, config.NodeID, blob.CommitRequest) error  { return errNoCluster }

// sessionGate builds a gate whose only bucket is config-defined and owned
// by the session's account, so authorization is the only thing under test.
func sessionGate(t *testing.T, p auth.CredentialProvider) http.Handler {
	t.Helper()
	cert, key, _ := testcerts.Generate(t)
	s, err := gate.New(gate.Config{
		Region: sessionRegion,
		NodeID: 1,
		Buckets: []handlers.BucketConfig{{
			Name: "session-bucket", Region: sessionRegion,
			Public: false, AccountID: auth.TestSessionAccount,
		}},
		TLSCert:  cert,
		TLSKey:   key,
		Meta:     noMeta{},
		Blob:     noBlob{},
		CredProv: p,
	})
	require.NoError(t, err)
	return s
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
	h := sessionGate(t, auth.NewSessionProvider(k, sessions, users, nil, policies))

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
	h := sessionGate(t, auth.NewSessionProvider(k, sessions, users, nil, policies))

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
		h := sessionGate(t, auth.NewSessionProvider(k, sessions, nil, roles, policies))

		assert.Equal(t, http.StatusOK, headBucket(t, h, auth.TestSessionAKID, secret),
			"an allowed assumed-role request must pass through to the handler")
	})

	t.Run("role policy denies → 403", func(t *testing.T) {
		roles, policies := auth.RoleWithPolicy(t, "S3Deny", auth.DenyAllS3Policy)
		sessions := auth.AssumedRoleSession(t, k, secret, auth.TestSessionRoleARN, time.Now().UTC().Add(time.Hour))
		h := sessionGate(t, auth.NewSessionProvider(k, sessions, nil, roles, policies))

		assert.Equal(t, http.StatusForbidden, headBucket(t, h, auth.TestSessionAKID, secret),
			"a denied assumed-role request must not reach the handler")
	})
}
