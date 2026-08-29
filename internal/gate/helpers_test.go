package gate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/stretchr/testify/require"
)

// resolveRouter mirrors setupRoutes' group structure, terminating every route
// shape in next rather than a real handler. Middleware tests must route through
// chi, because the resolvers read the URL parameters a match produces.
func resolveRouter(next http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.StripSlashes)

	r.Group(func(r chi.Router) {
		r.Use(mws...)
		r.Method(http.MethodGet, "/", next)
	})
	r.Group(func(r chi.Router) {
		r.Use(append([]func(http.Handler) http.Handler{resolveBucket}, mws...)...)
		r.Handle("/{bucket}", next)
	})
	r.Group(func(r chi.Router) {
		r.Use(append([]func(http.Handler) http.Handler{resolveObject}, mws...)...)
		r.Handle("/{bucket}/*", next)
	})
	return r
}

// signOpts configures signTestReq.
type signOpts struct{ signingTime time.Time }

// withSignTime overrides the signing time, used to forge clock skew. The
// server derives "now" itself, so a skewed signing time exercises its
// timestamp validation.
func withSignTime(t time.Time) func(*signOpts) {
	return func(o *signOpts) { o.signingTime = t }
}

// signTestReq signs req with the given credentials and body payload hash via
// the AWS SDK SigV4 signer. body may be nil for body-less requests
// (sha256.Sum256(nil) is the empty SHA-256 the server expects). Fails the
// test on signer error.
func signTestReq(t *testing.T, req *http.Request, body []byte,
	accessKey, secret, region, service string, opts ...func(*signOpts)) {
	t.Helper()
	o := signOpts{signingTime: time.Now().UTC()}
	for _, fn := range opts {
		fn(&o)
	}

	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])
	// The server recovers the signed payload hash from this header; the SDK doesn't set it.
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	signer := v4.NewSigner(func(so *v4.SignerOptions) {
		so.DisableURIPathEscaping = service == "s3"
	})
	require.NoError(t, signer.SignHTTP(context.Background(),
		aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secret},
		req, payloadHash, service, region, o.signingTime))
}

// fakeBlob stands in for the shard client. New requires one, but no test that
// takes this reaches a route that reads or writes a shard.
type fakeBlob struct{}

func (fakeBlob) Put(context.Context, config.NodeID, blob.PutRequest, io.Reader) (*blob.PutResponse, error) {
	return nil, errors.New("no blob nodes")
}

func (fakeBlob) Get(context.Context, config.NodeID, blob.GetRequest) (io.ReadCloser, error) {
	return nil, errors.New("no blob nodes")
}

func (fakeBlob) Delete(context.Context, config.NodeID, blob.DeleteRequest) (*blob.DeleteResponse, error) {
	return nil, errors.New("no blob nodes")
}

func (fakeBlob) Stat(context.Context, config.NodeID, blob.StatRequest) (*blob.StatResponse, error) {
	return nil, errors.New("no blob nodes")
}

func (fakeBlob) Commit(context.Context, config.NodeID, blob.CommitRequest) error {
	return errors.New("no blob nodes")
}

func (fakeBlob) Abort(context.Context, config.NodeID, blob.CommitRequest) error {
	return errors.New("no blob nodes")
}

// newTestGate builds a gate through the production constructor, filling in
// what New requires and a test rarely cares about: a loadable TLS pair and the
// two cluster clients. Whatever cfg already sets is left alone.
func newTestGate(t *testing.T, cfg Config) *Server {
	t.Helper()
	if cfg.TLSCert == "" {
		cfg.TLSCert, cfg.TLSKey, _ = testcerts.Generate(t)
	}
	if cfg.Meta == nil {
		cfg.Meta = newFakeMeta(t)
	}
	if cfg.Blob == nil {
		cfg.Blob = fakeBlob{}
	}
	// Every gate needs an id: it goes into the write epoch, and zero is not one.
	if cfg.NodeID == 0 {
		cfg.NodeID = 1
	}
	s, err := New(cfg)
	require.NoError(t, err)
	return s
}

// newAuthTestConfig returns an inline Config for tests that only exercise
// auth middleware (no real backend needed). Avoids reading any TOML file.
func newAuthTestConfig() Config {
	return Config{
		Region: "ap-southeast-2",
		Buckets: []handlers.BucketConfig{
			{Name: "test-bucket01", Region: "ap-southeast-2", Public: true},
			{Name: "private", Region: "ap-southeast-2", Public: false},
			{Name: "local", Region: "ap-southeast-2", Public: false},
		},
		Auth: []auth.Entry{
			{
				AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
				SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				Policy: []auth.PolicyRule{
					{Bucket: "private", Actions: []string{"s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListAllMyBuckets"}},
					{Bucket: "local", Actions: []string{"s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListAllMyBuckets"}},
				},
			},
		},
	}
}
