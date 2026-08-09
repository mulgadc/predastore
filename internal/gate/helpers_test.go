package gate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/stretchr/testify/require"
)

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

// newAuthTestConfig returns an inline Config for tests that only exercise
// auth middleware (no real backend needed). Avoids reading any TOML file.
func newAuthTestConfig() *Config {
	return &Config{
		Region: "ap-southeast-2",
		Buckets: []handlers.BucketConfig{
			{Name: "test-bucket01", Region: "ap-southeast-2", Type: "distributed", Public: true},
			{Name: "private", Region: "ap-southeast-2", Type: "distributed", Public: false},
			{Name: "local", Region: "ap-southeast-2", Type: "distributed", Public: false},
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
