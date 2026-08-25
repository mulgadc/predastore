package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/require"
)

const (
	payloadTestKey    = "AKIAIOSFODNN7EXAMPLE"
	payloadTestSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

// verifiedPut returns a PUT whose body has passed SigV4 verification, signed over the digest
// of signedBody while carrying sentBody — the on-path rewrite a payload check must catch.
func verifiedPut(t *testing.T, signedBody, sentBody []byte) *http.Request {
	t.Helper()

	req, err := http.NewRequest(http.MethodPut, "https://bucket.example.com/object.txt", bytes.NewReader(sentBody))
	require.NoError(t, err)
	req.ContentLength = int64(len(sentBody))

	sum := sha256.Sum256(signedBody)
	payloadHash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	creds := aws.Credentials{AccessKeyID: payloadTestKey, SecretAccessKey: payloadTestSecret}
	require.NoError(t, signer.SignHTTP(context.Background(), creds, req, payloadHash, "s3", "ap-southeast-2", time.Now().UTC()))

	signed, err := sigv4.Parse(req)
	require.NoError(t, err)
	_, err = signed.Verify(payloadTestSecret, "ap-southeast-2", "s3")
	require.NoError(t, err)

	return req
}

// TestFinishPayload covers the write path's payload check for a body too large for sigv4 to
// buffer, which it verifies as the body streams instead of at Verify.
func TestFinishPayload(t *testing.T) {
	large := bytes.Repeat([]byte("a"), sigv4.MaxPayloadLen+1)

	t.Run("intact body", func(t *testing.T) {
		req := verifiedPut(t, large, large)

		// The write path reads exactly Content-Length and stops short of EOF.
		_, err := io.CopyN(io.Discard, req.Body, int64(len(large)))
		require.NoError(t, err)

		require.NoError(t, finishPayload(req))
	})

	t.Run("rewritten body", func(t *testing.T) {
		req := verifiedPut(t, large, bytes.Repeat([]byte("b"), len(large)))

		_, err := io.CopyN(io.Discard, req.Body, int64(len(large)))
		require.NoError(t, err)

		require.ErrorIs(t, finishPayload(req), model.ErrContentSHA256MismatchError)
	})

	t.Run("body already at EOF", func(t *testing.T) {
		// A small body is verified during Verify, so nothing is left to check here.
		req := verifiedPut(t, []byte("hello"), []byte("hello"))
		_, err := io.ReadAll(req.Body)
		require.NoError(t, err)

		require.NoError(t, finishPayload(req))
	})
}
