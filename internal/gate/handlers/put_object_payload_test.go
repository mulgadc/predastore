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
func verifiedPut(tb testing.TB, signedBody, sentBody []byte) *http.Request {
	tb.Helper()

	sum := sha256.Sum256(signedBody)

	return signedPut(tb, sentBody, hex.EncodeToString(sum[:]))
}

// signedPut returns a PUT of sentBody that has passed SigV4 verification with payloadHash as
// x-amz-content-sha256, which is a digest to bind the body or a sentinel to leave it unbound.
func signedPut(tb testing.TB, sentBody []byte, payloadHash string) *http.Request {
	tb.Helper()

	req := signPut(tb, sentBody, payloadHash)
	require.NoError(tb, verifyPut(req))

	return req
}

// signPut returns a signed but unverified PUT, so a caller can time the verification.
func signPut(tb testing.TB, sentBody []byte, payloadHash string) *http.Request {
	tb.Helper()

	req, err := http.NewRequest(http.MethodPut, "https://bucket.example.com/object.txt", bytes.NewReader(sentBody))
	require.NoError(tb, err)
	req.ContentLength = int64(len(sentBody))
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	creds := aws.Credentials{AccessKeyID: payloadTestKey, SecretAccessKey: payloadTestSecret}
	require.NoError(tb, signer.SignHTTP(context.Background(), creds, req, payloadHash, "s3", "ap-southeast-2", time.Now().UTC()))

	return req
}

// verifyPut runs the gate's SigV4 check over req, binding its body when the signed
// x-amz-content-sha256 is a digest.
func verifyPut(req *http.Request) error {
	signed, err := sigv4.Parse(req)
	if err != nil {
		return err
	}

	_, err = signed.Verify(payloadTestSecret, "ap-southeast-2", "s3")

	return err
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

		require.NoError(t, finishPayload(req, nil))
	})

	t.Run("rewritten body", func(t *testing.T) {
		req := verifiedPut(t, large, bytes.Repeat([]byte("b"), len(large)))

		// The declared length ends the body, so the write path's own read fails on the
		// last byte: the rewrite is caught before the drain rather than by it.
		_, err := io.CopyN(io.Discard, req.Body, int64(len(large)))
		require.ErrorIs(t, err, sigv4.ErrContentSHA256Mismatch)
		require.ErrorIs(t, mapPutErr(err), model.ErrContentSHA256MismatchError)

		require.ErrorIs(t, finishPayload(req, nil), model.ErrContentSHA256MismatchError)
	})

	t.Run("body already at EOF", func(t *testing.T) {
		// A small body is verified during Verify, so nothing is left to check here.
		req := verifiedPut(t, []byte("hello"), []byte("hello"))
		_, err := io.ReadAll(req.Body)
		require.NoError(t, err)

		require.NoError(t, finishPayload(req, nil))
	})
}
