package gate

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/gate/chunked"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deriveSigningKey is AWS's four-step key derivation, written from the spec so
// the chain the test signs with does not come from the code under test.
func deriveSigningKey(secret, date, region, service string) []byte {
	mac := func(key []byte, data string) []byte {
		h := hmac.New(sha256.New, key)
		h.Write([]byte(data))
		return h.Sum(nil)
	}
	k := mac([]byte("AWS4"+secret), date)
	k = mac(k, region)
	k = mac(k, service)
	return mac(k, "aws4_request")
}

// clientChunkedBody frames payload as one signed chunk plus the terminating
// chunk, the way a client signing STREAMING-AWS4-HMAC-SHA256-PAYLOAD does.
func clientChunkedBody(key []byte, seed, scope, timestamp, payload string) string {
	prev := seed
	sign := func(chunk string) string {
		sum := sha256.Sum256([]byte(chunk))
		empty := sha256.Sum256(nil)
		sts := strings.Join([]string{
			"AWS4-HMAC-SHA256-PAYLOAD", timestamp, scope, prev,
			hex.EncodeToString(empty[:]), hex.EncodeToString(sum[:]),
		}, "\n")
		h := hmac.New(sha256.New, key)
		h.Write([]byte(sts))
		prev = hex.EncodeToString(h.Sum(nil))
		return prev
	}

	var buf strings.Builder
	fmt.Fprintf(&buf, "%x;chunk-signature=%s\r\n%s\r\n", len(payload), sign(payload), payload)
	fmt.Fprintf(&buf, "0;chunk-signature=%s\r\n\r\n", sign(""))
	return buf.String()
}

// framedLen is the wire length of clientChunkedBody's output. Content-Length is
// signed, so it has to be known before the seed signature exists.
func framedLen(payload string) int {
	const sigExt = len(";chunk-signature=") + 64
	return len(fmt.Sprintf("%x", len(payload))) + sigExt + 2 + len(payload) + 2 +
		1 + sigExt + 2 + 2
}

// authSignature pulls Signature= out of the Authorization header, which is the
// seed of a client's chunk signature chain.
func authSignature(tb testing.TB, req *http.Request) string {
	tb.Helper()

	_, after, found := strings.Cut(req.Header.Get("Authorization"), "Signature=")
	require.True(tb, found, "signed request carries no Signature=")
	return strings.TrimSpace(after)
}

// TestSignedPayloadChainEndToEnd drives a signed chunked PUT through the auth
// middleware and continues its chain in the handler. The chain the client signs
// with is derived from the secret independently, so a wrong credential scope or
// timestamp format in the middleware fails here rather than passing against
// itself.
func TestSignedPayloadChainEndToEnd(t *testing.T) {
	const (
		payload = "hello world"
		region  = "ap-southeast-2"
		key     = "AKIAIOSFODNN7EXAMPLE"
		secret  = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	)

	now := time.Now().UTC()
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", now.Format("20060102"), region)
	signingKey := deriveSigningKey(secret, now.Format("20060102"), region, "s3")

	newSignedPut := func(t *testing.T, mode sigv4.ContentMode, tamper bool) *http.Request {
		t.Helper()

		// The signature covers Content-Length, so the framed body's length has to
		// be settled before its chunk signatures can exist.
		size := framedLen(payload)
		req := httptest.NewRequest(http.MethodPut, "https://s3.example.com/private/o.txt",
			strings.NewReader(strings.Repeat("x", size)))
		req.ContentLength = int64(size)
		req.Header.Set("X-Amz-Content-Sha256", string(mode))
		req.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(payload)))

		signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
		require.NoError(t, signer.SignHTTP(context.Background(),
			aws.Credentials{AccessKeyID: key, SecretAccessKey: secret},
			req, string(mode), "s3", region, now))

		body := clientChunkedBody(signingKey, authSignature(t, req), scope,
			now.Format(sigv4.AmzTimeFormat), payload)
		require.Len(t, body, size, "framedLen must match what the client actually sends")
		if tamper {
			body = strings.Replace(body, payload, strings.ToUpper(payload), 1)
		}
		req.Body = io.NopCloser(strings.NewReader(body))

		return req
	}

	// decodeResult is what the handler observed, collected rather than asserted
	// in place: a failed assertion inside a handler aborts the wrong goroutine.
	type decodeResult struct {
		payload handlers.SignedPayload
		body    string
		err     error
		reached bool
	}

	// decodeAt continues the chain the middleware seeded, which is what the
	// write path does, and records what came out of the body.
	decodeAt := func(res *decodeResult) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			res.reached = true
			res.payload = handlers.SignedPayloadFrom(r.Context())
			if res.payload.Chain == nil {
				return
			}
			dec := chunked.NewDecoder(r.Body, int64(len(payload)), chunked.WithChain(res.payload.Chain))
			got, err := io.ReadAll(dec)
			res.body, res.err = string(got), err
		})
	}

	// serve runs one signed PUT through the middleware and the decoding handler,
	// checking the parts every case shares.
	serve := func(t *testing.T, mode sigv4.ContentMode, tamper bool) decodeResult {
		t.Helper()

		s := newTestGate(t, newAuthTestConfig())
		var res decodeResult
		rec := httptest.NewRecorder()
		resolveRouter(decodeAt(&res), s.sigV4AuthMiddleware).
			ServeHTTP(rec, newSignedPut(t, mode, tamper))

		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.True(t, res.reached, "the request never reached the handler")
		require.True(t, res.payload.Signed)
		require.True(t, res.payload.Framed())
		require.NotNil(t, res.payload.Chain, "a signed streaming body must carry a chain")

		return res
	}

	for _, mode := range []sigv4.ContentMode{sigv4.StreamingSigned, sigv4.StreamingSignedTrailer} {
		t.Run(string(mode), func(t *testing.T) {
			res := serve(t, mode, false)

			require.NoError(t, res.err)
			assert.Equal(t, payload, res.body)
		})
	}

	t.Run("rejects a body rewritten after signing", func(t *testing.T) {
		// Authentication passes: the signed sentinel says nothing about the body,
		// which is exactly why the chain has to be checked as it decodes.
		res := serve(t, sigv4.StreamingSigned, true)

		assert.ErrorIs(t, res.err, chunked.ErrChunkSignature)
	})
}

// TestSignedPayloadModes covers what the middleware puts on the context for the
// modes that are not a signed stream, since each one routes the body differently.
func TestSignedPayloadModes(t *testing.T) {
	tests := []struct {
		name   string
		mode   sigv4.ContentMode
		framed bool
		chain  bool
	}{
		{"unsigned payload", sigv4.UnsignedPayload, false, false},
		{"unsigned trailer", sigv4.StreamingUnsignedTrailer, true, false},
		{"signed stream", sigv4.StreamingSigned, true, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestGate(t, newAuthTestConfig())

			req := httptest.NewRequest(http.MethodGet, "https://s3.example.com/private/o.txt", nil)
			req.Header.Set("X-Amz-Content-Sha256", string(tc.mode))
			signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
			require.NoError(t, signer.SignHTTP(context.Background(),
				aws.Credentials{AccessKeyID: "AKIAIOSFODNN7EXAMPLE",
					SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
				req, string(tc.mode), "s3", "ap-southeast-2", time.Now().UTC()))

			var p handlers.SignedPayload
			rec := httptest.NewRecorder()
			resolveRouter(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				p = handlers.SignedPayloadFrom(r.Context())
			}), s.sigV4AuthMiddleware).ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.True(t, p.Signed)
			assert.Equal(t, tc.mode, p.Mode)
			assert.Equal(t, tc.framed, p.Framed())
			assert.Equal(t, tc.chain, p.Chain != nil)
		})
	}
}
