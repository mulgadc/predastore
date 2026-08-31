package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/require"
)

// BenchmarkPutWritePath measures what the payload check costs the write path: the SigV4
// verification, then writeObject and finishPayload, the sequence PutObject runs before a
// placement lands. The verified case binds the body to its signed digest; UNSIGNED-PAYLOAD
// is the same write with the binding skipped, so the difference is the cost of verifying.
//
// Both halves of the check are inside the timer, which matters because they fall in
// different places: a body at or below sigv4.MaxPayloadLen is hashed during Verify, and a
// larger one as writeObject reads it.
func BenchmarkPutWritePath(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{name: "64KiB", size: 64 << 10},
		{name: "1MiB", size: 1 << 20},
		{name: "8MiB", size: 8 << 20},
		{name: "32MiB", size: 32 << 20},
	}

	for _, sz := range sizes {
		body := bytes.Repeat([]byte("x"), sz.size)
		sum := sha256.Sum256(body)

		modes := []struct {
			name        string
			payloadHash string
		}{
			{name: "verified", payloadHash: hex.EncodeToString(sum[:])},
			{name: "unsigned", payloadHash: string(sigv4.UnsignedPayload)},
		}

		for _, mode := range modes {
			b.Run(sz.name+"/"+mode.name, func(b *testing.B) {
				f := newWriteFixture(4, 2)
				ctx := context.Background()
				objectHash := model.ObjectHash("bucket", "object.txt")

				// Signing once keeps the SDK signer out of the measurement. The signature
				// covers the declared hash rather than the bytes, so it stays valid for
				// every iteration that replays the same body.
				signed := signPut(b, body, mode.payloadHash)

				b.SetBytes(int64(sz.size))
				b.ReportAllocs()
				b.ResetTimer()

				for range b.N {
					req := replayPut(signed, body)

					require.NoError(b, verifyPut(req))

					decoded, size, dec := decodeBody(req)
					_, _, err := f.write(ctx, objectHash, decoded, size)
					require.NoError(b, err)

					require.NoError(b, finishPayload(req, dec))
				}
			})
		}
	}
}

// replayPut rebuilds a signed request over a fresh reader, so each iteration starts from an
// unconsumed body without re-signing.
func replayPut(signed *http.Request, body []byte) *http.Request {
	req := signed.Clone(signed.Context())
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))

	return req
}
