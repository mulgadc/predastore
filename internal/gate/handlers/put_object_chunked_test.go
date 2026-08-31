package handlers

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/minio/crc64nvme"
	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// framedBody wraps payload in a single aws-chunked frame with a trailing
// checksum header, the shape aws-cli sends for a multi-megabyte PUT. The
// trailer is a zero CRC, which no non-empty payload hashes to.
func framedBody(payload string) string {
	return framedBodyChecksum(payload, "AAAAAAAAAAA=")
}

func framedBodyChecksum(payload, checksum string) string {
	return fmt.Sprintf("%x\r\n%s\r\n0\r\nx-amz-checksum-crc64nvme:%s\r\n\r\n",
		len(payload), payload, checksum)
}

// crc64Trailer is the trailer value a correct client would send for payload.
func crc64Trailer(payload string) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], crc64nvme.Checksum([]byte(payload)))
	return base64.StdEncoding.EncodeToString(b[:])
}

// chunkedPut builds a PUT carrying framed bytes, with contentEncoding set only
// when non-empty so a request with the header absent can be tested too.
func chunkedPut(tb testing.TB, payload, contentEncoding string, p SignedPayload) *http.Request {
	return chunkedPutBody(tb, framedBody(payload), payload, contentEncoding, p)
}

func chunkedPutBody(tb testing.TB, body, payload, contentEncoding string, p SignedPayload) *http.Request {
	tb.Helper()

	req, err := http.NewRequest(http.MethodPut, "https://bucket.example.com/o", strings.NewReader(body))
	require.NoError(tb, err)
	req.ContentLength = int64(len(body))
	req.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(payload)))
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}

	return req.WithContext(WithSignedPayload(req.Context(), p))
}

// TestDecodeBodyDetectsFraming covers which requests get their framing stripped.
// Storing framing as object data is silent corruption: the write succeeds, the
// object is wrong, and nothing reports it until the object is read back.
func TestDecodeBodyDetectsFraming(t *testing.T) {
	const payload = "hello world"

	t.Run("signed streaming mode with no Content-Encoding", func(t *testing.T) {
		// AWS documents the header as optional on a chunked upload, so its
		// absence says nothing about whether the body is framed.
		req := chunkedPut(t, payload, "", SignedPayload{
			Signed: true, Mode: sigv4.StreamingUnsignedTrailer,
		})

		body, size, dec := decodeBody(req)
		require.NotNil(t, dec)
		assert.Equal(t, int64(len(payload)), size)

		got, err := io.ReadAll(body)
		require.NoError(t, err)
		assert.Equal(t, payload, string(got))
	})

	t.Run("signed streaming mode with a compound Content-Encoding", func(t *testing.T) {
		req := chunkedPut(t, payload, "aws-chunked, gzip", SignedPayload{
			Signed: true, Mode: sigv4.StreamingSignedTrailer,
		})

		body, _, dec := decodeBody(req)
		require.NotNil(t, dec)

		got, err := io.ReadAll(body)
		require.NoError(t, err)
		assert.Equal(t, payload, string(got))
	})

	t.Run("signed literal digest ignores a spoofed Content-Encoding", func(t *testing.T) {
		// Content-Encoding is not a signed header, so anything on the path can
		// add it. A client that signed a digest of the exact bytes it sent did
		// not frame them, whatever the header now says.
		req := chunkedPut(t, payload, "aws-chunked", SignedPayload{Signed: true})

		body, size, dec := decodeBody(req)
		assert.Nil(t, dec)
		assert.Equal(t, req.ContentLength, size)

		got, err := io.ReadAll(body)
		require.NoError(t, err)
		assert.Equal(t, framedBody(payload), string(got))
	})

	t.Run("signed unsigned-payload is not framed", func(t *testing.T) {
		req := chunkedPut(t, payload, "aws-chunked", SignedPayload{
			Signed: true, Mode: sigv4.UnsignedPayload,
		})

		_, _, dec := decodeBody(req)
		assert.Nil(t, dec)
	})

	t.Run("unauthenticated request falls back to Content-Encoding", func(t *testing.T) {
		// A public-bucket write signs nothing, so the header is all there is.
		req := chunkedPut(t, payload, "aws-chunked", SignedPayload{})

		body, _, dec := decodeBody(req)
		require.NotNil(t, dec)

		got, err := io.ReadAll(body)
		require.NoError(t, err)
		assert.Equal(t, payload, string(got))
	})

	t.Run("unauthenticated request with no Content-Encoding", func(t *testing.T) {
		req := chunkedPut(t, payload, "", SignedPayload{})

		_, _, dec := decodeBody(req)
		assert.Nil(t, dec)
	})

	t.Run("framed body with no decoded length is undeclared", func(t *testing.T) {
		req := chunkedPut(t, payload, "", SignedPayload{
			Signed: true, Mode: sigv4.StreamingUnsignedTrailer,
		})
		req.Header.Del("X-Amz-Decoded-Content-Length")

		// Content-Length measures the framing, so with the decoded length gone
		// there is no size the splitter can use and the write must be rejected.
		_, size, dec := decodeBody(req)
		require.NotNil(t, dec)
		assert.Equal(t, int64(-1), size)
	})
}

// TestFinishPayloadTrailerChecksum covers the trailing checksum, which the
// decoder computes on every framed body and, before this, never compared.
func TestFinishPayloadTrailerChecksum(t *testing.T) {
	const payload = "hello world"

	t.Run("mismatched trailer is rejected", func(t *testing.T) {
		req := chunkedPut(t, payload, "", SignedPayload{
			Signed: true, Mode: sigv4.StreamingUnsignedTrailer,
		})

		body, _, dec := decodeBody(req)
		_, err := io.ReadAll(body)
		require.NoError(t, err)

		// The fixture's trailer is a zero CRC, which "hello world" is not.
		require.Error(t, finishPayload(req, dec))
	})

	t.Run("matching trailer is accepted", func(t *testing.T) {
		framed := framedBodyChecksum(payload, crc64Trailer(payload))
		req := chunkedPutBody(t, framed, payload, "", SignedPayload{
			Signed: true, Mode: sigv4.StreamingUnsignedTrailer,
		})

		body, _, dec := decodeBody(req)
		_, err := io.ReadAll(body)
		require.NoError(t, err)

		sum, ok := dec.TrailerChecksum()
		require.True(t, ok)
		assert.Equal(t, crc64Trailer(payload), sum, "the sent trailer is read, not invented")

		require.NoError(t, finishPayload(req, dec))
	})

	t.Run("framed body with no trailer at all", func(t *testing.T) {
		// STREAMING-UNSIGNED-PAYLOAD-TRAILER names a trailer but a body that
		// omits it is still framed; there is simply nothing to compare.
		framed := fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(payload), payload)
		req := chunkedPutBody(t, framed, payload, "", SignedPayload{
			Signed: true, Mode: sigv4.StreamingUnsignedTrailer,
		})

		body, _, dec := decodeBody(req)
		_, err := io.ReadAll(body)
		require.NoError(t, err)

		assert.NoError(t, finishPayload(req, dec))
	})
}
