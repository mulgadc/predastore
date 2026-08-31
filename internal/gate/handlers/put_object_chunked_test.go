package handlers

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/minio/crc64nvme"
	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/gate/chunked"
	"github.com/mulgadc/predastore/internal/gate/model"
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

	t.Run("trailer mode with no trailer is rejected", func(t *testing.T) {
		// STREAMING-UNSIGNED-PAYLOAD-TRAILER promises a trailing checksum in the
		// sentinel the client signed. With no chain either, accepting this
		// leaves the body with nothing checking it at all.
		framed := fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(payload), payload)
		req := chunkedPutBody(t, framed, payload, "", SignedPayload{
			Signed: true, Mode: sigv4.StreamingUnsignedTrailer,
		})

		body, _, dec := decodeBody(req)
		_, err := io.ReadAll(body)
		require.NoError(t, err)

		assert.ErrorIs(t, finishPayload(req, dec), model.ErrMalformedChunkedBodyError)
	})

	t.Run("signed stream with no trailer is accepted", func(t *testing.T) {
		// STREAMING-AWS4-HMAC-SHA256-PAYLOAD promises no trailer, and its chain
		// already covers the body, so there is nothing missing here.
		framed := fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(payload), payload)
		req := chunkedPutBody(t, framed, payload, "", SignedPayload{
			Signed: true, Mode: sigv4.StreamingSigned,
		})

		body, _, dec := decodeBody(req)
		_, err := io.ReadAll(body)
		require.NoError(t, err)

		assert.NoError(t, finishPayload(req, dec))
	})

	t.Run("undeclared checksum algorithm is rejected", func(t *testing.T) {
		// Hashing has to start before the body is read, so a trailer the client
		// never named in X-Amz-Trailer cannot be verified after the fact.
		framed := fmt.Sprintf("%x\r\n%s\r\n0\r\nx-amz-checksum-crc32c:AAAAAA==\r\n\r\n",
			len(payload), payload)
		req := chunkedPutBody(t, framed, payload, "", SignedPayload{
			Signed: true, Mode: sigv4.StreamingUnsignedTrailer,
		})

		body, _, dec := decodeBody(req)
		_, err := io.ReadAll(body)
		require.NoError(t, err)

		assert.ErrorIs(t, finishPayload(req, dec), model.ErrMalformedChunkedBodyError)
	})

	t.Run("declared crc32c is verified", func(t *testing.T) {
		sum := crc32.Checksum([]byte(payload), crc32.MakeTable(crc32.Castagnoli))
		var raw [4]byte
		binary.BigEndian.PutUint32(raw[:], sum)
		encoded := base64.StdEncoding.EncodeToString(raw[:])

		framed := fmt.Sprintf("%x\r\n%s\r\n0\r\nx-amz-checksum-crc32c:%s\r\n\r\n",
			len(payload), payload, encoded)
		req := chunkedPutBody(t, framed, payload, "", SignedPayload{
			Signed: true, Mode: sigv4.StreamingUnsignedTrailer,
		})
		req.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")

		body, _, dec := decodeBody(req)
		_, err := io.ReadAll(body)
		require.NoError(t, err)

		assert.NoError(t, finishPayload(req, dec))
	})
}

// TestDecodeBodyDeclaredLength covers a body that disagrees with the length the
// write path sized itself from. The writer stops at the declared length, so
// without this the object is stored truncated and its checksum still verifies.
func TestDecodeBodyDeclaredLength(t *testing.T) {
	const payload = "hello world"

	t.Run("understated length is rejected", func(t *testing.T) {
		req := chunkedPutBody(t, framedBodyChecksum(payload, crc64Trailer(payload)),
			payload, "", SignedPayload{Signed: true, Mode: sigv4.StreamingUnsignedTrailer})
		req.Header.Set("X-Amz-Decoded-Content-Length", "5")

		body, size, _ := decodeBody(req)
		assert.Equal(t, int64(5), size)

		_, err := io.ReadAll(body)
		assert.ErrorIs(t, err, chunked.ErrMalformedFraming)
	})

	t.Run("overstated length is rejected", func(t *testing.T) {
		req := chunkedPutBody(t, framedBodyChecksum(payload, crc64Trailer(payload)),
			payload, "", SignedPayload{Signed: true, Mode: sigv4.StreamingUnsignedTrailer})
		req.Header.Set("X-Amz-Decoded-Content-Length", "99")

		body, _, _ := decodeBody(req)
		_, err := io.ReadAll(body)
		assert.ErrorIs(t, err, chunked.ErrMalformedFraming)
	})
}
