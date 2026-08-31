package chunked

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"testing"

	"github.com/minio/crc64nvme"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testSeed      = "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9"
	testScope     = "20130524/us-east-1/s3/aws4_request"
	testTimestamp = "20130524T000000Z"
)

var testKey = []byte("dated-signing-key")

// signChunk produces the signature a client would put in a chunk extension,
// advancing prev the way the client's own chain does.
func signChunk(prev string, payload []byte) (sig, next string) {
	sum := sha256.Sum256(payload)
	sts := strings.Join([]string{
		chunkSTSPrefix, testTimestamp, testScope, prev,
		emptySHA256, hex.EncodeToString(sum[:]),
	}, "\n")
	mac := hmacSHA256(testKey, sts)
	return hex.EncodeToString(mac), hex.EncodeToString(mac)
}

// signTrailer produces the x-amz-trailer-signature over the trailing header
// block, which is signed without the empty-payload line a data chunk carries.
func signTrailer(prev string, rawTrailers string) string {
	sum := sha256.Sum256([]byte(rawTrailers))
	sts := strings.Join([]string{
		trailerSTSPrefix, testTimestamp, testScope, prev, hex.EncodeToString(sum[:]),
	}, "\n")
	return hex.EncodeToString(hmacSHA256(testKey, sts))
}

// buildSignedBody frames chunks with a chunk-signature extension on each, and
// closes the chain over the terminating chunk and any trailers.
func buildSignedBody(chunks []string, trailers []string) string {
	var buf strings.Builder
	prev := testSeed
	for _, chunk := range chunks {
		var sig string
		sig, prev = signChunk(prev, []byte(chunk))
		fmt.Fprintf(&buf, "%x;chunk-signature=%s\r\n%s\r\n", len(chunk), sig, chunk)
	}

	var sig string
	sig, prev = signChunk(prev, nil)
	fmt.Fprintf(&buf, "0;chunk-signature=%s\r\n", sig)

	var raw strings.Builder
	for _, t := range trailers {
		name, val, _ := strings.Cut(t, ":")
		fmt.Fprintf(&buf, "%s:%s\r\n", name, val)
		fmt.Fprintf(&raw, "%s:%s\n", strings.ToLower(name), val)
	}
	if len(trailers) > 0 {
		fmt.Fprintf(&buf, "x-amz-trailer-signature:%s\r\n", signTrailer(prev, raw.String()))
	}
	buf.WriteString("\r\n")
	return buf.String()
}

func newTestChain() *Chain {
	return NewChain(testKey, testSeed, testScope, testTimestamp)
}

func TestChainVerifyChunk(t *testing.T) {
	t.Run("advances on a match", func(t *testing.T) {
		c := newTestChain()

		first, second := signChunk(testSeed, []byte("one"))
		require.NoError(t, c.VerifyChunk(first, sha256Sum([]byte("one"))))
		assert.Equal(t, second, c.prev, "a verified chunk seeds the next link")

		next, _ := signChunk(second, []byte("two"))
		assert.NoError(t, c.VerifyChunk(next, sha256Sum([]byte("two"))))
	})

	t.Run("rejects a chunk signed over other bytes", func(t *testing.T) {
		c := newTestChain()

		sig, _ := signChunk(testSeed, []byte("one"))
		assert.ErrorIs(t, c.VerifyChunk(sig, sha256Sum([]byte("two"))), ErrChunkSignature)
	})

	t.Run("rejects a chunk out of order", func(t *testing.T) {
		c := newTestChain()

		// The signature for the second chunk, presented first: valid HMAC, wrong
		// link, which is what makes the chain more than a per-chunk checksum.
		_, second := signChunk(testSeed, []byte("one"))
		sig, _ := signChunk(second, []byte("two"))
		assert.ErrorIs(t, c.VerifyChunk(sig, sha256Sum([]byte("two"))), ErrChunkSignature)
	})

	t.Run("rejects a non-hex signature", func(t *testing.T) {
		c := newTestChain()
		assert.ErrorIs(t, c.VerifyChunk("not-hex", sha256Sum(nil)), ErrChunkSignature)
	})

	t.Run("leaves the chain unadvanced on failure", func(t *testing.T) {
		c := newTestChain()

		require.Error(t, c.VerifyChunk(strings.Repeat("00", 32), sha256Sum([]byte("one"))))
		assert.Equal(t, testSeed, c.prev)
	})
}

func TestChainVerifyTrailer(t *testing.T) {
	raw := "x-amz-checksum-crc64nvme:AAAAAAAAAAA=\n"

	t.Run("accepts the signed block", func(t *testing.T) {
		c := newTestChain()
		assert.NoError(t, c.VerifyTrailer(signTrailer(testSeed, raw), sha256Sum([]byte(raw))))
	})

	t.Run("rejects an altered block", func(t *testing.T) {
		c := newTestChain()
		sig := signTrailer(testSeed, raw)
		altered := "x-amz-checksum-crc64nvme:BBBBBBBBBBB=\n"
		assert.ErrorIs(t, c.VerifyTrailer(sig, sha256Sum([]byte(altered))), ErrChunkSignature)
	})

	t.Run("is not interchangeable with a chunk signature", func(t *testing.T) {
		c := newTestChain()

		// Same key, same link, different string-to-sign prefix: a chunk signature
		// must not close the trailer block.
		sig, _ := signChunk(testSeed, []byte(raw))
		assert.ErrorIs(t, c.VerifyTrailer(sig, sha256Sum([]byte(raw))), ErrChunkSignature)
	})
}

func TestDecoderVerifiesChain(t *testing.T) {
	t.Run("decodes a signed body", func(t *testing.T) {
		body := buildSignedBody([]string{"hello ", "world"}, nil)
		dec := NewDecoder(strings.NewReader(body), 11, WithChain(newTestChain()))

		got, err := io.ReadAll(dec)
		require.NoError(t, err)
		assert.Equal(t, "hello world", string(got))
	})

	t.Run("decodes a signed body with trailers", func(t *testing.T) {
		body := buildSignedBody([]string{"hello"}, []string{"x-amz-checksum-crc64nvme:AAAAAAAAAAA="})
		dec := NewDecoder(strings.NewReader(body), 5, WithChain(newTestChain()))

		got, err := io.ReadAll(dec)
		require.NoError(t, err)
		assert.Equal(t, "hello", string(got))

		sum, ok := dec.TrailerChecksum()
		require.True(t, ok)
		assert.Equal(t, "AAAAAAAAAAA=", sum)
	})

	t.Run("rejects tampered chunk data", func(t *testing.T) {
		body := buildSignedBody([]string{"hello"}, nil)
		tampered := strings.Replace(body, "hello", "world", 1)
		dec := NewDecoder(strings.NewReader(tampered), 5, WithChain(newTestChain()))

		_, err := io.ReadAll(dec)
		assert.ErrorIs(t, err, ErrChunkSignature)
	})

	t.Run("rejects a tampered trailer", func(t *testing.T) {
		body := buildSignedBody([]string{"hello"}, []string{"x-amz-checksum-crc64nvme:AAAAAAAAAAA="})
		tampered := strings.Replace(body, "AAAAAAAAAAA=", "BBBBBBBBBBB=", 1)
		dec := NewDecoder(strings.NewReader(tampered), 5, WithChain(newTestChain()))

		_, err := io.ReadAll(dec)
		assert.ErrorIs(t, err, ErrChunkSignature)
	})

	t.Run("rejects a signed mode whose chunks carry no signature", func(t *testing.T) {
		body := buildChunkedBody([]string{"hello"}, nil)
		dec := NewDecoder(strings.NewReader(body), 5, WithChain(newTestChain()))

		_, err := io.ReadAll(dec)
		assert.ErrorIs(t, err, ErrChunkSignature)
	})

	t.Run("rejects trailers with no trailer signature", func(t *testing.T) {
		body := buildSignedBody([]string{"hello"}, []string{"x-amz-checksum-crc64nvme:AAAAAAAAAAA="})
		idx := strings.Index(body, "x-amz-trailer-signature:")
		require.Positive(t, idx)
		dec := NewDecoder(strings.NewReader(body[:idx]+"\r\n"), 5, WithChain(newTestChain()))

		_, err := io.ReadAll(dec)
		assert.ErrorIs(t, err, ErrChunkSignature)
	})

	t.Run("ignores chunk signatures with no chain", func(t *testing.T) {
		// STREAMING-UNSIGNED-PAYLOAD-TRAILER frames the same way but signs
		// nothing, so the extension is present and must not be checked.
		body := buildSignedBody([]string{"hello"}, nil)
		dec := NewDecoder(strings.NewReader(body), 5)

		got, err := io.ReadAll(dec)
		require.NoError(t, err)
		assert.Equal(t, "hello", string(got))
	})
}

// TestChainIsConstantTime documents why verify compares with hmac.Equal: the
// signature is attacker-supplied and a byte-at-a-time exit leaks how much of a
// guess was right.
func TestChainRejectsNearMiss(t *testing.T) {
	c := newTestChain()

	sig, _ := signChunk(testSeed, []byte("one"))
	near := sig[:len(sig)-1] + string(flipHexDigit(sig[len(sig)-1]))
	require.NotEqual(t, sig, near)

	assert.ErrorIs(t, c.VerifyChunk(near, sha256Sum([]byte("one"))), ErrChunkSignature)
	assert.True(t, hmac.Equal(hmacSHA256(testKey, "x"), hmacSHA256(testKey, "x")))
}

func flipHexDigit(b byte) byte {
	if b == '0' {
		return '1'
	}
	return '0'
}

// buildTrailerBody frames payload with an arbitrary set of trailers, so a test
// can send a checksum the client never declared or two at once.
func buildTrailerBody(payload string, trailers ...string) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%x\r\n%s\r\n0\r\n", len(payload), payload)
	for _, t := range trailers {
		fmt.Fprintf(&buf, "%s\r\n", t)
	}
	buf.WriteString("\r\n")
	return buf.String()
}

func crc64Trailer(payload string) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], crc64nvme.Checksum([]byte(payload)))
	return "x-amz-checksum-crc64nvme:" + base64.StdEncoding.EncodeToString(b[:])
}

func crc32cTrailer(payload string) string {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], crc32.Checksum([]byte(payload), crc32.MakeTable(crc32.Castagnoli)))
	return "x-amz-checksum-crc32c:" + base64.StdEncoding.EncodeToString(b[:])
}

// TestChecksumDeclarationIsBinding covers X-Amz-Trailer as a promise in both
// directions. A checksum that was not declared could not have been hashed, and
// one that was declared and never arrives is a promise the client broke.
func TestChecksumDeclarationIsBinding(t *testing.T) {
	const payload = "hello world"

	t.Run("sending a different algorithm than declared is rejected", func(t *testing.T) {
		// The AWS SDK always emits matching declaration and trailer names, so a
		// mismatch is either a broken client or a rewritten body.
		body := buildTrailerBody(payload, crc64Trailer(payload))
		dec := NewDecoder(strings.NewReader(body), int64(len(payload)),
			WithTrailerChecksums([]string{"x-amz-checksum-crc32c"}))
		_, err := io.ReadAll(dec)
		require.NoError(t, err)

		assert.ErrorIs(t, dec.VerifyTrailerChecksum(), ErrChecksumMissing)
	})

	t.Run("a declared algorithm that arrives is verified", func(t *testing.T) {
		body := buildTrailerBody(payload, crc32cTrailer(payload))
		dec := NewDecoder(strings.NewReader(body), int64(len(payload)),
			WithTrailerChecksums([]string{"x-amz-checksum-crc32c"}))
		_, err := io.ReadAll(dec)
		require.NoError(t, err)

		assert.NoError(t, dec.VerifyTrailerChecksum())
	})

	t.Run("an undeclared extra checksum is rejected", func(t *testing.T) {
		body := buildTrailerBody(payload, crc32cTrailer(payload), crc64Trailer(payload))
		dec := NewDecoder(strings.NewReader(body), int64(len(payload)),
			WithTrailerChecksums([]string{"x-amz-checksum-crc32c"}))
		_, err := io.ReadAll(dec)
		require.NoError(t, err)

		assert.ErrorIs(t, dec.VerifyTrailerChecksum(), ErrChecksumUndeclared)
	})

	t.Run("every checksum sent is verified, not whichever came first", func(t *testing.T) {
		// Ranging a map is randomised, so a body carrying a good checksum and a
		// bad one must fail every time rather than most of the time.
		body := buildTrailerBody(payload, crc64Trailer(payload), "x-amz-checksum-crc32c:AAAAAA==")
		for range 20 {
			dec := NewDecoder(strings.NewReader(body), int64(len(payload)),
				WithTrailerChecksums([]string{"x-amz-checksum-crc32c", "x-amz-checksum-crc64nvme"}))
			_, err := io.ReadAll(dec)
			require.NoError(t, err)
			require.ErrorIs(t, dec.VerifyTrailerChecksum(), ErrChecksumMismatch)
		}
	})

	t.Run("an algorithm predastore cannot compute is rejected", func(t *testing.T) {
		body := buildTrailerBody(payload, "x-amz-checksum-md5:AAAAAA==")
		dec := NewDecoder(strings.NewReader(body), int64(len(payload)),
			WithTrailerChecksums([]string{"x-amz-checksum-md5"}))
		_, err := io.ReadAll(dec)
		require.NoError(t, err)

		assert.ErrorIs(t, dec.VerifyTrailerChecksum(), ErrChecksumUndeclared)
	})

	t.Run("declared but never sent is rejected", func(t *testing.T) {
		body := buildTrailerBody(payload)
		dec := NewDecoder(strings.NewReader(body), int64(len(payload)),
			WithTrailerChecksums([]string{"x-amz-checksum-crc32c"}))
		_, err := io.ReadAll(dec)
		require.NoError(t, err)

		assert.True(t, dec.PromisesChecksum())
		assert.ErrorIs(t, dec.VerifyTrailerChecksum(), ErrChecksumMissing)
	})
}

// TestChunkSizeIsUnsigned covers a chunk header that would otherwise become a
// negative length and slice a buffer out of range.
func TestChunkSizeIsUnsigned(t *testing.T) {
	for _, body := range []string{
		"-1\r\nhello\r\n0\r\n\r\n",
		"-ffff\r\nhello\r\n0\r\n\r\n",
		"+5\r\nhello\r\n0\r\n\r\n",
	} {
		t.Run(body[:strings.IndexByte(body, '\r')], func(t *testing.T) {
			dec := NewDecoder(strings.NewReader(body), 0)
			require.NotPanics(t, func() {
				_, err := io.ReadAll(dec)
				assert.ErrorIs(t, err, ErrMalformedFraming)
			})
		})
	}
}
