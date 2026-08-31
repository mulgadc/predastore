package chunked

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// ErrChunkSignature is returned when a chunk's signature does not continue the
// chain seeded by the request signature, which means the bytes in that chunk
// are not the bytes the client signed.
var ErrChunkSignature = errors.New("chunk signature does not match")

// ErrMalformedFraming is returned when an aws-chunked body's framing does not
// parse. It is separate from a signature failure because the two are different
// answers to the client: one is malformed, the other is unauthenticated.
var ErrMalformedFraming = errors.New("malformed aws-chunked framing")

// ErrChecksumMissing is returned when a body promised a trailing checksum and
// did not send one. The mode names the trailer, so its absence is malformed
// rather than a client declining to checksum.
var ErrChecksumMissing = errors.New("promised checksum trailer is absent")

// ErrChecksumUndeclared is returned for a checksum trailer the client never
// named in X-Amz-Trailer. Hashing has to begin before the body is read, so
// there is nothing to compare it against.
var ErrChecksumUndeclared = errors.New("checksum trailer was not declared")

// ErrChecksumMismatch is returned when a body does not match the checksum it
// carried.
var ErrChecksumMismatch = errors.New("checksum does not match")

// The string-to-sign prefixes AWS defines for the two links in the chain: one
// per data chunk, and one for the trailing header block.
const (
	chunkSTSPrefix   = "AWS4-HMAC-SHA256-PAYLOAD"
	trailerSTSPrefix = "AWS4-HMAC-SHA256-TRAILER"
)

// emptySHA256 is the hex SHA-256 of no bytes, which every chunk's string-to-sign
// carries in place of the canonical-request hash a normal signature would use.
var emptySHA256 = hex.EncodeToString(sha256Sum(nil))

// Chain verifies the per-chunk signature chain of an aws-chunked body. A client
// signing STREAMING-AWS4-HMAC-SHA256-PAYLOAD covers its body with a chain of
// HMACs seeded from the request signature, and that chain is the only thing
// binding the body to the principal: the payload hash the request signed is a
// sentinel rather than a digest.
type Chain struct {
	key       []byte
	scope     string
	timestamp string
	prev      string
}

// NewChain seeds a chain from a verified request. signingKey is the request's
// dated SigV4 signing key, seed its signature, scope the credential scope
// "<date>/<region>/<service>/aws4_request", and timestamp its x-amz-date.
func NewChain(signingKey []byte, seed, scope, timestamp string) *Chain {
	return &Chain{key: signingKey, scope: scope, timestamp: timestamp, prev: seed}
}

// VerifyChunk checks one data chunk against the chain and advances it.
// payloadSHA256 is the SHA-256 of the chunk's decoded bytes, taken as a digest
// rather than the bytes so a caller can hash a chunk as it streams instead of
// holding a client-sized buffer.
func (c *Chain) VerifyChunk(signature string, payloadSHA256 []byte) error {
	return c.verify(signature, strings.Join([]string{
		chunkSTSPrefix,
		c.timestamp,
		c.scope,
		c.prev,
		emptySHA256,
		hex.EncodeToString(payloadSHA256),
	}, "\n"))
}

// VerifyTrailer checks the trailing header block, which is signed without the
// empty-payload line that a data chunk carries.
func (c *Chain) VerifyTrailer(signature string, trailerSHA256 []byte) error {
	return c.verify(signature, strings.Join([]string{
		trailerSTSPrefix,
		c.timestamp,
		c.scope,
		c.prev,
		hex.EncodeToString(trailerSHA256),
	}, "\n"))
}

// verify compares a supplied signature against the one this link should carry
// and, on a match, makes it the seed for the next link.
func (c *Chain) verify(signature, stringToSign string) error {
	want := hmacSHA256(c.key, stringToSign)
	got, err := hex.DecodeString(strings.TrimSpace(signature))
	if err != nil {
		return fmt.Errorf("%w: signature is not hex", ErrChunkSignature)
	}
	// hmac.Equal rather than ==: the comparison is on attacker-supplied input,
	// and a byte-at-a-time exit leaks how much of a guess was right.
	if !hmac.Equal(want, got) {
		return ErrChunkSignature
	}
	c.prev = hex.EncodeToString(want)
	return nil
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func sha256Sum(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}
