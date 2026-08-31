package chunked

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"strconv"
	"strings"

	"github.com/minio/crc64nvme"
)

// Decoder decodes AWS-style chunked uploads with trailing checksum,
// e.g. bodies that look like:
//
//	<size-hex>\r\n
//	<data...>\r\n
//	0\r\n
//	x-amz-checksum-crc64nvme:<base64>\r\n
//	\r\n
//
// It implements io.Reader and streams out ONLY the real object bytes.
type Decoder struct {
	r                    *bufio.Reader
	curChunkRemaining    int64
	expectChunkDataCRLF  bool
	done                 bool
	decodedRemainingHint int64 // optional sanity check, may be 0
	digest               hash.Hash64
	trailers             map[string]string

	// Chain verification. chain is nil unless the client signed its chunks. The
	// signature arrives in the header that precedes the data, so a chunk can
	// only be checked once all of it has been read: it is hashed as it streams
	// and compared at the chunk boundary, which keeps the client's chunk size
	// out of our memory.
	chain     *Chain
	chunkSig  string
	chunkHash hash.Hash
	rawTrail  []byte
}

// Option configures a Decoder.
type Option func(*Decoder)

// WithChain verifies each chunk against the signature chain seeded from the
// request signature, and rejects a body whose chunks do not continue it.
func WithChain(c *Chain) Option {
	return func(d *Decoder) { d.chain = c }
}

// NewDecoder wraps src and returns a streaming decoder.
// decodedLenHint can be 0 if you don't care, or x-amz-decoded-content-length if present.
func NewDecoder(src io.Reader, decodedLenHint int64, opts ...Option) *Decoder {
	d := &Decoder{
		r:                    bufio.NewReader(src),
		decodedRemainingHint: decodedLenHint,
		digest:               crc64nvme.New(),
		trailers:             make(map[string]string),
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Read implements io.Reader, returning only the decoded payload bytes.
func (d *Decoder) Read(p []byte) (int, error) {
	if d.done {
		return 0, io.EOF
	}

	// If we finished a chunk on a previous call, consume its trailing CRLF.
	if d.expectChunkDataCRLF {
		if err := d.consumeCRLF(); err != nil {
			return 0, err
		}
		d.expectChunkDataCRLF = false
	}

	// Ensure we have an active chunk.
	if d.curChunkRemaining == 0 {
		if err := d.readNextChunkHeader(); err != nil {
			if errors.Is(err, io.EOF) {
				return 0, io.EOF
			}
			return 0, err
		}
		// After readNextChunkHeader, 0-size means we already read trailers & are done.
		if d.done {
			return 0, io.EOF
		}
	}

	// Limit read to current chunk.
	if int64(len(p)) > d.curChunkRemaining {
		p = p[:d.curChunkRemaining]
	}

	n, err := d.r.Read(p)
	if n > 0 {
		d.curChunkRemaining -= int64(n)
		_, _ = d.digest.Write(p[:n]) // checksum update cannot fail
		if d.chunkHash != nil {
			_, _ = d.chunkHash.Write(p[:n])
		}

		if d.decodedRemainingHint > 0 {
			d.decodedRemainingHint -= int64(n)
		}

		if d.curChunkRemaining == 0 {
			// Next Read must consume CRLF after this chunk's data.
			d.expectChunkDataCRLF = true

			// The chunk is complete, so its signature can finally be checked.
			// The bytes have already been handed to the caller: the write path
			// records no placement until the body is finished, so a chunk that
			// fails here leaves shards nothing refers to rather than an object.
			if err := d.verifyChunk(); err != nil {
				return n, err
			}
		}
	}

	if err != nil {
		return n, err
	}

	return n, nil
}

// CRC64 returns the computed CRC64NVME checksum of the decoded payload.
func (d *Decoder) CRC64() uint64 {
	return d.digest.Sum64()
}

// TrailerChecksum returns the x-amz-checksum-crc64nvme trailer value if present.
func (d *Decoder) TrailerChecksum() (string, bool) {
	val, ok := d.trailers["x-amz-checksum-crc64nvme"]
	if ok {
		return val, true
	}
	// be a bit tolerant, in case a proxy changed case
	for k, v := range d.trailers {
		if strings.EqualFold(k, "x-amz-checksum-crc64nvme") {
			return v, true
		}
	}
	return "", false
}

// VerifyTrailerChecksum validates the trailer checksum (if present)
// against the streamed CRC64NVME value.
func (d *Decoder) VerifyTrailerChecksum() error {
	trailer, ok := d.TrailerChecksum()
	if !ok {
		return fmt.Errorf("missing x-amz-checksum-crc64nvme trailer")
	}
	return VerifyCRC64NVME(trailer, d.CRC64())
}

// VerifyCRC64NVME is a helper for validating a base64-encoded CRC64NVME
// checksum against a computed uint64. It returns nil on success.
func VerifyCRC64NVME(base64Checksum string, crc uint64) error {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(base64Checksum))
	if err != nil {
		return fmt.Errorf("invalid base64 CRC64NVME: %w", err)
	}
	if len(raw) != 8 {
		return fmt.Errorf("invalid CRC64NVME length: got %d, want 8", len(raw))
	}
	expected := binary.BigEndian.Uint64(raw)
	if expected != crc {
		return fmt.Errorf("CRC64NVME mismatch: expected %016x, got %016x", expected, crc)
	}
	return nil
}

// readNextChunkHeader reads the next "<size>[;ext...]\r\n" line.
// If size==0, it reads trailers and marks the decoder as done.
func (d *Decoder) readNextChunkHeader() error {
	line, err := d.r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("%w: failed to read chunk header: %w", ErrMalformedFraming, err)
	}
	// tolerate either "\r\n" or "\n" endings
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return fmt.Errorf("%w: empty chunk header", ErrMalformedFraming)
	}

	// The extensions carry chunk-signature, which is the whole of what binds a
	// signed body to its principal, so they are parsed rather than skipped.
	ext := ""
	if idx := strings.IndexByte(line, ';'); idx >= 0 {
		ext, line = line[idx+1:], line[:idx]
	}
	sizeStr := strings.TrimSpace(line)
	size, err := strconv.ParseInt(sizeStr, 16, 64)
	if err != nil {
		return fmt.Errorf("%w: invalid chunk size %q: %w", ErrMalformedFraming, sizeStr, err)
	}

	if d.chain != nil {
		d.chunkSig = chunkSignature(ext)
		if d.chunkSig == "" {
			return fmt.Errorf("%w: chunk carries no chunk-signature", ErrChunkSignature)
		}
		d.chunkHash = sha256.New()
	}

	if size == 0 {
		// The terminating chunk signs no data, so it closes the chain against
		// an empty payload before the trailers are read.
		if err := d.verifyChunk(); err != nil {
			return err
		}
		if err := d.readTrailers(); err != nil {
			return err
		}
		if err := d.verifyTrailer(); err != nil {
			return err
		}
		d.done = true
		return io.EOF
	}

	d.curChunkRemaining = size
	return nil
}

// verifyChunk closes out the current chunk against the chain. It is a no-op on
// an unsigned body, which is the STREAMING-UNSIGNED-PAYLOAD-TRAILER case and a
// deliberate client opt-out rather than an omission.
func (d *Decoder) verifyChunk() error {
	if d.chain == nil {
		return nil
	}
	sum := sha256Sum(nil)
	if d.chunkHash != nil {
		sum = d.chunkHash.Sum(nil)
	}
	if err := d.chain.VerifyChunk(d.chunkSig, sum); err != nil {
		return err
	}
	d.chunkHash, d.chunkSig = nil, ""
	return nil
}

// verifyTrailer checks the trailing header block against the chain. A signed
// body without a trailer never reaches this with a signature to check, so an
// absent trailer signature is only an error when trailers were actually sent.
func (d *Decoder) verifyTrailer() error {
	if d.chain == nil {
		return nil
	}
	sig, ok := d.trailers["x-amz-trailer-signature"]
	if !ok {
		if len(d.rawTrail) == 0 {
			return nil
		}
		return fmt.Errorf("%w: trailers carry no x-amz-trailer-signature", ErrChunkSignature)
	}
	return d.chain.VerifyTrailer(sig, sha256Sum(d.rawTrail))
}

// chunkSignature pulls chunk-signature out of a chunk extension list, which is
// ";name=value" repeated and is not required to hold anything else.
func chunkSignature(ext string) string {
	for part := range strings.SplitSeq(ext, ";") {
		name, value, found := strings.Cut(part, "=")
		if found && strings.EqualFold(strings.TrimSpace(name), "chunk-signature") {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// readTrailers reads lines until an empty line, populating d.trailers.
func (d *Decoder) readTrailers() error {
	for {
		line, err := d.r.ReadString('\n')
		if err != nil {
			return fmt.Errorf("%w: failed to read trailer line: %w", ErrMalformedFraming, err)
		}
		if line == "\r\n" || line == "\n" {
			// end of trailers
			break
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		colon := strings.IndexByte(line, ':')
		if colon <= 0 {
			// malformed trailer, ignore it
			continue
		}
		name := strings.ToLower(strings.TrimSpace(line[:colon]))
		val := strings.TrimSpace(line[colon+1:])
		d.trailers[name] = val

		// The trailer signature covers the trailing headers but not itself, and
		// signs them one per line as "name:value\n".
		if name != "x-amz-trailer-signature" {
			d.rawTrail = append(d.rawTrail, (name + ":" + val + "\n")...)
		}
	}
	return nil
}

func (d *Decoder) consumeCRLF() error {
	b1, err := d.r.ReadByte()
	if err != nil {
		return fmt.Errorf("%w: failed to read CR after chunk data: %w", ErrMalformedFraming, err)
	}
	b2, err := d.r.ReadByte()
	if err != nil {
		return fmt.Errorf("%w: failed to read LF after chunk data: %w", ErrMalformedFraming, err)
	}
	// be tolerant: accept "\n\n" or "\r\n" etc, but ideally we see "\r\n"
	if b2 != '\n' {
		return fmt.Errorf("%w: invalid chunk terminator, want \\r\\n, got %q%q", ErrMalformedFraming, b1, b2)
	}
	return nil
}
