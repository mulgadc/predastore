package chunked

import (
	"bufio"
	"bytes"
	// SHA-1 is one of the checksum algorithms S3 lets a client pick for
	// x-amz-checksum-sha1. It verifies an integrity value the client chose and
	// guards nothing, so its collision weakness is not in play here.
	"crypto/sha1" //nolint:gosec // client-selected integrity checksum, not a security primitive
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"slices"
	"sort"
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
	r                   *bufio.Reader
	curChunkRemaining   int64
	expectChunkDataCRLF bool
	done                bool
	digest              hash.Hash64
	trailers            map[string]string

	// declaredLen is x-amz-decoded-content-length when a caller vouched for it,
	// and -1 otherwise. decoded counts what came out. The write path sizes
	// itself from the declared length, so a body that disagrees with it would
	// otherwise be stored truncated with its checksum still verifying.
	declaredLen int64
	decoded     int64

	// sums holds a running hash per checksum trailer the client promised in
	// X-Amz-Trailer, and promised is that declaration itself. The promise
	// arrives before the body and the value after it, so the hash has to be
	// chosen up front and the two sets reconciled at the end.
	sums     map[string]hash.Hash
	promised []string

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

// WithDeclaredLength requires the decoded body to be exactly n bytes, which is
// what x-amz-decoded-content-length promised and what the write path sized
// itself from. A body that disagrees is rejected rather than truncated to fit.
func WithDeclaredLength(n int64) Option {
	return func(d *Decoder) { d.declaredLen = n }
}

// WithTrailerChecksums prepares to verify the checksum trailers the client
// named in X-Amz-Trailer. An algorithm not named here cannot be checked when it
// arrives, because hashing it had to start before the body was read.
//
// A name with no hash behind it is still recorded as promised, so an algorithm
// predastore cannot compute is rejected rather than quietly forgotten.
func WithTrailerChecksums(names []string) Option {
	return func(d *Decoder) {
		for _, name := range names {
			name = strings.ToLower(strings.TrimSpace(name))
			if !strings.HasPrefix(name, checksumPrefix) {
				continue
			}
			d.promised = append(d.promised, name)
			if newHash, ok := checksumAlgos[name]; ok {
				d.sums[name] = newHash()
			}
		}
		sort.Strings(d.promised)
		d.promised = slices.Compact(d.promised)
	}
}

// NewDecoder wraps src and returns a streaming decoder.
// decodedLenHint can be 0 if you don't care, or x-amz-decoded-content-length if present.
func NewDecoder(src io.Reader, decodedLenHint int64, opts ...Option) *Decoder {
	d := &Decoder{
		r:           bufio.NewReader(src),
		digest:      crc64nvme.New(),
		trailers:    make(map[string]string),
		declaredLen: -1,
		sums:        make(map[string]hash.Hash),
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
			// Only the terminating chunk ends a body, and it sets done before
			// reporting EOF. An EOF from anywhere else is a truncated stream:
			// calling that a clean end skips every trailing check.
			if errors.Is(err, io.EOF) && d.done {
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
		d.decoded += int64(n)
		_, _ = d.digest.Write(p[:n]) // checksum update cannot fail
		if d.chunkHash != nil {
			_, _ = d.chunkHash.Write(p[:n])
		}
		for _, h := range d.sums {
			_, _ = h.Write(p[:n])
		}

		// Caught here as well as at the terminating chunk, so an overlong body
		// stops at the declared length instead of streaming on.
		if d.declaredLen >= 0 && d.decoded > d.declaredLen {
			return n, fmt.Errorf("%w: body exceeds the declared %d bytes",
				ErrMalformedFraming, d.declaredLen)
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
		// A chunk that ends before the length its header declared is truncated,
		// not finished, whatever the transport says.
		if errors.Is(err, io.EOF) && d.curChunkRemaining > 0 {
			return n, fmt.Errorf("%w: chunk ended %d bytes early",
				ErrMalformedFraming, d.curChunkRemaining)
		}
		return n, err
	}

	return n, nil
}

// CRC64 returns the computed CRC64NVME checksum of the decoded payload.
func (d *Decoder) CRC64() uint64 {
	return d.digest.Sum64()
}

const (
	checksumPrefix    = "x-amz-checksum-"
	checksumCRC64NVME = "x-amz-checksum-crc64nvme"
)

// checksumAlgos are the x-amz-checksum-* trailers S3 defines, each keyed by the
// trailer name a client sends and mapped to the hash that verifies it.
var checksumAlgos = map[string]func() hash.Hash{
	"x-amz-checksum-crc32":     func() hash.Hash { return crc32.NewIEEE() },
	"x-amz-checksum-crc32c":    func() hash.Hash { return crc32.New(crc32.MakeTable(crc32.Castagnoli)) },
	"x-amz-checksum-crc64nvme": func() hash.Hash { return crc64nvme.New() },
	"x-amz-checksum-sha1":      sha1.New,
	"x-amz-checksum-sha256":    sha256.New,
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

// checksumTrailers returns every x-amz-checksum-* trailer the body carried, in
// name order. Sorted because ranging a map is randomised, and a body carrying
// two checksums must not verify whichever one the iteration happened to pick.
func (d *Decoder) checksumTrailers() []string {
	var names []string
	for k := range d.trailers {
		if strings.HasPrefix(k, checksumPrefix) {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	return names
}

// ChecksumTrailer reports whether the body carried any checksum trailer.
func (d *Decoder) ChecksumTrailer() (name, value string, ok bool) {
	names := d.checksumTrailers()
	if len(names) == 0 {
		return "", "", false
	}
	return names[0], d.trailers[names[0]], true
}

// PromisesChecksum reports whether X-Amz-Trailer named a checksum. The promise
// is the client's, not the signature's, so it binds an unauthenticated write
// exactly as it binds a signed one.
func (d *Decoder) PromisesChecksum() bool {
	return len(d.promised) > 0
}

// VerifyTrailerChecksum checks the body against every checksum trailer it
// carried, and against what X-Amz-Trailer said those would be.
//
// The declared set is enforced in both directions. A trailer that was not
// declared cannot be verified, because hashing had to start before the body was
// read; a declared one that never arrives is a promise the client broke.
func (d *Decoder) VerifyTrailerChecksum() error {
	sent := d.checksumTrailers()
	if len(sent) == 0 {
		return fmt.Errorf("%w: body carries no checksum trailer", ErrChecksumMissing)
	}

	if len(d.promised) > 0 {
		for _, name := range d.promised {
			if _, ok := d.trailers[name]; !ok {
				return fmt.Errorf("%w: %s was declared but not sent", ErrChecksumMissing, name)
			}
		}
		for _, name := range sent {
			if !slices.Contains(d.promised, name) {
				return fmt.Errorf("%w: %s was sent but not declared in X-Amz-Trailer",
					ErrChecksumUndeclared, name)
			}
		}
	}

	for _, name := range sent {
		if err := d.verifyOneChecksum(name, d.trailers[name]); err != nil {
			return err
		}
	}
	return nil
}

// verifyOneChecksum compares one trailer against the hash that was running for
// it. crc64nvme is always running, so it is checked whether or not the client
// declared it; anything else had to be declared to have been hashed at all.
func (d *Decoder) verifyOneChecksum(name, value string) error {
	if name == checksumCRC64NVME {
		return VerifyCRC64NVME(value, d.CRC64())
	}

	h, ok := d.sums[name]
	if !ok {
		return fmt.Errorf("%w: %s cannot be verified", ErrChecksumUndeclared, name)
	}
	want, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("%w: %s is not base64", ErrChecksumMismatch, name)
	}
	if got := h.Sum(nil); !bytes.Equal(want, got) {
		return fmt.Errorf("%w: %s expected %x, got %x", ErrChecksumMismatch, name, want, got)
	}
	return nil
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
	// Unsigned, so a leading '-' is rejected by the parse rather than becoming a
	// negative length that later slices a buffer out of range. 63 bits keeps the
	// result representable as the int64 the rest of the decoder counts in.
	sizeStr := strings.TrimSpace(line)
	size, err := strconv.ParseUint(sizeStr, 16, 63)
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
		// A short body reaches its terminator legitimately, so the declared
		// length can only be settled here.
		if d.declaredLen >= 0 && d.decoded != d.declaredLen {
			return fmt.Errorf("%w: body is %d bytes, declared %d",
				ErrMalformedFraming, d.decoded, d.declaredLen)
		}
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

	d.curChunkRemaining = int64(size)
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
