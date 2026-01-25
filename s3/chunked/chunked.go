package chunked

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"net/http"
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
}

// NewDecoder wraps src and returns a streaming decoder.
// decodedLenHint can be 0 if you don't care, or x-amz-decoded-content-length if present.
func NewDecoder(src io.Reader, decodedLenHint int64) *Decoder {
	return &Decoder{
		r:                    bufio.NewReader(src),
		decodedRemainingHint: decodedLenHint,
		digest:               crc64nvme.New(),
		trailers:             make(map[string]string),
	}
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
			if err == io.EOF {
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

		if d.decodedRemainingHint > 0 {
			d.decodedRemainingHint -= int64(n)
		}

		if d.curChunkRemaining == 0 {
			// Next Read must consume CRLF after this chunk's data.
			d.expectChunkDataCRLF = true
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
		return fmt.Errorf("failed to read chunk header: %w", err)
	}
	// tolerate either "\r\n" or "\n" endings
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return fmt.Errorf("empty chunk header")
	}

	// strip any chunk extensions after ';'
	if idx := strings.IndexByte(line, ';'); idx >= 0 {
		line = line[:idx]
	}
	sizeStr := strings.TrimSpace(line)
	size, err := parseHexInt64(sizeStr)
	if err != nil {
		return fmt.Errorf("invalid chunk size %q: %w", sizeStr, err)
	}

	if size == 0 {
		// Final chunk: read trailers, then final empty line.
		if err := d.readTrailers(); err != nil {
			return err
		}
		d.done = true
		return io.EOF
	}

	d.curChunkRemaining = size
	return nil
}

// readTrailers reads lines until an empty line, populating d.trailers.
func (d *Decoder) readTrailers() error {
	for {
		line, err := d.r.ReadString('\n')
		if err != nil {
			return fmt.Errorf("failed to read trailer line: %w", err)
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
	}
	return nil
}

func (d *Decoder) consumeCRLF() error {
	b1, err := d.r.ReadByte()
	if err != nil {
		return fmt.Errorf("failed to read CR after chunk data: %w", err)
	}
	b2, err := d.r.ReadByte()
	if err != nil {
		return fmt.Errorf("failed to read LF after chunk data: %w", err)
	}
	// be tolerant: accept "\n\n" or "\r\n" etc, but ideally we see "\r\n"
	if b2 != '\n' {
		return fmt.Errorf("invalid chunk terminator, want \\r\\n, got %q%q", b1, b2)
	}
	return nil
}

// parseHexInt64 parses a hex string (no 0x prefix) into int64.
func parseHexInt64(s string) (int64, error) {
	var n int64
	if s == "" {
		return 0, fmt.Errorf("empty hex string")
	}
	for _, ch := range s {
		var v int64
		switch {
		case ch >= '0' && ch <= '9':
			v = int64(ch - '0')
		case ch >= 'a' && ch <= 'f':
			v = int64(ch-'a') + 10
		case ch >= 'A' && ch <= 'F':
			v = int64(ch-'A') + 10
		default:
			return 0, fmt.Errorf("invalid hex digit %q", ch)
		}
		n = (n << 4) | v
	}
	return n, nil
}

// NewHTTPBodyReader returns an io.Reader for net/http request bodies.
func NewHTTPBodyReader(r *http.Request) io.Reader {
	return r.Body
}
