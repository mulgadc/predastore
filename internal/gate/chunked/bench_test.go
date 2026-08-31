package chunked

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"testing"
)

// benchChunkSize is what a client frames with. aws-cli and the SDKs pick 64 KiB
// unless the object forces a larger part, so the per-chunk costs — a header
// parse, a hash finalise and an HMAC — are paid roughly this often.
const benchChunkSize = 64 << 10

// buildBenchBody frames payload in benchChunkSize chunks, with signatures when
// signed. Building the body outside the timer keeps the client's own signing
// out of a measurement of the server's verification.
func buildBenchBody(payload []byte, signed bool) string {
	var buf strings.Builder
	prev := testSeed

	write := func(chunk []byte) {
		if !signed {
			fmt.Fprintf(&buf, "%x\r\n%s\r\n", len(chunk), chunk)
			return
		}
		sum := sha256.Sum256(chunk)
		sts := strings.Join([]string{
			chunkSTSPrefix, testTimestamp, testScope, prev,
			emptySHA256, hex.EncodeToString(sum[:]),
		}, "\n")
		prev = hex.EncodeToString(hmacSHA256(testKey, sts))
		fmt.Fprintf(&buf, "%x;chunk-signature=%s\r\n%s\r\n", len(chunk), prev, chunk)
	}

	for off := 0; off < len(payload); off += benchChunkSize {
		end := min(off+benchChunkSize, len(payload))
		write(payload[off:end])
	}
	write(nil)

	// The terminating chunk's own framing differs: no data, no trailing CRLF.
	body := buf.String()
	return strings.TrimSuffix(body, "\r\n") + "\r\n"
}

// BenchmarkDecoder measures what verifying a framed body costs. unsigned is
// STREAMING-UNSIGNED-PAYLOAD-TRAILER, which is what the default aws-cli path
// sends; signed adds the chunk signature chain, so the difference between them
// is the price of authenticating a body that was previously trusted.
func BenchmarkDecoder(b *testing.B) {
	for _, size := range []struct {
		name string
		n    int
	}{
		{"1MiB", 1 << 20},
		{"8MiB", 8 << 20},
	} {
		payload := make([]byte, size.n)
		for i := range payload {
			payload[i] = byte(i)
		}

		for _, mode := range []struct {
			name   string
			signed bool
		}{
			{"unsigned", false},
			{"signed", true},
		} {
			body := buildBenchBody(payload, mode.signed)

			b.Run(size.name+"/"+mode.name, func(b *testing.B) {
				b.SetBytes(int64(size.n))
				b.ReportAllocs()

				for b.Loop() {
					var opts []Option
					if mode.signed {
						opts = append(opts, WithChain(newTestChain()))
					}
					dec := NewDecoder(strings.NewReader(body), int64(size.n), opts...)
					if _, err := io.Copy(io.Discard, dec); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
