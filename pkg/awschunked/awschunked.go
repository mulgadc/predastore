package awschunked

import (
	"io"

	"github.com/mulgadc/predastore/pkg/sigv4"
)

// The streaming content modes that frame an aws-chunked body. They are typed as
// sigv4.ContentMode; the non-streaming sigv4.UnsignedPayload sentinel lives alongside
// the type in pkg/sigv4, which needs it for the presigned-URL path.
const (
	// StreamingUnsignedPayloadTrailer frames the body as aws-chunked with a trailer but no
	// per-chunk signatures; only the trailer checksum protects the payload.
	StreamingUnsignedPayloadTrailer sigv4.ContentMode = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"

	// StreamingV4Payload frames the body as aws-chunked with a per-chunk signature chain
	// seeded by the request signature.
	StreamingV4Payload sigv4.ContentMode = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"

	// StreamingV4PayloadTrailer is StreamingV4Payload followed by a signed trailer chunk.
	StreamingV4PayloadTrailer sigv4.ContentMode = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
)

type Reader struct {
	r io.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		r: r,
	}
}
