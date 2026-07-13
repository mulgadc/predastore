package awschunked

import (
	"io"

	"github.com/mulgadc/predastore/pkg/sigv4"
)

// Decoder decodes both unsigned and signed S3 chunked uploads, including
// trailing chunks. It implements io.Reader and streams out ONLY the real
// object bytes.
type Decoder struct {
}

func NewDecoder(req *sigv4.SignedRequest, body io.Reader) (*Decoder, error) {

}
