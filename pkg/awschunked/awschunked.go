package awschunked

import (
	"io"

	"github.com/mulgadc/predastore/pkg/sigv4"
)

type Reader struct {
	r   io.Reader
	err error
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		r: r,
	}
}
