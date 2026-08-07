package s3

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/s3/chunked"
)

const maxChunkedFramingBytes int64 = 1 << 20

type decodedRequestBody struct {
	Reader io.Reader
	Length int64
}

// decodeObjectBody validates the authoritative plaintext size, bounds the HTTP
// body, and removes aws-chunked framing exactly once at S3 ingress.
func decodeObjectBody(w http.ResponseWriter, r *http.Request, maxDecoded int64) (decodedRequestBody, error) {
	if maxDecoded < 0 {
		return decodedRequestBody{}, fmt.Errorf("max decoded body size must be non-negative")
	}

	if strings.EqualFold(r.Header.Get("Content-Encoding"), "aws-chunked") {
		decodedLength, err := strconv.ParseInt(r.Header.Get("X-Amz-Decoded-Content-Length"), 10, 64)
		if err != nil || decodedLength < 0 {
			return decodedRequestBody{}, backend.NewS3Error(
				backend.ErrInvalidRequest,
				"X-Amz-Decoded-Content-Length must be a non-negative integer",
				http.StatusBadRequest,
			)
		}
		if decodedLength > maxDecoded {
			return decodedRequestBody{}, backend.NewS3Error(
				backend.ErrEntityTooLarge,
				"Decoded request body exceeds the maximum supported size",
				http.StatusBadRequest,
			)
		}

		// AWS signing chunks add little overhead at normal chunk sizes. Bound it
		// independently so a malicious stream of tiny framing chunks cannot make
		// the HTTP server consume an unbounded body for a small decoded object.
		wireLimit := decodedLength + decodedLength/4 + maxChunkedFramingBytes
		r.Body = http.MaxBytesReader(w, r.Body, wireLimit)
		return decodedRequestBody{
			Reader: chunked.NewDecoder(r.Body, decodedLength),
			Length: decodedLength,
		}, nil
	}

	if r.ContentLength < 0 {
		return decodedRequestBody{}, backend.NewS3Error(
			backend.ErrInvalidRequest,
			"Content-Length is required",
			http.StatusLengthRequired,
		)
	}
	if r.ContentLength > maxDecoded {
		return decodedRequestBody{}, backend.NewS3Error(
			backend.ErrEntityTooLarge,
			"Request body exceeds the maximum supported size",
			http.StatusBadRequest,
		)
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxDecoded)
	return decodedRequestBody{Reader: r.Body, Length: r.ContentLength}, nil
}
