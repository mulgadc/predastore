package s3

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mulgadc/predastore/backend"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeObjectBody(t *testing.T) {
	t.Run("plain body", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPut, "/", strings.NewReader("payload"))
		body, err := decodeObjectBody(httptest.NewRecorder(), r, 100)
		require.NoError(t, err)
		assert.Equal(t, int64(7), body.Length)
		got, err := io.ReadAll(body.Reader)
		require.NoError(t, err)
		assert.Equal(t, "payload", string(got))
	})

	t.Run("aws chunked body", func(t *testing.T) {
		wire := "7\r\npayload\r\n0\r\n\r\n"
		r := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(wire))
		r.Header.Set("Content-Encoding", "aws-chunked")
		r.Header.Set("X-Amz-Decoded-Content-Length", "7")
		body, err := decodeObjectBody(httptest.NewRecorder(), r, 100)
		require.NoError(t, err)
		got, err := io.ReadAll(body.Reader)
		require.NoError(t, err)
		assert.Equal(t, "payload", string(got))
	})

	t.Run("missing decoded length", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(""))
		r.Header.Set("Content-Encoding", "aws-chunked")
		_, err := decodeObjectBody(httptest.NewRecorder(), r, 100)
		s3Err, ok := backend.IsS3Error(err)
		require.True(t, ok)
		assert.Equal(t, backend.ErrInvalidRequest, s3Err.Code)
	})

	t.Run("too large", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPut, "/", strings.NewReader("payload"))
		_, err := decodeObjectBody(httptest.NewRecorder(), r, 3)
		s3Err, ok := backend.IsS3Error(err)
		require.True(t, ok)
		assert.Equal(t, backend.ErrEntityTooLarge, s3Err.Code)
	})

	t.Run("unknown length", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPut, "/", strings.NewReader("payload"))
		r.ContentLength = -1
		_, err := decodeObjectBody(httptest.NewRecorder(), r, 100)
		s3Err, ok := backend.IsS3Error(err)
		require.True(t, ok)
		assert.Equal(t, http.StatusLengthRequired, s3Err.StatusCode)
	})
}
