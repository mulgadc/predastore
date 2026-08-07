package s3

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3RequestOperation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		method string
		target string
		want   string
	}{
		{http.MethodPut, "/bucket/key", "s3:PutObject"},
		{http.MethodGet, "/bucket/key", "s3:GetObject"},
		{http.MethodPost, "/bucket/key?uploads", "CreateMultipartUpload"},
		{http.MethodPut, "/bucket/key?partNumber=3&uploadId=id", "UploadPart"},
		{http.MethodPost, "/bucket/key?uploadId=id", "CompleteMultipartUpload"},
		{http.MethodDelete, "/bucket/key?uploadId=id", "AbortMultipartUpload"},
	}
	for _, test := range tests {
		t.Run(test.want, func(t *testing.T) {
			r := httptest.NewRequest(test.method, test.target, nil)
			assert.Equal(t, test.want, s3RequestOperation(r))
		})
	}
}

func TestS3AccessLogMiddleware(t *testing.T) {
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	handler := s3AccessLogMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, err := io.WriteString(w, "ok")
		require.NoError(t, err)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPut,
		"/bucket/key?partNumber=1&uploadId=secret", bytes.NewReader([]byte("part"))))

	var entry map[string]any
	require.NoError(t, json.Unmarshal(output.Bytes(), &entry))
	assert.Equal(t, "S3 request", entry["msg"])
	assert.Equal(t, "UploadPart", entry["operation"])
	assert.Equal(t, "bucket", entry["bucket"])
	assert.Equal(t, "key", entry["key"])
	assert.Equal(t, float64(http.StatusCreated), entry["status"])
	assert.Equal(t, float64(4), entry["request_bytes"])
	assert.Equal(t, float64(2), entry["response_bytes"])
	assert.NotContains(t, output.String(), "secret")
	assert.NotNil(t, entry["duration_us"])
}
