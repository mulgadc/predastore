package s3

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5/middleware"
)

// s3AccessLogMiddleware records one concise entry for every S3 request at the
// normal info level. It intentionally does not log the raw query string: a
// presigned request contains credentials in its query parameters.
func s3AccessLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		wrapped := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(wrapped, r)

		status := wrapped.Status()
		if status == 0 {
			status = http.StatusOK
		}
		duration := time.Since(started)
		bucket, key := parseS3Path(r.URL.Path)
		slog.Info("S3 request",
			"method", r.Method,
			"operation", s3RequestOperation(r),
			"bucket", bucket,
			"key", key,
			"status", status,
			"request_bytes", r.ContentLength,
			"response_bytes", wrapped.BytesWritten(),
			"duration", duration,
			"duration_us", duration.Microseconds(),
		)
	})
}

func s3RequestOperation(r *http.Request) string {
	query := r.URL.Query()
	switch {
	case r.Method == http.MethodPost && query.Has("uploads"):
		return "CreateMultipartUpload"
	case r.Method == http.MethodPut && query.Get("uploadId") != "" && query.Get("partNumber") != "":
		if _, err := strconv.Atoi(query.Get("partNumber")); err == nil {
			return "UploadPart"
		}
	case r.Method == http.MethodPost && query.Get("uploadId") != "":
		return "CompleteMultipartUpload"
	case r.Method == http.MethodDelete && query.Get("uploadId") != "":
		return "AbortMultipartUpload"
	}
	return s3Action(r.Method, r.URL.Path)
}
