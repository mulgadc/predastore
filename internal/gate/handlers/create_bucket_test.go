package handlers

import (
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errBody fails every read, standing in for the SigV4-verified body that reports a rewritten
// payload to whoever reads it.
type errBody struct{ err error }

func (b errBody) Read([]byte) (int, error) { return 0, b.err }
func (b errBody) Close() error             { return nil }

// TestCreateBucketRejectsUnreadableConfiguration covers the read that carries the payload
// check. Discarding its error created the bucket under a location constraint that failed its
// signed digest, or under none at all when the body never arrived.
func TestCreateBucketRejectsUnreadableConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		code string
	}{
		{name: "payload digest mismatch", err: sigv4.ErrContentSHA256Mismatch, code: "XAmzContentSHA256Mismatch"},
		{name: "read failure", err: errors.New("connection reset by peer"), code: "MalformedXML"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPut, "/"+testBucket, errBody{err: tc.err})
			req.ContentLength = 128
			req = req.WithContext(WithBucket(req.Context(), model.Bucket{Name: testBucket}))
			w := httptest.NewRecorder()

			// The nil MetaClient is the assertion: reaching the store at all means the
			// handler carried on past a body it could not read.
			CreateBucket(nil, testCache(), Config{Region: "ap-southeast-2"}).ServeHTTP(w, req)

			require.Equal(t, http.StatusBadRequest, w.Code, "body: %s", w.Body.String())

			var s3err S3Error
			require.NoError(t, xml.NewDecoder(w.Body).Decode(&s3err))
			assert.Equal(t, tc.code, s3err.Code)
		})
	}
}
