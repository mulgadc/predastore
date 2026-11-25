package handlers

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mismatchBody is the part of an S3 error body that reports a payload-hash mismatch.
type mismatchBody struct {
	Code     string `xml:"Code"`
	Client   string `xml:"ClientComputedContentSHA256"`
	Computed string `xml:"S3ComputedContentSHA256"`
}

func decodeMismatch(t *testing.T, rr *httptest.ResponseRecorder) mismatchBody {
	t.Helper()

	var body mismatchBody
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &body))

	return body
}

func hexSum(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// TestPutObjectStreamedMismatchReportsBothDigests sends a rewritten body too large to buffer
// through the PUT handler, so the mismatch surfaces from the write path's read of the body.
func TestPutObjectStreamedMismatchReportsBothDigests(t *testing.T) {
	f := newHandlerFixture("bucket")

	signed := bytes.Repeat([]byte("a"), sigv4.MaxPayloadLen+1)
	sent := bytes.Repeat([]byte("b"), len(signed))

	req := verifiedPut(t, signed, sent).WithContext(objectCtx("bucket", "object.txt"))
	rr := httptest.NewRecorder()
	PutObject(f.mc, f.bc, f.ring, f.cache, f.cfg).ServeHTTP(rr, req)

	require.Equal(t, http.StatusBadRequest, rr.Code)
	body := decodeMismatch(t, rr)
	assert.Equal(t, "XAmzContentSHA256Mismatch", body.Code)
	assert.Equal(t, hexSum(signed), body.Client)
	assert.Equal(t, hexSum(sent), body.Computed)
}

// TestErrorBodiesWithoutDigestsOmitThem keeps the digest elements off every other error,
// and off a bare mismatch sentinel that carries no digests.
func TestErrorBodiesWithoutDigestsOmitThem(t *testing.T) {
	for name, err := range map[string]error{
		"bare mismatch": sigv4.ErrContentSHA256Mismatch,
		"no such key":   model.ErrNoSuchKeyError,
	} {
		t.Run(name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			HandleError(rr, httptest.NewRequest(http.MethodPut, "/bucket/key", nil), err)

			assert.NotContains(t, rr.Body.String(), "ClientComputedContentSHA256")
			assert.NotContains(t, rr.Body.String(), "S3ComputedContentSHA256")
		})
	}
}
