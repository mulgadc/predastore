package gate

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/stretchr/testify/assert"
)

func TestGetObjectNoBucketPermissions(t *testing.T) {
	config := newAuthTestConfig()

	server := newTestGate(t, config)

	req := httptest.NewRequest(http.MethodGet, "/private/note.txt", nil)
	signTestReq(t, req, nil, "BADACCESSKEY", "BADSECRETKEY", config.Region, "s3")

	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")

	var s3error handlers.S3Error
	err := xml.Unmarshal(rr.Body.Bytes(), &s3error)
	assert.NoError(t, err, "XML parsing failed")
	assert.Equal(t, "InvalidAccessKeyId", s3error.Code, "Error message should indicate invalid access key")
}
