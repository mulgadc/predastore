package s3

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteObjectNoAuth(t *testing.T) {
	config := newAuthTestConfig()

	server := NewHTTP2ServerWithBackend(config, nil, NewConfigProvider(config.Auth))

	// Send a delete request
	req := httptest.NewRequest(http.MethodDelete, "/local/unknownfile.txt", nil)

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")
}

func TestDeleteObjectBadAuth(t *testing.T) {
	config := newAuthTestConfig()

	server := NewHTTP2ServerWithBackend(config, nil, NewConfigProvider(config.Auth))

	// Send a delete request
	req := httptest.NewRequest(http.MethodDelete, "/local/unknownfile.txt", nil)

	// Sign with bad credentials to exercise the InvalidAccessKeyId path.
	signTestReq(t, req, nil, "BADACCESSKEY", "BADSECRETKEY", config.Region, "s3")

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")
}
