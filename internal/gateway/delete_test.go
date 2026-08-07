package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/predastore/internal/gateway/auth"
	"github.com/stretchr/testify/assert"
)

func TestDeleteObjectNoAuth(t *testing.T) {
	config := newAuthTestConfig()

	server := NewHandler(config, Clients{}, auth.NewConfigProvider(config.Auth))

	// Send a delete request
	req := httptest.NewRequest(http.MethodDelete, "/local/unknownfile.txt", nil)

	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")
}

func TestDeleteObjectBadAuth(t *testing.T) {
	config := newAuthTestConfig()

	server := NewHandler(config, Clients{}, auth.NewConfigProvider(config.Auth))

	// Send a delete request
	req := httptest.NewRequest(http.MethodDelete, "/local/unknownfile.txt", nil)

	// Sign with bad credentials to exercise the InvalidAccessKeyId path.
	signTestReq(t, req, nil, "BADACCESSKEY", "BADSECRETKEY", config.Region, "s3")

	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")
}
