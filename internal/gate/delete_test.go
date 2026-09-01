package gate

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteObjectNoAuth(t *testing.T) {
	config := newAuthTestConfig()

	server := newTestGate(t, config)

	// Send a delete request
	req := httptest.NewRequest(http.MethodDelete, "/local/unknownfile.txt", nil)

	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")
}

// The batch delete is selected by ?delete alone. A POST at a bucket without it
// is not an operation predastore serves, and must not fall through to one.
func TestPostBucketWithoutDeleteIsNotAllowed(t *testing.T) {
	config := newAuthTestConfig()
	server := newTestGate(t, config)

	req := httptest.NewRequest(http.MethodPost, "/local", nil)
	signTestReq(t, req, nil, "AKIAIOSFODNN7EXAMPLE",
		"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", config.Region, "s3")

	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rr.Code, "body: %s", rr.Body.String())
}

func TestDeleteObjectsRoutesThroughTheGate(t *testing.T) {
	config := newAuthTestConfig()
	server := newTestGate(t, config)

	body := []byte("<Delete><Object><Key>gone.txt</Key></Object></Delete>")
	req := httptest.NewRequest(http.MethodPost, "/local?delete=", bytes.NewReader(body))
	signTestReq(t, req, body, "AKIAIOSFODNN7EXAMPLE",
		"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", config.Region, "s3")

	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "<DeleteResult>")
	assert.Contains(t, rr.Body.String(), "gone.txt")
}

func TestDeleteObjectBadAuth(t *testing.T) {
	config := newAuthTestConfig()

	server := newTestGate(t, config)

	// Send a delete request
	req := httptest.NewRequest(http.MethodDelete, "/local/unknownfile.txt", nil)

	// Sign with bad credentials to exercise the InvalidAccessKeyId path.
	signTestReq(t, req, nil, "BADACCESSKEY", "BADSECRETKEY", config.Region, "s3")

	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")
}
