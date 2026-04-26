package s3

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/predastore/auth"
	"github.com/stretchr/testify/assert"
)

func TestDeleteObjectNoAuth(t *testing.T) {
	config := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := config.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	server := NewHTTP2ServerWithBackend(config, nil, NewConfigProvider(config.Auth))

	// Send a delete request
	req := httptest.NewRequest(http.MethodDelete, "/local/unknownfile.txt", nil)

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")
}

func TestDeleteObjectBadAuth(t *testing.T) {
	config := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := config.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	server := NewHTTP2ServerWithBackend(config, nil, NewConfigProvider(config.Auth))

	// Send a delete request
	req := httptest.NewRequest(http.MethodDelete, "/local/unknownfile.txt", nil)

	// Use our utility function to generate a valid authorization header
	timestamp := time.Now().UTC().Format("20060102T150405Z")

	err = auth.GenerateAuthHeaderReq("BADACCESSKEY", "BADSECRETKEY", timestamp, config.Region, "s3", req)
	assert.NoError(t, err, "Error generating auth header")

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")
}
