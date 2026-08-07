package gateway

import (
	"context"
	"testing"

	"github.com/mulgadc/predastore/internal/gateway/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServer_Run_RequiresTLS asserts the gateway refuses to serve in the clear
// rather than falling back to plaintext HTTP.
func TestServer_Run_RequiresTLS(t *testing.T) {
	s := newGateway(&Config{Region: "us-east-1"}, nil, nil, auth.NewConfigProvider(nil))

	err := s.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS is required")
}

// TestServer_Run_BadCertificate asserts a broken TLS pair fails Run rather than
// leaving a half-started listener behind.
func TestServer_Run_BadCertificate(t *testing.T) {
	s := newGateway(&Config{Region: "us-east-1"}, nil, nil, auth.NewConfigProvider(nil))
	s.cfg = ServerConfig{
		Host:    "127.0.0.1",
		Port:    0,
		TLSCert: "testdata/test-bucket01/test.txt",
		TLSKey:  "testdata/test-bucket01/test.txt",
	}

	err := s.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS certificate")
}

// TestNewHandler builds the routing surface without a listener: the seam
// httptest-driven handler tests use.
func TestNewHandler(t *testing.T) {
	h := NewHandler(&Config{Region: "us-east-1"}, Clients{}, auth.NewConfigProvider(nil))
	assert.NotNil(t, h)
}
