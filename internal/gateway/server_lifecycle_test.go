package gateway

import (
	"context"
	"testing"

	"github.com/mulgadc/predastore/internal/gateway/auth"
	"github.com/stretchr/testify/assert"
)

func newLifecycleServer(t *testing.T) *HTTP2Server {
	t.Helper()
	config := &Config{Region: "us-east-1"}
	return NewHTTP2Server(config, Clients{}, auth.NewConfigProvider(nil))
}

func TestHTTP2Server_Shutdown_NilServer(t *testing.T) {
	server := newLifecycleServer(t)
	// server.server is nil (no ListenAndServe called)
	err := server.Shutdown(context.Background())
	assert.NoError(t, err)
}

func TestHTTP2Server_GetRouter(t *testing.T) {
	server := newLifecycleServer(t)
	assert.NotNil(t, server.GetRouter())
}

func TestHTTP2Server_GetHandler(t *testing.T) {
	server := newLifecycleServer(t)
	assert.NotNil(t, server.GetHandler())
}
