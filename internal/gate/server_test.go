package gate

import (
	"testing"

	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wired is a Config New accepts, which each test below then breaks one field
// of. The TLS pair is generated because New loads it.
func wired(t *testing.T) Config {
	t.Helper()
	cert, key, _ := testcerts.Generate(t)
	return Config{
		Region:  "ap-southeast-2",
		TLSCert: cert,
		TLSKey:  key,
		Meta:    newFakeMeta(t),
		Blob:    fakeBlob{},
	}
}

// TestNew_MissingConfig verifies New refuses to run on zero values rather than
// serving an empty region with no buckets.
func TestNew_MissingConfig(t *testing.T) {
	_, err := New(Config{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "region is required")
}

// The cluster clients are the gate's only way to reach state and shards, so a
// gate without them must fail to build rather than 500 on every request.
func TestNew_MissingClients(t *testing.T) {
	cfg := wired(t)
	cfg.Meta = nil
	_, err := New(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no metadata client")

	cfg = wired(t)
	cfg.Blob = nil
	_, err = New(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no blob client")
}

// TestNew_RequiresTLS asserts the gate refuses to build in the clear rather
// than falling back to plaintext HTTP.
func TestNew_RequiresTLS(t *testing.T) {
	cfg := wired(t)
	cfg.TLSCert, cfg.TLSKey = "", ""
	_, err := New(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS is required")
}

// TestNew_BadCertificate asserts a broken TLS pair fails construction rather
// than surviving to Run and dying with the listener half up.
func TestNew_BadCertificate(t *testing.T) {
	cfg := wired(t)
	cfg.TLSCert = "testdata/test-bucket01/test.txt"
	cfg.TLSKey = "testdata/test-bucket01/test.txt"
	_, err := New(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS certificate")
}

// TestNew_BuildsRouter builds the routing surface without a listener: the seam
// the httptest-driven handler tests drive.
func TestNew_BuildsRouter(t *testing.T) {
	s, err := New(wired(t))
	require.NoError(t, err)
	assert.NotNil(t, s.router)
}
