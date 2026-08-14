// What the gate offers over ALPN. HTTP/2 is off unless the config asks for
// it, because a co-located block-storage client multiplexes every request onto
// one connection, where a 4 MiB chunk PUT exhausts the connection's
// flow-control window and stalls the ranged GETs sharing it.
package gate

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestALPNOffersHTTP11OnlyByDefault(t *testing.T) {
	require.Equal(t, []string{"http/1.1"}, alpnProtocols(false))
	require.Equal(t, []string{"h2", "http/1.1"}, alpnProtocols(true))
}

// The two must agree: Serve installs the h2 handler whenever TLSConfig
// mentions h2, so an ALPN list and a Protocols set that disagree would leave
// the gate advertising a protocol it is not wired for, or the reverse.
func TestHTTPProtocolsTrackALPN(t *testing.T) {
	off := httpProtocols(false)
	require.True(t, off.HTTP1())
	require.False(t, off.HTTP2())
	require.False(t, off.UnencryptedHTTP2())

	on := httpProtocols(true)
	require.True(t, on.HTTP1())
	require.True(t, on.HTTP2())
}

// negotiatedWith serves a TLS listener using the gate's exact protocol values
// and reports what an h2-capable client ends up speaking.
func negotiatedWith(t *testing.T, enableHTTP2 bool) string {
	t.Helper()

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.Proto))
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	// Restart the handler under the gate's own settings rather than httptest's.
	srv.Config.Protocols = httpProtocols(enableHTTP2)
	srv.TLS.NextProtos = alpnProtocols(enableHTTP2)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:    srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs,
			NextProtos: []string{"h2", "http/1.1"},
			MinVersion: tls.VersionTLS13,
		},
		ForceAttemptHTTP2: true,
	}
	resp, err := (&http.Client{Transport: tr}).Get(srv.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	buf := make([]byte, 32)
	n, _ := resp.Body.Read(buf)
	return string(buf[:n])
}

// An h2-capable client must fall back on its own when the gate stays quiet
// about h2 — that fallback is the whole mechanism, so assert it end to end.
func TestClientFallsBackToHTTP11WhenGateDoesNotOfferH2(t *testing.T) {
	require.Equal(t, "HTTP/1.1", negotiatedWith(t, false))
}

func TestClientGetsHTTP2WhenGateOffersIt(t *testing.T) {
	require.Equal(t, "HTTP/2.0", negotiatedWith(t, true))
}

// The gate must build either way; the default itself is the config package's
// business, asserted in its own test.
func TestGateBuildsWithEitherProtocolSetting(t *testing.T) {
	for _, enable := range []bool{false, true} {
		cfg := wired(t)
		cfg.EnableHTTP2 = enable

		s, err := New(cfg)
		require.NoError(t, err)
		require.Equal(t, enable, s.cfg.EnableHTTP2)
	}
}
