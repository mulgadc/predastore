package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientAddrTrustsXRealIPOnlyFromLoopback(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, remote, header, want string
	}{
		{"loopback v4 proxied", "127.0.0.1:51234", "203.0.113.7", "203.0.113.7"},
		{"loopback v6 proxied", "[::1]:51234", "2001:db8::7", "2001:db8::7"},
		{"loopback header padded", "127.0.0.1:51234", " 203.0.113.7 ", "203.0.113.7"},
		{"loopback without header", "127.0.0.1:51234", "", "127.0.0.1:51234"},
		{"loopback with a header that is not an address", "127.0.0.1:51234", "evil\nline", "127.0.0.1:51234"},
		{"direct client forging the header", "198.51.100.9:443", "10.0.0.1", "198.51.100.9:443"},
		{"direct client without header", "198.51.100.9:443", "", "198.51.100.9:443"},
		{"remote address without a port", "198.51.100.9", "10.0.0.1", "198.51.100.9"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
			r.RemoteAddr = tc.remote
			if tc.header != "" {
				r.Header.Set("X-Real-IP", tc.header)
			}
			assert.Equal(t, tc.want, ClientAddr(r))
		})
	}
}
