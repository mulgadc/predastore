package gate

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An object sub-resource request must be refused before it reaches a handler.
// The fallback route for each of these methods acts on the object itself, so a
// request that fell through wrote the sub-resource document as the object body
// on PUT, and removed the object on DELETE.
func TestObjectSubResourcesAreRefusedThroughTheGate(t *testing.T) {
	config := newAuthTestConfig()

	for _, tc := range []struct {
		name   string
		method string
		target string
		body   []byte
	}{
		{"put tagging", http.MethodPut, "/local/doc.txt?tagging",
			[]byte("<Tagging><TagSet><Tag><Key>a</Key><Value>1</Value></Tag></TagSet></Tagging>")},
		{"put retention", http.MethodPut, "/local/doc.txt?retention",
			[]byte("<Retention><Mode>GOVERNANCE</Mode></Retention>")},
		{"put legal hold", http.MethodPut, "/local/doc.txt?legal-hold",
			[]byte("<LegalHold><Status>ON</Status></LegalHold>")},
		{"get tagging", http.MethodGet, "/local/doc.txt?tagging", nil},
		{"delete tagging", http.MethodDelete, "/local/doc.txt?tagging", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newTestGate(t, config)

			req := httptest.NewRequest(tc.method, tc.target, bytes.NewReader(tc.body))
			signTestReq(t, req, tc.body, "AKIAIOSFODNN7EXAMPLE",
				"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", config.Region, "s3")

			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			require.Equal(t, http.StatusNotImplemented, rr.Code, "body: %s", rr.Body.String())
			assert.Contains(t, rr.Body.String(), "NotImplemented")
		})
	}
}
