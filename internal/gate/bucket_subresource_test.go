package gate

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// serveBucketSubResource drives one bucket sub-resource request through the
// whole gate, signed, so the answer under test is the one a client receives.
func serveBucketSubResource(t *testing.T, method, target string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	config := newAuthTestConfig()
	server := newTestGate(t, config)

	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	signTestReq(t, req, body, "AKIAIOSFODNN7EXAMPLE",
		"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", config.Region, "s3")

	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	return rr
}

// Reading a bucket control predastore does not implement answers the code S3
// answers for a control that is simply not configured. A client cannot tell the
// two apart and does not need to: nothing is set, and nothing can set it. The
// Terraform AWS provider reads these during refresh and takes the code as
// "absent", where a 501 is an error that fails the plan.
func TestBucketControlReadAnswersTheUnsetCode(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target string
		code   string
	}{
		{"encryption", "/local?encryption", "ServerSideEncryptionConfigurationNotFoundError"},
		{"lifecycle", "/local?lifecycle", "NoSuchLifecycleConfiguration"},
		{"policy", "/local?policy", "NoSuchBucketPolicy"},
		{"ownershipControls", "/local?ownershipControls", "OwnershipControlsNotFoundError"},
		{"cors", "/local?cors", "NoSuchCORSConfiguration"},
		{"publicAccessBlock", "/local?publicAccessBlock", "NoSuchPublicAccessBlockConfiguration"},
		{"object-lock", "/local?object-lock", "ObjectLockConfigurationNotFoundError"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := serveBucketSubResource(t, http.MethodGet, tc.target, nil)

			require.Equal(t, http.StatusNotFound, rr.Code, "body: %s", rr.Body.String())
			assert.Contains(t, rr.Body.String(), tc.code)
		})
	}
}

// A control AWS answers with a populated 200 when it is unset gets no
// synthesised success here, because that would claim a control predastore does
// not have. This is the fail-open GetBucketOwnershipControls used to be, kept
// closed for the rest.
func TestBucketControlReadWithNoUnsetCodeStaysNotImplemented(t *testing.T) {
	for _, target := range []string{
		"/local?acl", "/local?notification", "/local?logging",
		"/local?accelerate", "/local?requestPayment",
	} {
		t.Run(target, func(t *testing.T) {
			rr := serveBucketSubResource(t, http.MethodGet, target, nil)

			require.Equal(t, http.StatusNotImplemented, rr.Code, "body: %s", rr.Body.String())
			assert.Contains(t, rr.Body.String(), "NotImplemented")
		})
	}
}

// Writing an unimplemented control is refused whatever its read answers. A
// not-found code on a write would tell a caller the write may yet succeed.
func TestBucketControlWriteStaysNotImplemented(t *testing.T) {
	for _, tc := range []struct {
		name   string
		method string
		target string
		body   []byte
	}{
		{"put encryption", http.MethodPut, "/local?encryption",
			[]byte("<ServerSideEncryptionConfiguration></ServerSideEncryptionConfiguration>")},
		{"put lifecycle", http.MethodPut, "/local?lifecycle",
			[]byte("<LifecycleConfiguration></LifecycleConfiguration>")},
		{"put policy", http.MethodPut, "/local?policy", []byte(`{"Statement":[]}`)},
		{"put ownershipControls", http.MethodPut, "/local?ownershipControls",
			[]byte("<OwnershipControls></OwnershipControls>")},
		{"delete policy", http.MethodDelete, "/local?policy", nil},
		{"delete lifecycle", http.MethodDelete, "/local?lifecycle", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := serveBucketSubResource(t, tc.method, tc.target, tc.body)

			require.Equal(t, http.StatusNotImplemented, rr.Code, "body: %s", rr.Body.String())
			assert.Contains(t, rr.Body.String(), "NotImplemented")
		})
	}
}

// An ordinary listing must still list. The SDKs append parameters of their own
// to it, and a fallthrough that rejected anything it did not recognise would
// turn a working listing into an error.
func TestOrdinaryListingIsNotTreatedAsASubResource(t *testing.T) {
	for _, target := range []string{
		"/local", "/local?list-type=2", "/local?list-type=2&x-id=ListObjectsV2",
		"/local?prefix=a/&delimiter=/", "/local?max-keys=10",
	} {
		t.Run(target, func(t *testing.T) {
			rr := serveBucketSubResource(t, http.MethodGet, target, nil)

			assert.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
		})
	}
}
