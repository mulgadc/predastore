package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testRegion  = "us-east-1"
	testService = "s3db"
	testAccess  = "TESTACCESSKEY"
	testSecret  = "TESTSECRETKEY"
)

// staticLookup returns a SecretLookup over a fixed credentials map.
func staticLookup(creds map[string]string) SecretLookup {
	return func(id string) (string, error) {
		s, ok := creds[id]
		if !ok {
			return "", fmt.Errorf("invalid access key: %s", id)
		}
		return s, nil
	}
}

// signRequestWithPayloadHash signs req committing to payloadHash in the
// canonical request without reading req.Body. Mirrors GenerateAuthHeaderReq
// but substitutes a precomputed payload hash so tests can exercise the
// X-Amz-Content-Sha256 streaming-bypass paths.
func signRequestWithPayloadHash(t *testing.T, req *http.Request, payloadHash, timestamp string) {
	t.Helper()
	date := timestamp[:8]
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-date:%s\n", host, timestamp)
	signedHeaders := "host;x-amz-date"
	canonicalURI := UriEncode(req.URL.Path, false)
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalRequest := fmt.Sprintf(
		"%s\n%s\n\n%s\n%s\n%s",
		req.Method,
		canonicalURI,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	)
	scope := fmt.Sprintf("%s/%s/%s/aws4_request", date, testRegion, testService)
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s", timestamp, scope, HashSHA256(canonicalRequest))
	key := GetSigningKey(testSecret, date, testRegion, testService)
	signature := HmacSHA256Hex(key, stringToSign)
	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s/%s/%s/aws4_request, SignedHeaders=%s, Signature=%s",
		testAccess, date, testRegion, testService, signedHeaders, signature,
	))
	req.Header.Set("X-Amz-Date", timestamp)
}

func TestHashSHA256(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		},
		{
			name:     "hello",
			input:    "hello",
			expected: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
		},
		{
			name:     "deterministic",
			input:    "test123",
			expected: HashSHA256("test123"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, HashSHA256(tt.input))
		})
	}
}

func TestHmacSHA256(t *testing.T) {
	key := []byte("secret")
	result := hmacSHA256(key, "data")
	assert.Len(t, result, 32, "HMAC-SHA256 should produce 32 bytes")

	// Same inputs should produce same output
	assert.Equal(t, result, hmacSHA256(key, "data"))

	// Different data should produce different output
	assert.NotEqual(t, result, hmacSHA256(key, "other"))

	// Different key should produce different output
	assert.NotEqual(t, result, hmacSHA256([]byte("other"), "data"))
}

func TestHmacSHA256Hex(t *testing.T) {
	key := []byte("secret")
	hexResult := HmacSHA256Hex(key, "data")

	// Should be 64 hex chars (32 bytes)
	assert.Len(t, hexResult, 64)
	assert.Equal(t, hexResult, "1b2c16b75bd2a870c114153ccda5bcfca63314bc722fa160d690de133ccbb9db")
}

func TestGetSigningKey(t *testing.T) {
	// AWS SigV4 key derivation: "AWS4" + secret → date → region → service → "aws4_request"
	key := GetSigningKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20150830", "us-east-1", "iam")
	assert.Len(t, key, 32)

	// Deterministic
	key2 := GetSigningKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20150830", "us-east-1", "iam")
	assert.Equal(t, key, key2)

	// Different inputs produce different keys
	key3 := GetSigningKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20150830", "us-west-2", "iam")
	assert.NotEqual(t, key, key3)

	key4 := GetSigningKey("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20150830", "us-east-1", "s3")
	assert.NotEqual(t, key, key4)
}

func TestUriEncode(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		encodeSlash bool
		expected    string
	}{
		{
			name:        "unreserved characters pass through",
			input:       "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~",
			encodeSlash: true,
			expected:    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~",
		},
		{
			name:        "slash not encoded when encodeSlash=false",
			input:       "/path/to/object",
			encodeSlash: false,
			expected:    "/path/to/object",
		},
		{
			name:        "slash encoded when encodeSlash=true",
			input:       "/path/to/object",
			encodeSlash: true,
			expected:    "%2Fpath%2Fto%2Fobject",
		},
		{
			name:        "spaces are encoded",
			input:       "hello world",
			encodeSlash: true,
			expected:    "hello%20world",
		},
		{
			name:        "special characters encoded",
			input:       "foo=bar&baz",
			encodeSlash: true,
			expected:    "foo%3Dbar%26baz",
		},
		{
			name:        "empty string",
			input:       "",
			encodeSlash: true,
			expected:    "",
		},
		{
			name:        "percent sign encoded",
			input:       "100%",
			encodeSlash: true,
			expected:    "100%25",
		},
		{
			name:        "plus sign encoded",
			input:       "a+b",
			encodeSlash: true,
			expected:    "a%2Bb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, UriEncode(tt.input, tt.encodeSlash))
		})
	}
}

func TestGenerateAuthHeaderReq(t *testing.T) {
	t.Run("GET request without body", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "https://s3.us-east-1.amazonaws.com/my-bucket?list-type=2", nil)
		err := GenerateAuthHeaderReq("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
			"20150830T123600Z", "us-east-1", "s3", req)

		require.NoError(t, err)

		authHeader := req.Header.Get("Authorization")
		assert.Contains(t, authHeader, "AWS4-HMAC-SHA256")
		assert.Contains(t, authHeader, "Credential=AKIAIOSFODNN7EXAMPLE/20150830/us-east-1/s3/aws4_request")
		assert.Contains(t, authHeader, "SignedHeaders=host;x-amz-date")
		assert.Contains(t, authHeader, "Signature=")
		assert.Equal(t, "20150830T123600Z", req.Header.Get("X-Amz-Date"))
	})

	t.Run("PUT request with body", func(t *testing.T) {
		body := []byte(`{"key": "value"}`)
		req, _ := http.NewRequest(http.MethodPut, "https://s3.us-east-1.amazonaws.com/my-bucket/my-key", bytes.NewReader(body))
		err := GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req)

		require.NoError(t, err)

		// Body should still be readable after signing
		readBody, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		assert.Equal(t, body, readBody)

		assert.Contains(t, req.Header.Get("Authorization"), "AWS4-HMAC-SHA256")
	})

	t.Run("request with explicit Host", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "https://s3.amazonaws.com/", nil)
		req.Host = "custom-host.example.com"
		err := GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req)

		require.NoError(t, err)
		assert.Contains(t, req.Header.Get("Authorization"), "Signature=")
	})

	t.Run("deterministic signatures", func(t *testing.T) {
		makeReq := func() *http.Request {
			r, _ := http.NewRequest(http.MethodGet, "https://s3.us-east-1.amazonaws.com/bucket/key", nil)
			return r
		}

		req1 := makeReq()
		req2 := makeReq()

		_ = GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req1)
		_ = GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req2)

		assert.Equal(t, req1.Header.Get("Authorization"), req2.Header.Get("Authorization"))
	})

	t.Run("empty path defaults to root", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "https://s3.amazonaws.com", nil)
		err := GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req)
		require.NoError(t, err)
		assert.Contains(t, req.Header.Get("Authorization"), "Signature=")
	})

	t.Run("query parameters are canonicalized", func(t *testing.T) {
		req1, _ := http.NewRequest(http.MethodGet, "https://s3.amazonaws.com/?b=2&a=1", nil)
		req2, _ := http.NewRequest(http.MethodGet, "https://s3.amazonaws.com/?a=1&b=2", nil)

		_ = GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req1)
		_ = GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req2)

		assert.Equal(t, req1.Header.Get("Authorization"), req2.Header.Get("Authorization"))
	})

	t.Run("path with special characters", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "https://s3.amazonaws.com/bucket/path+with spaces", nil)
		// URL-encode the path properly
		req.URL.Path = "/bucket/path+with spaces"
		err := GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req)
		require.NoError(t, err)
		assert.Contains(t, req.Header.Get("Authorization"), "Signature=")
	})
}

func TestRequireSignedHeaders(t *testing.T) {
	tests := []struct {
		name    string
		headers []string
		wantErr string
	}{
		{
			name:    "host and x-amz-date present",
			headers: []string{"host", "x-amz-date"},
			wantErr: "",
		},
		{
			name:    "host, x-amz-date, content-type present",
			headers: []string{"content-type", "host", "x-amz-date"},
			wantErr: "",
		},
		{
			name:    "case-insensitive Host accepted",
			headers: []string{"Host", "X-Amz-Date"},
			wantErr: "",
		},
		{
			name:    "leading/trailing whitespace tolerated",
			headers: []string{" host ", "\tx-amz-date\t"},
			wantErr: "",
		},
		{
			name:    "missing host",
			headers: []string{"x-amz-date"},
			wantErr: "host",
		},
		{
			name:    "missing x-amz-date",
			headers: []string{"host"},
			wantErr: "x-amz-date",
		},
		{
			name:    "both missing",
			headers: []string{"content-type"},
			wantErr: "host",
		},
		{
			name:    "empty list",
			headers: []string{},
			wantErr: "host",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := RequireSignedHeaders(tt.headers)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestTimeFormatConstants(t *testing.T) {
	assert.Equal(t, "20060102T150405Z", TimeFormat)
	assert.Equal(t, "20060102", ShortTimeFormat)
}

func TestGenerateAuthHeaderReq_URLHostFallback(t *testing.T) {
	// When req.Host is empty, should use req.URL.Host
	req := &http.Request{
		Method: http.MethodGet,
		URL: &url.URL{
			Scheme: "https",
			Host:   "url-host.example.com",
			Path:   "/",
		},
		Header: make(http.Header),
	}

	err := GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req)
	require.NoError(t, err)

	// Both should produce valid signatures
	auth := req.Header.Get("Authorization")
	assert.Contains(t, auth, "AWS4-HMAC-SHA256")
}

// TestVerifySigV4Request covers the verifier happy path plus the three
// structural rejection modes. Migrated from s3db.TestValidateSignatureHTTP.
func TestVerifySigV4Request(t *testing.T) {
	lookup := staticLookup(map[string]string{testAccess: testSecret})

	tests := []struct {
		name          string
		method        string
		path          string
		setup         func(req *http.Request)
		wantAccessKey string
		wantErr       error
	}{
		{
			name:   "Missing Authorization header",
			method: http.MethodGet,
			path:   "/v1/get/test-table/test-key",
			setup: func(req *http.Request) {
				req.Host = "localhost:6660"
			},
			wantErr: ErrMissingAuth,
		},
		{
			name:   "Valid signature",
			method: http.MethodGet,
			path:   "/v1/get/test-table/test-key",
			setup: func(req *http.Request) {
				req.Host = "localhost:6660"
				ts := time.Now().UTC().Format(TimeFormat)
				require.NoError(t, GenerateAuthHeaderReq(testAccess, testSecret, ts, testRegion, testService, req))
			},
			wantAccessKey: testAccess,
		},
		{
			name:   "Invalid access key",
			method: http.MethodGet,
			path:   "/v1/get/test-table/test-key",
			setup: func(req *http.Request) {
				req.Host = "localhost:6660"
				ts := time.Now().UTC().Format(TimeFormat)
				require.NoError(t, GenerateAuthHeaderReq("INVALIDACCESSKEY", testSecret, ts, testRegion, testService, req))
			},
			// SecretLookup returns its own error (not a sentinel).
		},
		{
			name:   "Invalid signature (wrong secret)",
			method: http.MethodGet,
			path:   "/v1/get/test-table/test-key",
			setup: func(req *http.Request) {
				req.Host = "localhost:6660"
				ts := time.Now().UTC().Format(TimeFormat)
				require.NoError(t, GenerateAuthHeaderReq(testAccess, "WRONGSECRETKEY", ts, testRegion, testService, req))
			},
			wantErr: ErrSignatureMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			tt.setup(req)

			gotKey, gotErr := VerifySigV4Request(req, testRegion, testService, lookup)

			if tt.wantAccessKey != "" {
				require.NoError(t, gotErr)
				assert.Equal(t, tt.wantAccessKey, gotKey)
				return
			}
			require.Error(t, gotErr)
			if tt.wantErr != nil {
				assert.ErrorIs(t, gotErr, tt.wantErr)
			}
		})
	}
}

// TestVerifySigV4Request_ClockSkew exercises the ±5 min clock-skew window.
// Captured Authorization headers must not be replayable indefinitely.
// Migrated from s3db.TestValidateSignatureHTTP_ReplayProtection.
func TestVerifySigV4Request_ClockSkew(t *testing.T) {
	lookup := staticLookup(map[string]string{testAccess: testSecret})

	tests := []struct {
		name    string
		setup   func(req *http.Request)
		wantErr error
	}{
		{
			name: "Stale X-Amz-Date (>5 min) rejected",
			setup: func(req *http.Request) {
				stale := time.Now().UTC().Add(-10 * time.Minute).Format(TimeFormat)
				require.NoError(t, GenerateAuthHeaderReq(testAccess, testSecret, stale, testRegion, testService, req))
			},
			wantErr: ErrClockSkew,
		},
		{
			name: "Future X-Amz-Date (>5 min) rejected",
			setup: func(req *http.Request) {
				future := time.Now().UTC().Add(10 * time.Minute).Format(TimeFormat)
				require.NoError(t, GenerateAuthHeaderReq(testAccess, testSecret, future, testRegion, testService, req))
			},
			wantErr: ErrClockSkew,
		},
		{
			name: "Malformed X-Amz-Date rejected",
			setup: func(req *http.Request) {
				ts := time.Now().UTC().Format(TimeFormat)
				require.NoError(t, GenerateAuthHeaderReq(testAccess, testSecret, ts, testRegion, testService, req))
				req.Header.Set("X-Amz-Date", "not-a-date")
			},
			wantErr: ErrInvalidDateFormat,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/get/test-table/test-key", nil)
			req.Host = "localhost:6660"
			tt.setup(req)

			_, err := VerifySigV4Request(req, testRegion, testService, lookup)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestVerifySigV4Request_RequireSignedHeaders verifies that requests
// whose SigV4 SignedHeaders list omits "host" or "x-amz-date" are rejected
// before signature comparison. AWS SDKs always sign both; omitting either
// would let a captured Authorization header replay against a different
// vhost or outside the X-Amz-Date skew window.
func TestVerifySigV4Request_RequireSignedHeaders(t *testing.T) {
	lookup := staticLookup(map[string]string{testAccess: testSecret})

	rewriteSignedHeaders := func(req *http.Request, newList string) {
		ah := req.Header.Get("Authorization")
		parts := strings.Split(ah, ", ")
		require.Len(t, parts, 3)
		parts[1] = "SignedHeaders=" + newList
		req.Header.Set("Authorization", strings.Join(parts, ", "))
	}

	tests := []struct {
		name       string
		signedList string
		wantSub    string
	}{
		{"missing host rejected", "x-amz-date", "host"},
		{"missing x-amz-date rejected", "host", "x-amz-date"},
		{"neither present rejected", "content-type", "host"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/get/test-table/test-key", nil)
			req.Host = "localhost:6660"
			ts := time.Now().UTC().Format(TimeFormat)
			require.NoError(t, GenerateAuthHeaderReq(testAccess, testSecret, ts, testRegion, testService, req))
			rewriteSignedHeaders(req, tt.signedList)

			_, err := VerifySigV4Request(req, testRegion, testService, lookup)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidAuthFormat)
			assert.Contains(t, err.Error(), tt.wantSub)
		})
	}
}

// TestVerifySigV4Request_BodyLimit exercises the io.LimitReader reject
// path: a body 1 byte over the verifyMaxBodySize cap returns ErrBodyTooLarge
// without buffering the rest. Replaces s3db.TestValidateSignatureHTTP_
// MaxBytesReader, which validated a now-obsolete caller responsibility
// (the verifier handles capping internally).
func TestVerifySigV4Request_BodyLimit(t *testing.T) {
	lookup := staticLookup(map[string]string{testAccess: testSecret})

	const overByOne = verifyMaxBodySize + 1
	body := bytes.Repeat([]byte("A"), overByOne)

	req := httptest.NewRequest(http.MethodPost, "/v1/put/test-table/test-key", bytes.NewReader(body))
	req.Host = "localhost:6660"
	ts := time.Now().UTC().Format(TimeFormat)
	require.NoError(t, GenerateAuthHeaderReq(testAccess, testSecret, ts, testRegion, testService, req))

	_, err := VerifySigV4Request(req, testRegion, testService, lookup)
	assert.ErrorIs(t, err, ErrBodyTooLarge)
}

// TestVerifySigV4Request_RegionMismatch verifies explicit region enforcement.
// The s3 middleware rejected mismatch cryptographically; s3db accepted
// whatever the client claimed. Both now reject explicitly with diagnostics.
func TestVerifySigV4Request_RegionMismatch(t *testing.T) {
	lookup := staticLookup(map[string]string{testAccess: testSecret})

	ts := time.Now().UTC().Format(TimeFormat)
	req := httptest.NewRequest(http.MethodGet, "/v1/get/test-table/test-key", nil)
	req.Host = "localhost:6660"
	require.NoError(t, GenerateAuthHeaderReq(testAccess, testSecret, ts, "ap-southeast-2", testService, req))

	_, err := VerifySigV4Request(req, testRegion, testService, lookup)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidCredScope)
	assert.Contains(t, err.Error(), "region")
	assert.Contains(t, err.Error(), "ap-southeast-2")
	assert.Contains(t, err.Error(), testRegion)
}

// TestVerifySigV4Request_ServiceMismatch verifies that a client claiming
// service=s3 against the s3db control plane (or vice versa) is rejected
// explicitly. Previously both verifiers silently accepted any service.
func TestVerifySigV4Request_ServiceMismatch(t *testing.T) {
	lookup := staticLookup(map[string]string{testAccess: testSecret})

	ts := time.Now().UTC().Format(TimeFormat)
	req := httptest.NewRequest(http.MethodGet, "/v1/get/test-table/test-key", nil)
	req.Host = "localhost:6660"
	require.NoError(t, GenerateAuthHeaderReq(testAccess, testSecret, ts, testRegion, "s3", req))

	_, err := VerifySigV4Request(req, testRegion, testService, lookup)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidCredScope)
	assert.Contains(t, err.Error(), "service")
}

// TestVerifySigV4Request_TypedMismatchError verifies the dual-mode error
// channel: errors.Is(err, ErrSignatureMismatch) for control flow, and
// errors.As(err, &*SigMismatchError) for rich diagnostic logging.
func TestVerifySigV4Request_TypedMismatchError(t *testing.T) {
	lookup := staticLookup(map[string]string{testAccess: testSecret})

	ts := time.Now().UTC().Format(TimeFormat)
	req := httptest.NewRequest(http.MethodGet, "/v1/get/test-table/test-key", nil)
	req.Host = "localhost:6660"
	require.NoError(t, GenerateAuthHeaderReq(testAccess, "WRONGSECRETKEY", ts, testRegion, testService, req))

	_, err := VerifySigV4Request(req, testRegion, testService, lookup)
	require.Error(t, err)

	assert.ErrorIs(t, err, ErrSignatureMismatch)

	var smErr *SigMismatchError
	require.True(t, errors.As(err, &smErr), "expected *SigMismatchError, got %T: %v", err, err)
	assert.Equal(t, testAccess, smErr.AccessKeyID)
	assert.NotEmpty(t, smErr.CanonicalRequest)
	assert.NotEmpty(t, smErr.StringToSign)
	assert.Len(t, smErr.ExpectedSigPrefix, 8)
	assert.Len(t, smErr.ProvidedSigPrefix, 8)
	assert.NotEqual(t, smErr.ExpectedSigPrefix, smErr.ProvidedSigPrefix)
}

// TestVerifySigV4Request_StreamingPayloadBypassesBodyRead verifies that
// when the client advertises a streaming payload indicator via
// X-Amz-Content-Sha256, the verifier does NOT buffer the body. Critical
// for multi-GB PutObject — buffering would force every large upload
// through the 10 MiB cap.
func TestVerifySigV4Request_StreamingPayloadBypassesBodyRead(t *testing.T) {
	lookup := staticLookup(map[string]string{testAccess: testSecret})

	// Body would exceed the cap if buffered. UNSIGNED-PAYLOAD must skip.
	const oversize = verifyMaxBodySize + 1
	body := bytes.NewReader(bytes.Repeat([]byte("A"), oversize))

	ts := time.Now().UTC().Format(TimeFormat)
	req := httptest.NewRequest(http.MethodPut, "/v1/put/test-table/test-key", body)
	req.Host = "localhost:6660"
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	signRequestWithPayloadHash(t, req, "UNSIGNED-PAYLOAD", ts)

	_, err := VerifySigV4Request(req, testRegion, testService, lookup)
	assert.NoError(t, err, "streaming payload must bypass body buffering")
}

// TestVerifySigV4Request_PrecomputedPayloadHash verifies that a
// client-supplied hex SHA-256 in X-Amz-Content-Sha256 is trusted (and
// that the verifier does not re-read the body). Integrity is still
// guaranteed: a lying client cannot produce a valid signature against
// the hash it lied about.
func TestVerifySigV4Request_PrecomputedPayloadHash(t *testing.T) {
	lookup := staticLookup(map[string]string{testAccess: testSecret})

	bodyBytes := []byte("hello world")
	sum := sha256.Sum256(bodyBytes)
	payloadHash := hex.EncodeToString(sum[:])

	ts := time.Now().UTC().Format(TimeFormat)
	req := httptest.NewRequest(http.MethodPut, "/v1/put/test-table/test-key", bytes.NewReader(bodyBytes))
	req.Host = "localhost:6660"
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	signRequestWithPayloadHash(t, req, payloadHash, ts)

	_, err := VerifySigV4Request(req, testRegion, testService, lookup)
	assert.NoError(t, err)
}

// TestVerifySigV4Request_BodyRestored verifies that downstream handlers
// see the original body after the verifier has read it for hashing.
// Otherwise every signed PutObject would receive an empty body.
func TestVerifySigV4Request_BodyRestored(t *testing.T) {
	lookup := staticLookup(map[string]string{testAccess: testSecret})

	body := []byte("downstream handler must see these bytes")
	req := httptest.NewRequest(http.MethodPut, "/v1/put/test-table/test-key", bytes.NewReader(body))
	req.Host = "localhost:6660"
	ts := time.Now().UTC().Format(TimeFormat)
	require.NoError(t, GenerateAuthHeaderReq(testAccess, testSecret, ts, testRegion, testService, req))

	_, err := VerifySigV4Request(req, testRegion, testService, lookup)
	require.NoError(t, err)

	got, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	assert.Equal(t, body, got)
}

// TestIsHexSHA256 covers the streaming-payload hash detector.
func TestIsHexSHA256(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"empty", "", false},
		{"too short", "abc123", false},
		{"valid lowercase hex SHA-256", strings.Repeat("a", 64), true},
		{"valid digits", strings.Repeat("0", 64), true},
		{"uppercase hex rejected", strings.Repeat("A", 64), false},
		{"non-hex char in valid length", "g" + strings.Repeat("0", 63), false},
		{"one too long", strings.Repeat("a", 65), false},
		{"one too short", strings.Repeat("a", 63), false},
		{"UNSIGNED-PAYLOAD literal", "UNSIGNED-PAYLOAD", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isHexSHA256(tt.in))
		})
	}
}
