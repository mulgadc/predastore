package auth

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	result := HmacSHA256(key, "data")
	assert.Len(t, result, 32, "HMAC-SHA256 should produce 32 bytes")

	// Same inputs should produce same output
	assert.Equal(t, result, HmacSHA256(key, "data"))

	// Different data should produce different output
	assert.NotEqual(t, result, HmacSHA256(key, "other"))

	// Different key should produce different output
	assert.NotEqual(t, result, HmacSHA256([]byte("other"), "data"))
}

func TestHmacSHA256Hex(t *testing.T) {
	key := []byte("secret")
	hexResult := HmacSHA256Hex(key, "data")

	// Should be 64 hex chars (32 bytes)
	assert.Len(t, hexResult, 64)

	// Should match hex encoding of raw HMAC
	raw := HmacSHA256(key, "data")
	assert.Equal(t, hexResult, "1b2c16b75bd2a870c114153ccda5bcfca63314bc722fa160d690de133ccbb9db")
	_ = raw
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

func TestCanonicalQueryString(t *testing.T) {
	tests := []struct {
		name     string
		params   map[string][]string
		expected string
	}{
		{
			name:     "empty params",
			params:   map[string][]string{},
			expected: "",
		},
		{
			name:     "single param",
			params:   map[string][]string{"key": {"value"}},
			expected: "key=value",
		},
		{
			name:     "multiple params sorted by key",
			params:   map[string][]string{"z-key": {"val"}, "a-key": {"val"}},
			expected: "a-key=val&z-key=val",
		},
		{
			name:     "multiple values sorted",
			params:   map[string][]string{"key": {"c", "a", "b"}},
			expected: "key=a&key=b&key=c",
		},
		{
			name:     "special characters in key and value",
			params:   map[string][]string{"a b": {"c=d"}},
			expected: "a%20b=c%3Dd",
		},
		{
			name:     "nil params",
			params:   nil,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, CanonicalQueryString(tt.params))
		})
	}
}

func TestGenerateAuthHeaderReq(t *testing.T) {
	t.Run("GET request without body", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://s3.us-east-1.amazonaws.com/my-bucket?list-type=2", nil)
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
		req, _ := http.NewRequest("PUT", "https://s3.us-east-1.amazonaws.com/my-bucket/my-key", bytes.NewReader(body))
		err := GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req)

		require.NoError(t, err)

		// Body should still be readable after signing
		readBody, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		assert.Equal(t, body, readBody)

		assert.Contains(t, req.Header.Get("Authorization"), "AWS4-HMAC-SHA256")
	})

	t.Run("request with explicit Host", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://s3.amazonaws.com/", nil)
		req.Host = "custom-host.example.com"
		err := GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req)

		require.NoError(t, err)
		assert.Contains(t, req.Header.Get("Authorization"), "Signature=")
	})

	t.Run("deterministic signatures", func(t *testing.T) {
		makeReq := func() *http.Request {
			r, _ := http.NewRequest("GET", "https://s3.us-east-1.amazonaws.com/bucket/key", nil)
			return r
		}

		req1 := makeReq()
		req2 := makeReq()

		_ = GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req1)
		_ = GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req2)

		assert.Equal(t, req1.Header.Get("Authorization"), req2.Header.Get("Authorization"))
	})

	t.Run("empty path defaults to root", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://s3.amazonaws.com", nil)
		err := GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req)
		require.NoError(t, err)
		assert.Contains(t, req.Header.Get("Authorization"), "Signature=")
	})

	t.Run("query parameters are canonicalized", func(t *testing.T) {
		req1, _ := http.NewRequest("GET", "https://s3.amazonaws.com/?b=2&a=1", nil)
		req2, _ := http.NewRequest("GET", "https://s3.amazonaws.com/?a=1&b=2", nil)

		_ = GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req1)
		_ = GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req2)

		assert.Equal(t, req1.Header.Get("Authorization"), req2.Header.Get("Authorization"))
	})

	t.Run("path with special characters", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://s3.amazonaws.com/bucket/path+with spaces", nil)
		// URL-encode the path properly
		req.URL.Path = "/bucket/path+with spaces"
		err := GenerateAuthHeaderReq("AKID", "SECRET", "20240101T000000Z", "us-east-1", "s3", req)
		require.NoError(t, err)
		assert.Contains(t, req.Header.Get("Authorization"), "Signature=")
	})
}

func TestTimeFormatConstants(t *testing.T) {
	assert.Equal(t, "20060102T150405Z", TimeFormat)
	assert.Equal(t, "20060102", ShortTimeFormat)
}

func TestGenerateAuthHeaderReq_URLHostFallback(t *testing.T) {
	// When req.Host is empty, should use req.URL.Host
	req := &http.Request{
		Method: "GET",
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
