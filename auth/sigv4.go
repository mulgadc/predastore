// Package auth provides AWS Signature V4 authentication utilities
// shared between s3 and s3db packages.
package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
)

const (
	// TimeFormat is the full-width form to be used in the X-Amz-Date header.
	TimeFormat = "20060102T150405Z"

	// ShortTimeFormat is the shortened form used in credential scope.
	ShortTimeFormat = "20060102"
)

// HashSHA256 returns the hex-encoded SHA256 hash of the input
func HashSHA256(s string) string {
	hash := sha256.Sum256([]byte(s))
	return hex.EncodeToString(hash[:])
}

// HmacSHA256 returns the HMAC-SHA256 of data using the given key
func HmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// HmacSHA256Hex returns the hex-encoded HMAC-SHA256 of data using the given key
func HmacSHA256Hex(key []byte, data string) string {
	return hex.EncodeToString(HmacSHA256(key, data))
}

// GetSigningKey derives the signing key for AWS Signature V4
func GetSigningKey(secret, date, region, service string) []byte {
	kDate := HmacSHA256([]byte("AWS4"+secret), date)
	kRegion := HmacSHA256(kDate, region)
	kService := HmacSHA256(kRegion, service)
	kSigning := HmacSHA256(kService, "aws4_request")
	return kSigning
}

// UriEncode follows AWS's specific requirements for canonical URI encoding
func UriEncode(input string, encodeSlash bool) string {
	var builder strings.Builder
	builder.Grow(len(input) * 3)

	for _, b := range []byte(input) {
		if (b >= 'A' && b <= 'Z') ||
			(b >= 'a' && b <= 'z') ||
			(b >= '0' && b <= '9') ||
			b == '-' || b == '.' || b == '_' || b == '~' {
			builder.WriteByte(b)
		} else if b == '/' && !encodeSlash {
			builder.WriteByte(b)
		} else {
			builder.WriteString(fmt.Sprintf("%%%02X", b))
		}
	}

	return builder.String()
}

// CanonicalQueryString creates the canonical query string according to AWS specs
func CanonicalQueryString(queryParams map[string][]string) string {
	keys := make([]string, 0, len(queryParams))
	for k := range queryParams {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var pairs []string
	for _, key := range keys {
		values := queryParams[key]
		sort.Strings(values)

		encodedKey := UriEncode(key, true)
		for _, v := range values {
			encodedValue := UriEncode(v, true)
			pairs = append(pairs, fmt.Sprintf("%s=%s", encodedKey, encodedValue))
		}
	}

	return strings.Join(pairs, "&")
}

// GenerateAuthHeaderReq signs an HTTP request using AWS Signature V4
func GenerateAuthHeaderReq(accessKey, secretKey, timestamp, region, service string, req *http.Request) error {
	date := timestamp[:8]

	// Create canonical request
	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-date:%s\n", req.Host, timestamp)
	signedHeaders := "host;x-amz-date"

	var payloadHash string
	if req.Body != nil {
		bodyContent, err := io.ReadAll(req.Body)
		if err != nil {
			return err
		}
		req.Body = io.NopCloser(bytes.NewReader(bodyContent))
		payloadHash = HashSHA256(string(bodyContent))
	} else {
		payloadHash = HashSHA256("")
	}

	queryUrl := req.URL.Query()
	for key := range queryUrl {
		sort.Strings(queryUrl[key])
	}
	canonicalQueryString := strings.Replace(queryUrl.Encode(), "+", "%20", -1)

	canonicalRequest := fmt.Sprintf(
		"%s\n%s\n%s\n%s\n%s\n%s",
		req.Method,
		req.URL.Path,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	)

	hashedCanonicalRequest := HashSHA256(canonicalRequest)

	scope := fmt.Sprintf("%s/%s/%s/aws4_request", date, region, service)
	stringToSign := fmt.Sprintf(
		"AWS4-HMAC-SHA256\n%s\n%s\n%s",
		timestamp,
		scope,
		hashedCanonicalRequest,
	)

	signingKey := GetSigningKey(secretKey, date, region, service)
	signature := HmacSHA256Hex(signingKey, stringToSign)

	authHeader := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s/%s/%s/aws4_request, SignedHeaders=%s, Signature=%s",
		accessKey,
		date,
		region,
		service,
		signedHeaders,
		signature,
	)

	req.Header.Set("Authorization", authHeader)
	req.Header.Set("X-Amz-Date", timestamp)

	return nil
}
