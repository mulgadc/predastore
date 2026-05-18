// Package auth provides AWS Signature V4 authentication utilities
// shared between s3 and s3db packages.
package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	// TimeFormat is the full-width form used in the X-Amz-Date header.
	TimeFormat = "20060102T150405Z"

	// ShortTimeFormat is the shortened form used in credential scope.
	ShortTimeFormat = "20060102"
)

const (
	// verifyMaxClockSkew is the largest allowed difference between
	// X-Amz-Date and the server clock during request verification.
	// Matches AWS's documented 15-minute ceiling reduced to 5 minutes
	// to bound the replay window for captured Authorization headers.
	verifyMaxClockSkew = 5 * time.Minute

	// verifyMaxBodySize caps the request body read during SigV4
	// verification when the client did not advertise a payload hash.
	// Prevents unauthenticated callers from triggering OOM by streaming
	// an unbounded body before the signature check fails. Legitimate
	// large uploads use UNSIGNED-PAYLOAD, STREAMING-UNSIGNED-PAYLOAD-
	// TRAILER, or a precomputed hex SHA-256 in X-Amz-Content-Sha256.
	verifyMaxBodySize = 10 * 1024 * 1024
)

// Sentinels returned by VerifySigV4Request. Callers map these to
// transport-layer error codes (HTTP status, S3 error code, etc.).
var (
	ErrMissingAuth       = errors.New("auth: missing Authorization header")
	ErrInvalidAuthFormat = errors.New("auth: invalid Authorization header format")
	ErrInvalidCredScope  = errors.New("auth: invalid credential scope")
	ErrMissingDate       = errors.New("auth: missing X-Amz-Date header")
	ErrInvalidDateFormat = errors.New("auth: invalid X-Amz-Date header format")
	ErrClockSkew         = errors.New("auth: request timestamp outside allowed skew")
	ErrSignatureMismatch = errors.New("auth: signature mismatch")
	ErrBodyTooLarge      = errors.New("auth: request body exceeds limit")
)

// SecretLookup returns the secret access key for a claimed access key ID.
// Invoked by VerifySigV4Request after the Authorization header has been
// structurally parsed but before cryptographic verification. A non-nil
// error rejects the request without revealing whether the access key
// exists; callers may log the claimed ID for diagnostics but MUST NOT
// treat it as authenticated.
type SecretLookup func(accessKeyID string) (secret string, err error)

// SigMismatchError reports a signature mismatch with the diagnostic
// state needed to debug SDK-side signing bugs. Implements
// Is(ErrSignatureMismatch) so callers may use errors.Is for control
// flow or errors.As for rich logging.
type SigMismatchError struct {
	AccessKeyID       string
	CanonicalRequest  string
	StringToSign      string
	ExpectedSigPrefix string
	ProvidedSigPrefix string
}

func (e *SigMismatchError) Error() string        { return ErrSignatureMismatch.Error() }
func (e *SigMismatchError) Is(target error) bool { return target == ErrSignatureMismatch }

// VerifySigV4Request parses, looks up the secret via lookup, and verifies
// the AWS Signature V4 signature on r in one pass. r.Body is restored
// after reading so downstream handlers see the original payload.
//
// expectedRegion and expectedService are enforced against the credential
// scope; a mismatch returns ErrInvalidCredScope. They must come from
// server configuration — deriving them from the request itself would
// let the client validate against its own claim.
//
// On success, accessKeyID is the cryptographically verified principal.
// On error, accessKeyID is the *claimed* principal (empty if parsing
// failed before extracting it). Callers may use the claimed key for
// diagnostic logging but MUST NOT treat it as authenticated.
//
// Enforces:
//   - SignedHeaders must include host and x-amz-date
//   - X-Amz-Date must be present and within ±5 minutes of server clock
//   - Body capped at 10 MiB unless the client supplies UNSIGNED-PAYLOAD,
//     STREAMING-UNSIGNED-PAYLOAD-TRAILER, or a precomputed hex SHA-256
//     in X-Amz-Content-Sha256
func VerifySigV4Request(
	r *http.Request,
	expectedRegion, expectedService string,
	lookup SecretLookup,
) (accessKeyID string, err error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return "", ErrMissingAuth
	}

	parts := strings.Split(authHeader, ", ")
	if len(parts) != 3 {
		return "", ErrInvalidAuthFormat
	}

	credPart := strings.TrimPrefix(parts[0], "AWS4-HMAC-SHA256 Credential=")
	creds := strings.Split(credPart, "/")
	if len(creds) != 5 {
		return "", ErrInvalidCredScope
	}
	claimedKey, date, claimedRegion, claimedService := creds[0], creds[1], creds[2], creds[3]

	if claimedRegion != expectedRegion {
		return claimedKey, fmt.Errorf("%w: region (claimed=%q, expected=%q)", ErrInvalidCredScope, claimedRegion, expectedRegion)
	}
	if claimedService != expectedService {
		return claimedKey, fmt.Errorf("%w: service (claimed=%q, expected=%q)", ErrInvalidCredScope, claimedService, expectedService)
	}

	timestamp := r.Header.Get("X-Amz-Date")
	if timestamp == "" {
		return claimedKey, ErrMissingDate
	}
	parsedTime, err := time.Parse(TimeFormat, timestamp)
	if err != nil {
		return claimedKey, fmt.Errorf("%w: %v", ErrInvalidDateFormat, err)
	}
	if time.Since(parsedTime).Abs() > verifyMaxClockSkew {
		return claimedKey, ErrClockSkew
	}

	signedHeaders := strings.TrimPrefix(parts[1], "SignedHeaders=")
	headers := strings.Split(signedHeaders, ";")
	if err := RequireSignedHeaders(headers); err != nil {
		return claimedKey, fmt.Errorf("%w: %v", ErrInvalidAuthFormat, err)
	}

	secret, err := lookup(claimedKey)
	if err != nil {
		return claimedKey, err
	}

	canonicalURI := r.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalURI = UriEncode(canonicalURI, false)

	queryURL := r.URL.Query()
	for key := range queryURL {
		sort.Strings(queryURL[key])
	}
	canonicalQuery := strings.ReplaceAll(queryURL.Encode(), "+", "%20")

	sortedHeaders := append([]string(nil), headers...)
	sort.Strings(sortedHeaders)

	var canonicalHeaders strings.Builder
	for _, h := range sortedHeaders {
		var value string
		if h == "host" {
			// net/http moves the Host header off r.Header onto r.Host.
			value = r.Host
		} else {
			value = r.Header.Get(h)
		}
		fmt.Fprintf(&canonicalHeaders, "%s:%s\n", h, strings.TrimSpace(value))
	}

	payloadHash, err := computePayloadHash(r)
	if err != nil {
		return claimedKey, err
	}

	canonicalRequest := fmt.Sprintf(
		"%s\n%s\n%s\n%s\n%s\n%s",
		r.Method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders.String(),
		signedHeaders,
		payloadHash,
	)
	hashedCanonical := HashSHA256(canonicalRequest)

	scope := fmt.Sprintf("%s/%s/%s/aws4_request", date, expectedRegion, expectedService)
	stringToSign := fmt.Sprintf(
		"AWS4-HMAC-SHA256\n%s\n%s\n%s",
		timestamp,
		scope,
		hashedCanonical,
	)

	signingKey := GetSigningKey(secret, date, expectedRegion, expectedService)
	expectedSig := HmacSHA256Hex(signingKey, stringToSign)

	providedSig := strings.TrimPrefix(parts[2], "Signature=")
	if subtle.ConstantTimeCompare([]byte(expectedSig), []byte(providedSig)) != 1 {
		return claimedKey, &SigMismatchError{
			AccessKeyID:       claimedKey,
			CanonicalRequest:  canonicalRequest,
			StringToSign:      stringToSign,
			ExpectedSigPrefix: sigPrefix(expectedSig),
			ProvidedSigPrefix: sigPrefix(providedSig),
		}
	}

	return claimedKey, nil
}

// computePayloadHash returns the SHA-256 hash to commit to in the
// canonical request. Honors the standard X-Amz-Content-Sha256 streaming
// indicators and trusts a client-supplied precomputed hash — signature
// verification later guarantees integrity, since a lying client cannot
// produce a valid signature against the lied hash. Falls back to
// bounded body buffering when the client did not advertise a hash.
//
// Restores r.Body via bytes.NewReader so downstream handlers see the
// original bytes.
func computePayloadHash(r *http.Request) (string, error) {
	encoding := r.Header.Get("X-Amz-Content-Sha256")
	switch {
	case encoding == "STREAMING-UNSIGNED-PAYLOAD-TRAILER" || encoding == "UNSIGNED-PAYLOAD":
		return encoding, nil
	case isHexSHA256(encoding):
		return encoding, nil
	}
	if r.Body == nil {
		return HashSHA256(""), nil
	}
	// Read at most verifyMaxBodySize+1: if the extra byte arrives, the
	// payload exceeded the cap and we reject without buffering more.
	limited := io.LimitReader(r.Body, verifyMaxBodySize+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return "", fmt.Errorf("read request body: %w", err)
	}
	if int64(len(body)) > verifyMaxBodySize {
		return "", ErrBodyTooLarge
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	return HashSHA256(string(body)), nil
}

// sigPrefix returns the first 8 chars of a hex signature for diagnostics.
// Logging the full signature is avoided to limit replay exposure within
// the SigV4 clock-skew window.
func sigPrefix(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:8]
}

// isHexSHA256 reports whether s is exactly 64 lowercase hex characters
// (an encoded SHA-256 digest).
func isHexSHA256(s string) bool {
	if len(s) != hex.EncodedLen(32) {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// HashSHA256 returns the hex-encoded SHA256 hash of the input.
//
// Deprecated: only required by spinifex/spinifex/gateway/auth.go. Will
// become unexported once that file migrates to VerifySigV4Request.
func HashSHA256(s string) string {
	hash := sha256.Sum256([]byte(s))
	return hex.EncodeToString(hash[:])
}

// RequireSignedHeaders enforces that SigV4 SignedHeaders includes both
// "host" and "x-amz-date". Omitting either lets a captured Authorization
// header be replayed against a different vhost or outside the clock-skew
// window. AWS SDKs always sign both; rejecting requests that don't is safe.
//
// Deprecated: only required by spinifex/spinifex/gateway/auth.go. Will
// become unexported once that file migrates to VerifySigV4Request.
func RequireSignedHeaders(headers []string) error {
	var hasHost, hasDate bool
	for _, h := range headers {
		switch strings.ToLower(strings.TrimSpace(h)) {
		case "host":
			hasHost = true
		case "x-amz-date":
			hasDate = true
		}
	}
	if !hasHost {
		return fmt.Errorf("SignedHeaders must include host")
	}
	if !hasDate {
		return fmt.Errorf("SignedHeaders must include x-amz-date")
	}
	return nil
}

// hmacSHA256 returns the HMAC-SHA256 of data using the given key.
func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// HmacSHA256Hex returns the hex-encoded HMAC-SHA256 of data using the given key.
//
// Deprecated: only required by spinifex/spinifex/gateway/auth.go. Will
// become unexported once that file migrates to VerifySigV4Request.
func HmacSHA256Hex(key []byte, data string) string {
	return hex.EncodeToString(hmacSHA256(key, data))
}

// GetSigningKey derives the signing key for AWS Signature V4.
//
// Deprecated: only required by spinifex/spinifex/gateway/auth.go. Will
// become unexported once that file migrates to VerifySigV4Request.
func GetSigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	return kSigning
}

// UriEncode follows AWS's specific requirements for canonical URI encoding.
//
// Deprecated: only required by spinifex/spinifex/gateway/auth.go. Will
// become unexported once that file migrates to VerifySigV4Request.
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
			fmt.Fprintf(&builder, "%%%02X", b)
		}
	}

	return builder.String()
}

// GenerateAuthHeaderReq signs an HTTP request using AWS Signature V4.
//
// TODO: rename to SignSigV4Request for symmetry with VerifySigV4Request.
func GenerateAuthHeaderReq(accessKey, secretKey, timestamp, region, service string, req *http.Request) error {
	date := timestamp[:8]

	// req.Host is empty for newly created requests; fall back to URL.Host.
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}

	canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-date:%s\n", host, timestamp)
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
	canonicalQueryString := strings.ReplaceAll(queryUrl.Encode(), "+", "%20")

	canonicalURI := UriEncode(req.URL.Path, false)
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	canonicalRequest := fmt.Sprintf(
		"%s\n%s\n%s\n%s\n%s\n%s",
		req.Method,
		canonicalURI,
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
