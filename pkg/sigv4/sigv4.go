package sigv4

import (
	"errors"
	"time"
)

// TODO: Support browser-based POST policy uploads, which sign a base64-encoded
// policy document instead of a canonical request.

const (
	AmzTimeFormat      = "20060102T150405Z"
	AmzDateFormat      = "20060102"
	AmzScopeTerminator = "aws4_request"

	// MaxPayloadLen caps the body Parse will buffer to derive the content hash
	// for a non-S3 request. Non-S3 (control-plane) bodies are small; the cap bounds
	// pre-auth memory use.
	MaxPayloadLen = 10 << 20 // 10 MiB

	MaxClockSkew  = 15 * time.Minute
	MaxPresignAge = 7 * 24 * time.Hour
)

// ContentMode is an x-amz-content-sha256 sentinel used in place of a literal payload
// hash.
type ContentMode string

const (
	// UnsignedPayload indicates that the request body is not covered by the signature.
	// S3 only.
	UnsignedPayload ContentMode = "UNSIGNED-PAYLOAD"

	// EmptyPayload is the hex SHA-256 of an empty body, signed by a bodyless non-S3 request.
	EmptyPayload string = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

type Algorithm string

const (
	AlgorithmV4 Algorithm = "AWS4-HMAC-SHA256"
	// TODO: SigV4a (ECDSA-P256, X-Amz-Region-Set, region-less scope; Multi-Region
	// Access Points) is defined but not verified — getAuthComponents rejects it.
	AlgorithmV4a Algorithm = "AWS4-ECDSA-P256-SHA256"
)

// Sentinel errors returned by Parse and Verify. Callers match them with
// errors.Is to map an authentication failure onto the appropriate S3 response.
var (
	// ErrMissingAuthentication is returned when a request carries neither an
	// Authorization header nor presigned-URL query parameters.
	ErrMissingAuthentication = errors.New("missing authentication information")

	// ErrUnsupportedAlgorithm is returned for a signing algorithm this package
	// does not implement.
	ErrUnsupportedAlgorithm = errors.New("unsupported authentication algorithm")

	// ErrMalformedAuthorization is returned when the Authorization header cannot
	// be parsed or carries an invalid credential scope.
	ErrMalformedAuthorization = errors.New("malformed Authorization header")

	// ErrMalformedPresignedURL is returned when required presigned-URL query
	// parameters are missing or invalid.
	ErrMalformedPresignedURL = errors.New("malformed presigned URL")

	// ErrRequestTimeInvalid is returned when the request date is missing or
	// cannot be parsed.
	ErrRequestTimeInvalid = errors.New("invalid or missing request time")

	// ErrRequestTimeTooSkewed is returned when the request time is too far from
	// the server's clock.
	ErrRequestTimeTooSkewed = errors.New("request time too skewed")

	// ErrPresignedURLExpired is returned when a presigned URL is used outside its
	// validity window.
	ErrPresignedURLExpired = errors.New("presigned URL expired or not yet valid")

	// ErrMissingContentSHA256 is returned when a header-authed request to S3 omits
	// the x-amz-content-sha256 header.
	ErrMissingContentSHA256 = errors.New("missing x-amz-content-sha256 header")

	// ErrPayloadTooLarge is returned when a non-S3 request's body exceeds the size
	// Parse will buffer to derive the signed content hash.
	ErrPayloadTooLarge = errors.New("request payload exceeds maximum size for hashing")

	// ErrUnsignedHeader is returned when a header that must be signed (host,
	// Content-MD5, or an x-amz-* header) is absent from SignedHeaders.
	ErrUnsignedHeader = errors.New("required header is not signed")

	// ErrSignatureMismatch is returned when the computed signature does not match
	// the one supplied with the request.
	ErrSignatureMismatch = errors.New("signature mismatch")
)

// VerifiedRequest is the result of a successful Verify. It embeds the authenticated
// SignedRequest and carries the derived signing key.
type VerifiedRequest struct {
	*SignedRequest

	// SigningKey is the request's dated SigV4 signing key, exposed so a caller that signs
	// follow-on operations keyed on it need not re-derive the key.
	SigningKey []byte
}

// SignedRequest is the validated signing metadata Parse extracts from a request,
// carrying everything Verify needs to check the signature.
type SignedRequest struct {
	Canonical  CanonicalRequest
	Credential ScopedCredential
	Algorithm  Algorithm
	Timestamp  time.Time
	Signature  string
}

// CanonicalRequest holds the parsed components of the SigV4 canonical request.
type CanonicalRequest struct {
	Method string
	URI    string
	// Query excludes X-Amz-Signature, which signs the rest. Each key maps to all
	// of its values so repeated query keys canonicalize correctly.
	Query map[string][]string
	// Headers is every request header keyed lowercase, a superset of SignedHeaders.
	Headers map[string]string
	// SignedHeaders is the set of header names covered by the signature.
	SignedHeaders map[string]struct{}
	// ContentHash is the signed payload hash, used verbatim in the canonical request:
	// a mode sentinel, a hex digest, or empty for a non-S3 request that omits
	// x-amz-content-sha256.
	ContentHash string
}

// ScopedCredential is the parsed "<AKID>/YYYYMMDD/region/service/aws4_request"
// credential scope that keys the signing-key derivation.
type ScopedCredential struct {
	AccessKeyID string
	Date        string
	Region      string
	Service     string
}
