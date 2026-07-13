package sigv4

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// TODO: Support browser-based POST policy uploads, which sign a base64-encoded
// policy document instead of a canonical request.

const (
	amzTimeFormat      = "20060102T150405Z"
	amzDateFormat      = "20060102"
	amzScopeTerminator = "aws4_request"

	MaxClockSkew  = 15 * time.Minute
	MaxPresignAge = 7 * 24 * time.Hour
)

type algorithm string

const (
	algorithmV4 algorithm = "AWS4-HMAC-SHA256"
	// TODO: SigV4a (ECDSA-P256, X-Amz-Region-Set, region-less scope; Multi-Region
	// Access Points) is defined but not verified — getAuthComponents rejects it.
	algorithmV4a algorithm = "AWS4-ECDSA-P256-SHA256"
)

type contentSentinel string

const (
	UnsignedPayload                 contentSentinel = "UNSIGNED-PAYLOAD"
	StreamingUnsignedPayloadTrailer contentSentinel = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	StreamingV4Payload              contentSentinel = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	StreamingV4PayloadTrailer       contentSentinel = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
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

	// ErrMissingContentSHA256 is returned when a header-authed request omits the
	// required x-amz-content-sha256 header.
	ErrMissingContentSHA256 = errors.New("missing x-amz-content-sha256 header")

	// ErrUnsignedHeader is returned when a header that must be signed (host,
	// Content-MD5, or an x-amz-* header) is absent from SignedHeaders.
	ErrUnsignedHeader = errors.New("required header is not signed")

	// ErrContentSHA256Mismatch is returned when the request body does not hash to
	// the signed x-amz-content-sha256 value.
	ErrContentSHA256Mismatch = errors.New("x-amz-content-sha256 does not match request body")

	// ErrSignatureMismatch is returned when the computed signature does not match
	// the one supplied with the request.
	ErrSignatureMismatch = errors.New("signature mismatch")
)

// SignedRequest is the validated signing metadata Parse extracts from a request,
// carrying everything Verify needs to recompute and check the signature.
type SignedRequest struct {
	Canonical  canonicalRequest
	Credential scopedCredential
	Algorithm  algorithm
	Timestamp  time.Time
	Signature  string
}

// canonicalRequest holds the elements Verify reassembles into the SigV4 canonical
// request string before hashing it.
type canonicalRequest struct {
	Method string
	URI    string
	// Query excludes X-Amz-Signature, which signs the rest. Each key maps to all
	// of its values so repeated query keys canonicalize correctly.
	Query map[string][]string
	// Headers is every request header keyed lowercase, a superset of SignedHeaders.
	Headers map[string]string
	// SignedHeaders is the set of header names covered by the signature.
	SignedHeaders map[string]struct{}
	// ContentHash is the signed payload hash: a sentinel, a hex digest, or empty
	// when Verify must derive it from the body.
	ContentHash string
}

// scopedCredential is the parsed "<AKID>/YYYYMMDD/region/service/aws4_request"
// credential scope that keys the signing-key derivation.
type scopedCredential struct {
	AccessKeyID string
	Date        string
	Region      string
	Service     string
}

// uriEncode applies RFC 3986 percent-encoding to a canonical query component
// (space becomes %20, not '+').
func uriEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' ||
			c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}
