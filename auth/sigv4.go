// Package auth implements AWS Signature V4 (header-based) signing and
// verification. Signing delegates to aws-sdk-go-v2/aws/signer/v4;
// verification re-signs the request via the same SDK and byte-compares
// the resulting Authorization header against the one parsed off the
// wire. The package owns ParseHTTP, (*Request).Verify, and the Err*
// sentinels — surfaces the SDK does not provide.
package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	authHeaderKey = "Authorization"
	dateHeaderKey = "X-Amz-Date"

	timeFormat = "20060102T150405Z"

	maxClockSkew = 5 * time.Minute
)

// authHdrRE matches the AWS4-HMAC-SHA256 Authorization header emitted by
// aws-sdk-go-v2's SignHTTP. Submatches: 1=access key, 2=date YYYYMMDD,
// 3=region, 4=service, 5=signed-headers list, 6=signature (64 hex).
var authHdrRE = regexp.MustCompile(
	`^AWS4-HMAC-SHA256 Credential=([^/]+)/(\d{8})/([^/]+)/([^/]+)/aws4_request, ` +
		`SignedHeaders=([^,]+), Signature=([a-f0-9]{64})$`)

var (
	ErrMissingAuth         = errors.New("auth: missing Authorization header")
	ErrMissingDate         = errors.New("auth: missing X-Amz-Date header")
	ErrInvalidAuthFormat   = errors.New("auth: invalid Authorization header")
	ErrMissingSignedHeader = errors.New("auth: SignedHeaders missing required header")
	ErrMissingContentSHA   = errors.New("auth: missing X-Amz-Content-Sha256 header")
	ErrClockSkew           = errors.New("auth: request timestamp outside allowed skew")
	ErrSignatureMismatch   = errors.New("auth: signature mismatch")
	ErrBodyHashMismatch    = errors.New("auth: body hash does not match X-Amz-Content-Sha256")
)

// SigMismatchError carries the wire Authorization header alongside the
// header we reproduced via the SDK. Operators diff the two to locate
// the divergence (credential scope, SignedHeaders list, signature).
type SigMismatchError struct {
	AccessKeyID     string
	ExpectedAuthHdr string
	ProvidedAuthHdr string
}

func (e *SigMismatchError) Error() string        { return ErrSignatureMismatch.Error() }
func (e *SigMismatchError) Is(target error) bool { return target == ErrSignatureMismatch }

// Options tunes Sign and Verify.
type Options struct {
	Time     func() time.Time // defaults to time.Now
	bodyHash string           // set via WithBodyHash; consumed by Verify
}

// WithTime pins the time used by Sign (X-Amz-Date generation +
// credential scope) and Verify (skew comparison). Defaults to time.Now().
func WithTime(t time.Time) func(*Options) {
	return func(o *Options) { o.Time = func() time.Time { return t } }
}

// WithBodyHash enables Verify's payload-integrity check. The caller-
// supplied hex SHA-256 (typically computed from the capped request body)
// is constant-time-compared against the wire X-Amz-Content-Sha256
// header. SigV4 payload sentinels in the header (UNSIGNED-PAYLOAD and
// the STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER] family) skip the
// check — the header is not a hash in those modes.
//
// Opt-in. Verify without WithBodyHash trusts the header verbatim,
// matching default SignHTTP semantics where the payload hash is the
// caller's responsibility.
func WithBodyHash(hex string) func(*Options) {
	return func(o *Options) { o.bodyHash = hex }
}

func resolveOpts(optFns []func(*Options)) Options {
	o := Options{Time: time.Now}
	for _, fn := range optFns {
		fn(&o)
	}
	return o
}

// Request is a parsed SigV4 envelope: the named fields are extracted
// from the Authorization + X-Amz-Date headers; the embedded *http.Request
// is the original.
type Request struct {
	*http.Request

	AccessKeyID string
	SignedTime  time.Time
	Region      string
	Service     string

	authHeader string // verbatim Authorization header, for verify-side compare
}

// SignReq signs r in place per the SigV4 header scheme via
// aws-sdk-go-v2/aws/signer/v4.(*Signer).SignReq. payloadHash is the
// hex SHA-256 of the request body or a SigV4 sentinel; r.Body is not
// consumed.
func SignReq(
	r *http.Request,
	accessKeyID, secretAccessKey string,
	payloadHash, service, region string,
	optFns ...func(*Options),
) error {
	o := resolveOpts(optFns)

	// Server-side Verify needs X-Amz-Content-Sha256 to recover the payload
	// hash from the wire. The SDK signer doesn't set it (the SDK service-
	// client middleware does); set it here for direct-signing callers.
	r.Header.Set("X-Amz-Content-Sha256", payloadHash)

	return v4.NewSigner().SignHTTP(
		context.Background(),
		aws.Credentials{AccessKeyID: accessKeyID, SecretAccessKey: secretAccessKey},
		r, payloadHash, service, region, o.Time().UTC(),
	)
}

// ParseReq structurally validates the Authorization + X-Amz-Date
// headers and returns the parsed envelope. No secret is consulted and
// r.Body is not read.
func ParseReq(r *http.Request) (*Request, error) {
	hdr := r.Header.Get(authHeaderKey)
	if hdr == "" {
		return nil, ErrMissingAuth
	}
	m := authHdrRE.FindStringSubmatch(hdr)
	if m == nil {
		return nil, ErrInvalidAuthFormat
	}

	// Required SignedHeaders entries per SigV4 spec; surfacing these as
	// ErrMissingSignedHeader preserves S3's AuthorizationHeaderMalformed
	// error code instead of falling through to a signature mismatch.
	hasHost, hasDate := false, false
	for h := range strings.SplitSeq(m[5], ";") {
		switch strings.ToLower(strings.TrimSpace(h)) {
		case "host":
			hasHost = true
		case "x-amz-date":
			hasDate = true
		}
	}
	if !hasHost {
		return nil, fmt.Errorf("%w: host", ErrMissingSignedHeader)
	}
	if !hasDate {
		return nil, fmt.Errorf("%w: x-amz-date", ErrMissingSignedHeader)
	}

	ts := r.Header.Get(dateHeaderKey)
	if ts == "" {
		return nil, ErrMissingDate
	}
	time, err := time.Parse(timeFormat, ts)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidAuthFormat, err)
	}

	return &Request{
		Request:     r,
		AccessKeyID: m[1],
		SignedTime:  time,
		Region:      m[3],
		Service:     m[4],
		authHeader:  hdr,
	}, nil
}

// Verify cryptographically validates req against secretAccessKey by
// re-signing through the SDK with the expected timestamp/region/service
// and constant-time-comparing the resulting Authorization header against
// the wire value. Only clock skew is checked separately — every other
// divergence (region, service, payload hash, signed headers) surfaces
// as a SigMismatchError via the header comparison.
func (req *Request) Verify(
	secretAccessKey, service, region string,
	optFns ...func(*Options),
) error {
	o := resolveOpts(optFns)
	if o.Time().Sub(req.SignedTime).Abs() > maxClockSkew {
		return ErrClockSkew
	}

	payloadHash := req.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		return ErrMissingContentSHA
	}

	// Opt-in body-hash gate. Runs before the SDK re-sign so a body-swap
	// (header tampered to match a substituted payload) short-circuits
	// before the expensive crypto. Sentinel payloads (UNSIGNED-PAYLOAD,
	// STREAMING-*) carry no hash to check against.
	if o.bodyHash != "" && !isPayloadSentinel(payloadHash) {
		if subtle.ConstantTimeCompare([]byte(o.bodyHash), []byte(payloadHash)) != 1 {
			return ErrBodyHashMismatch
		}
	}

	// The SDK signs in place. Strip the wire Authorization so the SDK
	// doesn't see it, capture the freshly produced header, then restore
	// the wire value so downstream middleware observes the request
	// unchanged. signErr is captured first so the restore always runs.
	req.Header.Del(authHeaderKey)
	signErr := v4.NewSigner().SignHTTP(
		context.Background(),
		aws.Credentials{AccessKeyID: req.AccessKeyID, SecretAccessKey: secretAccessKey},
		req.Request, payloadHash, service, region, req.SignedTime.UTC(),
	)
	expected := req.Header.Get(authHeaderKey)
	req.Header.Set(authHeaderKey, req.authHeader)
	if signErr != nil {
		return fmt.Errorf("re-sign for verify: %w", signErr)
	}

	if subtle.ConstantTimeCompare([]byte(expected), []byte(req.authHeader)) != 1 {
		return &SigMismatchError{
			AccessKeyID:     req.AccessKeyID,
			ExpectedAuthHdr: expected,
			ProvidedAuthHdr: req.authHeader,
		}
	}
	return nil
}

// isPayloadSentinel reports whether s is one of the SigV4 payload-hash
// sentinel strings that callers send in X-Amz-Content-Sha256 instead of
// a hex SHA-256. Defined by the AWS S3 docs:
// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
func isPayloadSentinel(s string) bool {
	switch s {
	case "UNSIGNED-PAYLOAD",
		"STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
		"STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER":
		return true
	}
	return false
}
