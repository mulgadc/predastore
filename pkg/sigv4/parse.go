package sigv4

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// parseOption configures Parse.
type parseOption func(*parseOptions)

type parseOptions struct {
	now time.Time
}

// WithTime overrides the reference time Parse treats as "now" for the clock-skew
// and presigned-expiry checks. Defaults to time.Now() when unset.
func WithTime(t time.Time) parseOption {
	return func(o *parseOptions) { o.now = t }
}

// Parse extracts the SigV4 signing metadata from req, accepting both
// Authorization-header and presigned-URL requests.
func Parse(req *http.Request, opts ...parseOption) (*SignedRequest, error) {
	cfg := parseOptions{now: time.Now()}
	for _, opt := range opts {
		opt(&cfg)
	}

	presigned, algorithm, rawCredential, rawSignedHeaders, signature, err := getAuthComponents(req)
	if err != nil {
		return nil, err
	}

	timestamp, err := parseTimestamp(req, presigned, cfg.now)
	if err != nil {
		return nil, err
	}

	credential, err := parseCredential(rawCredential, timestamp)
	if err != nil {
		return nil, err
	}

	// Build the canonical request with the service the client signed under (from the
	// scope), so the reconstruction matches the wire signature.
	canonical, err := parseCanonicalRequest(req, presigned, credential.Service, rawSignedHeaders)
	if err != nil {
		return nil, err
	}

	return &SignedRequest{
		Algorithm:  algorithm,
		Timestamp:  timestamp,
		Credential: credential,
		Signature:  signature,
		Canonical:  canonical,
	}, nil
}

// getAuthComponents pulls the raw signing components from the Authorization header, or
// the X-Amz-* query parameters when the request is presigned, and returns the signing
// algorithm after confirming it is one this package supports.
func getAuthComponents(req *http.Request) (presigned bool, algo Algorithm, credential, signedHeaders, signature string, err error) {
	query := req.URL.Query()
	authHdr := req.Header.Get("Authorization")
	// Presigned when the Authorization header is absent but X-Amz-Algorithm is in the query.
	presigned = authHdr == "" && query.Get("X-Amz-Algorithm") != ""

	var rawAlgo string
	if presigned {
		rawAlgo = query.Get("X-Amz-Algorithm")
		credential = query.Get("X-Amz-Credential")
		signedHeaders = query.Get("X-Amz-SignedHeaders")
		signature = query.Get("X-Amz-Signature")
		if credential == "" || signedHeaders == "" || signature == "" {
			return false, "", "", "", "", fmt.Errorf("%w: required X-Amz-* query parameters are missing", ErrMalformedPresignedURL)
		}
	} else {
		if authHdr == "" {
			return false, "", "", "", "", fmt.Errorf("%w: no Authorization header present", ErrMissingAuthentication)
		}

		// Parse Authorization algorithm.
		var remainder string
		var found bool
		rawAlgo, remainder, found = strings.Cut(authHdr, " ")
		if !found {
			return false, "", "", "", "", fmt.Errorf("%w: one and only one ' ' (space) required", ErrMalformedAuthorization)
		}

		// Separate Authorization key=value components, and check we have the right quantity.
		authHdrParts := strings.Split(remainder, ",")
		if len(authHdrParts) != 3 {
			return false, "", "", "", "", fmt.Errorf("%w: incorrect number of components provided", ErrMalformedAuthorization)
		}

		// Check we have the correct fields, and get their value strings (without key prefix).
		var credFound, hdrsFound, sigFound bool
		credential, credFound = strings.CutPrefix(strings.TrimSpace(authHdrParts[0]), "Credential=")
		signedHeaders, hdrsFound = strings.CutPrefix(strings.TrimSpace(authHdrParts[1]), "SignedHeaders=")
		signature, sigFound = strings.CutPrefix(strings.TrimSpace(authHdrParts[2]), "Signature=")
		if !credFound || !hdrsFound || !sigFound {
			return false, "", "", "", "", fmt.Errorf("%w: required components are missing or in an incorrect order", ErrMalformedAuthorization)
		}
	}

	if rawAlgo != string(AlgorithmV4) {
		return false, "", "", "", "", fmt.Errorf("%w: %q", ErrUnsupportedAlgorithm, rawAlgo)
	}

	return presigned, Algorithm(rawAlgo), credential, signedHeaders, signature, nil
}

// parseTimestamp resolves the request timestamp and enforces its validity window.
func parseTimestamp(req *http.Request, presigned bool, now time.Time) (time.Time, error) {
	query := req.URL.Query()

	var ts time.Time
	if presigned {
		// Presigned URLs carry the time in the X-Amz-Date query parameter.
		t, err := time.Parse(AmzTimeFormat, query.Get("X-Amz-Date"))
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: X-Amz-Date query parameter is missing or invalid", ErrMalformedPresignedURL)
		}

		ts = t
	} else if dateHdr := req.Header.Get("X-Amz-Date"); dateHdr != "" {
		// Header-authed requests prefer the X-Amz-Date header.
		t, err := time.Parse(AmzTimeFormat, dateHdr)
		if err != nil {
			// Present but unparseable: fail rather than falling through to the Date header.
			return time.Time{}, fmt.Errorf("%w: requires a valid X-Amz-Date or Date header", ErrRequestTimeInvalid)
		}

		ts = t
	} else {
		// Fall back to the Date header when X-Amz-Date is absent.
		t, err := http.ParseTime(req.Header.Get("Date"))
		if err != nil {
			return time.Time{}, fmt.Errorf("%w: requires a valid X-Amz-Date or Date header", ErrRequestTimeInvalid)
		}

		ts = t
	}

	// Normalize to UTC for easy downstream comparison.
	ts = ts.UTC()

	if presigned {
		// Presigned URLs are valid for X-Amz-Expires seconds (<= MaxPresignAge) after X-Amz-Date.
		expires, err := strconv.Atoi(query.Get("X-Amz-Expires"))
		if err != nil || expires <= 0 {
			return time.Time{}, fmt.Errorf("%w: X-Amz-Expires query parameter is missing or invalid", ErrMalformedPresignedURL)
		}

		age := time.Duration(expires) * time.Second
		if age > MaxPresignAge {
			return time.Time{}, fmt.Errorf("%w: X-Amz-Expires exceeds the maximum of %s", ErrMalformedPresignedURL, MaxPresignAge)
		}

		// Reject if signed too far in the future (clock skew) or older than its expiry window.
		if since := now.Sub(ts); since < -MaxClockSkew || since > age {
			return time.Time{}, ErrPresignedURLExpired
		}
	} else if skew := now.Sub(ts).Abs(); skew > MaxClockSkew {
		// Header-authed requests must be within MaxClockSkew of the server clock.
		return time.Time{}, ErrRequestTimeTooSkewed
	}

	return ts, nil
}

// parseCredential splits and structurally validates the credential scope
// ("<AKID>/YYYYMMDD/region/service/aws4_request") against the request time. It extracts
// the region and service but does not match them against an endpoint; Verify does that.
func parseCredential(credential string, t time.Time) (ScopedCredential, error) {
	parts := strings.Split(credential, "/")
	if len(parts) != 5 {
		return ScopedCredential{}, fmt.Errorf("%w: expected Credential to be in the format \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\"", ErrMalformedAuthorization)
	}

	// The date must be well-formed and match the date component of the request timestamp.
	if _, err := time.Parse(AmzDateFormat, parts[1]); err != nil {
		return ScopedCredential{}, fmt.Errorf("%w: the second Credential element must be a date in the format \"YYYYMMDD\"", ErrMalformedAuthorization)
	} else if parts[1] != t.Format(AmzDateFormat) {
		return ScopedCredential{}, fmt.Errorf("%w: date does not match X-Amz-Date (or Date, if X-Amz-Date is not set)", ErrMalformedAuthorization)
	}

	// The scope must end with the fixed terminator.
	if parts[4] != AmzScopeTerminator {
		return ScopedCredential{}, fmt.Errorf("%w: terminal value; expected %q", ErrMalformedAuthorization, AmzScopeTerminator)
	}

	return ScopedCredential{
		AccessKeyID: parts[0],
		Date:        parts[1],
		Region:      parts[2],
		Service:     parts[3],
	}, nil
}

// parseCanonicalRequest assembles and validates the canonical request from req.
func parseCanonicalRequest(req *http.Request, presigned bool, service string, rawSignedHeaders string) (CanonicalRequest, error) {
	contentHash, err := resolveContentHash(req, presigned, service)
	if err != nil {
		return CanonicalRequest{}, err
	}

	headers, signedHeaders, err := parseHeaders(req, rawSignedHeaders)
	if err != nil {
		return CanonicalRequest{}, err
	}

	return CanonicalRequest{
		Method:        req.Method,
		URI:           parseURI(req, service),
		Query:         parseQuery(req),
		Headers:       headers,
		SignedHeaders: signedHeaders,
		ContentHash:   contentHash,
	}, nil
}

// resolveContentHash returns the hashed payload signed into the canonical request.
func resolveContentHash(req *http.Request, presigned bool, service string) (string, error) {
	if service == "s3" {
		if presigned {
			// S3 presigned URLs sign the UNSIGNED-PAYLOAD sentinel; the body is not covered.
			return string(UnsignedPayload), nil
		}

		// S3 mandates x-amz-content-sha256 and signs its value verbatim — a hex digest or
		// a sentinel.
		if h := strings.TrimSpace(req.Header.Get("X-Amz-Content-Sha256")); h != "" {
			return h, nil
		}

		return "", ErrMissingContentSHA256
	}

	// If the request is bodyless, sign the hash of the empty string.
	if req.Body == nil || req.Body == http.NoBody {
		return EmptyPayload, nil
	}

	// Cap the read so an oversized body can't exhaust memory before authentication.
	buf, err := io.ReadAll(io.LimitReader(req.Body, MaxPayloadLen+1))
	if err != nil {
		return "", fmt.Errorf("reading request body to hash payload: %w", err)
	}

	if int64(len(buf)) > MaxPayloadLen {
		return "", ErrPayloadTooLarge
	}

	// Rewind the consumed body so the handler can still read it.
	_ = req.Body.Close()
	req.Body = io.NopCloser(bytes.NewReader(buf))

	sum := sha256.Sum256(buf)

	return hex.EncodeToString(sum[:]), nil
}

// parseHeaders snapshots the request headers and splits the raw SignedHeaders
// list, confirming that every header requiring a signature is signed. It returns the
// header snapshot and the signed-header list.
func parseHeaders(req *http.Request, rawSignedHeaders string) (map[string]string, map[string]struct{}, error) {
	// canonicalHeaderValue renders a header's values into SigV4 canonical form: each
	// value trimmed with internal whitespace runs collapsed to a single space, then
	// comma-joined across a multi-valued header.
	canonicalHeaderValue := func(values []string) string {
		parts := make([]string, len(values))
		for i, v := range values {
			// Fields also drops spaces inside quoted strings; SDKs canonicalize the same way.
			parts[i] = strings.Join(strings.Fields(v), " ")
		}
		return strings.Join(parts, ",")
	}

	// Snapshot every request header, keyed lowercase.
	headers := make(map[string]string, len(req.Header)+2)
	for name, values := range req.Header {
		headers[strings.ToLower(name)] = canonicalHeaderValue(values)
	}

	// host lives on the request struct, not req.Header.
	headers["host"] = strings.TrimSpace(req.Host)
	// content-length also lives on the request struct. Reproduce it only when the length is
	// known: Go reports -1 for an unknown length, which must stay excluded from the headers.
	if req.ContentLength >= 0 {
		// A known length includes 0, so an empty-body PUT/DELETE reproduces as "content-length:0".
		headers["content-length"] = strconv.FormatInt(req.ContentLength, 10)
	}

	// Split the client's SignedHeaders list into a lookup set.
	signedHeaders := make(map[string]struct{})
	for name := range strings.SplitSeq(rawSignedHeaders, ";") {
		signedHeaders[strings.ToLower(name)] = struct{}{}
	}

	for name := range headers {
		// host, Content-MD5, and every x-amz-* header except x-amz-content-sha256 must be signed.
		required := name == "host" || name == "content-md5" || (strings.HasPrefix(name, "x-amz-") && name != "x-amz-content-sha256")
		if !required {
			continue
		}

		if _, ok := signedHeaders[name]; !ok {
			return nil, nil, fmt.Errorf("%w: %s", ErrUnsignedHeader, name)
		}
	}

	return headers, signedHeaders, nil
}

// parseURI returns the canonical URI path for the request.
func parseURI(req *http.Request, service string) string {
	if service == "s3" {
		// S3 single-encodes: the wire-form escaped path is already canonical.
		return req.URL.EscapedPath()
	}

	// Other services double-encode each segment (the SigV4 default), preserving the "/" separators.
	segments := strings.Split(req.URL.EscapedPath(), "/")
	for i, segment := range segments {
		segments[i] = uriEncode(segment)
	}

	return strings.Join(segments, "/")
}

// parseQuery captures the query parameters for canonical reconstruction,
// preserving every value of a repeated key.
func parseQuery(req *http.Request) map[string][]string {
	query := req.URL.Query()
	snapshot := make(map[string][]string, len(query))
	for key, values := range query {
		// X-Amz-Signature is excluded; it signs the rest.
		if key == "X-Amz-Signature" {
			continue
		}

		snapshot[key] = values
	}

	return snapshot
}
