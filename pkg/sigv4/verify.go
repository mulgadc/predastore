package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
)

// Verify recomputes the request signature with secretAccessKey and reports whether
// it matches the one supplied with the request. body is read (and consumed to EOF)
// only when the content hash is a non-sentinel value to check against, or must be
// derived from the body; pass a re-readable reader if the caller still needs the bytes.
func (req *SignedRequest) Verify(secretAccessKey string, body io.Reader) error {
	// The signed content hash was resolved by Parse (a sentinel, or a body hash checked below).
	contentHash := req.Canonical.ContentHash

	// A non-S3 request without x-amz-content-sha256 leaves it empty; hash the body now
	// (the signature check below then covers body integrity).
	bodyHashed := false
	if contentHash == "" {
		sum := sha256.New()
		if _, err := io.Copy(sum, body); err != nil {
			return fmt.Errorf("failed to read request body: %w", err)
		}
		contentHash = hex.EncodeToString(sum.Sum(nil))
		bodyHashed = true
	}

	// Canonical query string: sorted by encoded key, "k=v" pairs joined with '&'.
	pairs := make([]string, 0, len(req.Canonical.Query))
	for key, value := range req.Canonical.Query {
		pairs = append(pairs, uriEncode(key)+"="+uriEncode(value))
	}
	sort.Strings(pairs)

	// SigV4 signs the headers in sorted order, for both the header block and the list below.
	signedHeaders := make([]string, 0, len(req.Canonical.SignedHeaders))
	for name := range req.Canonical.SignedHeaders {
		signedHeaders = append(signedHeaders, name)
	}
	sort.Strings(signedHeaders)

	// Canonical headers: "name:value\n" per signed header.
	var headers strings.Builder
	for _, name := range signedHeaders {
		headers.WriteString(name)
		headers.WriteByte(':')
		headers.WriteString(req.Canonical.Headers[name])
		headers.WriteByte('\n')
	}

	uri := req.Canonical.URI
	if uri == "" {
		uri = "/"
	}

	// Assemble the canonical request and hash it.
	canonicalRequest := strings.Join([]string{
		req.Canonical.Method,
		uri,
		strings.Join(pairs, "&"),
		headers.String(), // trailing '\n' plus the join give the blank line before signed headers
		strings.Join(signedHeaders, ";"),
		contentHash,
	}, "\n")
	canonicalSum := sha256.Sum256([]byte(canonicalRequest))

	// String-to-sign over the credential scope and canonical request hash.
	scope := req.Credential.Date + "/" + req.Credential.Region + "/" + req.Credential.Service + "/" + amzScopeTerminator
	stringToSign := strings.Join([]string{
		string(algorithmV4),
		req.Timestamp.Format(amzTimeFormat),
		scope,
		hex.EncodeToString(canonicalSum[:]),
	}, "\n")

	// Derive the dated signing key: AWS4<secret> -> date -> region -> service -> aws4_request.
	signingKey := hmacSHA256([]byte("AWS4"+secretAccessKey), req.Credential.Date)
	signingKey = hmacSHA256(signingKey, req.Credential.Region)
	signingKey = hmacSHA256(signingKey, req.Credential.Service)
	signingKey = hmacSHA256(signingKey, amzScopeTerminator)

	// Constant-time compare our signature against the one the client provided.
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
	if !hmac.Equal([]byte(signature), []byte(req.Signature)) {
		return ErrSignatureMismatch
	}

	// The signature is authentic, so the signed content hash is now trusted. A hash we
	// computed from the body above is already covered by that check.
	if bodyHashed {
		return nil
	}
	switch contentStrategy(contentHash) {
	// Sentinels are signed verbatim; the body is never read here.
	case UnsignedPayload, StreamingUnsignedPayloadTrailer, StreamingV4Payload, StreamingV4PayloadTrailer:
	// A real hash: confirm the body matches what was signed.
	default:
		sum := sha256.New()
		if _, err := io.Copy(sum, body); err != nil {
			return fmt.Errorf("failed to read request body: %w", err)
		}
		if hex.EncodeToString(sum.Sum(nil)) != contentHash {
			return ErrContentSHA256Mismatch
		}
	}

	return nil
}

// hmacSHA256 returns the HMAC-SHA256 of data under key, one link in the signing-key chain.
func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}
