package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

// Verify checks the request signature under secretAccessKey, returning a VerifiedRequest
// when the request is authentic and ErrSignatureMismatch when it is not.
//
// It authenticates the request metadata and the signed payload hash only; it does not read
// the body. Confirming that the body actually hashes to the signed value is the caller's
// responsibility.
func (req *SignedRequest) Verify(secretAccessKey string) (*VerifiedRequest, error) {
	stringToSign := req.buildStringToSign()
	signingKey := req.buildSigningKey(secretAccessKey)

	// Constant-time compare our signature against the one the client provided.
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
	if !hmac.Equal([]byte(signature), []byte(req.Signature)) {
		return nil, ErrSignatureMismatch
	}

	return &VerifiedRequest{SignedRequest: req, SigningKey: signingKey}, nil
}

// buildCanonicalHash returns the hex SHA256 of the request's SigV4 canonical request.
func (req *SignedRequest) buildCanonicalHash() string {
	// The signed content hash was resolved by Parse (a sentinel or a hex digest) and is
	// used verbatim; Verify never derives it from the body.
	contentHash := req.Canonical.ContentHash

	// Canonical query: encode every value of each key into a "k=v" pair.
	pairs := make([]string, 0, len(req.Canonical.Query))
	for key, values := range req.Canonical.Query {
		for _, value := range values {
			pairs = append(pairs, uriEncode(key)+"="+uriEncode(value))
		}
	}
	// Sort by encoded key, then value; the '&' join happens in the canonical request below.
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
	return hex.EncodeToString(canonicalSum[:])
}

// buildStringToSign returns the SigV4 string-to-sign for the given canonical-request hash.
func (req *SignedRequest) buildStringToSign() string {
	// String-to-sign over the credential scope and canonical request hash.
	scope := req.Credential.Date + "/" + req.Credential.Region + "/" + req.Credential.Service + "/" + amzScopeTerminator
	return strings.Join([]string{
		string(algorithmV4),
		req.Timestamp.Format(amzTimeFormat),
		scope,
		req.buildCanonicalHash(),
	}, "\n")
}

// buildSigningKey derives the dated SigV4 signing key from secretAccessKey.
func (req *SignedRequest) buildSigningKey(secretAccessKey string) []byte {
	// AWS4<secret> -> date -> region -> service -> aws4_request.
	key := hmacSHA256([]byte("AWS4"+secretAccessKey), req.Credential.Date)
	key = hmacSHA256(key, req.Credential.Region)
	key = hmacSHA256(key, req.Credential.Service)
	key = hmacSHA256(key, amzScopeTerminator)
	return key
}

// hmacSHA256 returns the HMAC-SHA256 of data under key.
func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}
