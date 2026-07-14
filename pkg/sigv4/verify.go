package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// Verify checks the request signature under secretAccessKey and confirms the credential
// scope's region and service match region and service, returning a VerifiedRequest when
// the request is authentic. region is the region the caller requires the client to have
// signed with, which is not necessarily the endpoint's own.
func (req *SignedRequest) Verify(secretAccessKey, region, service string) (*VerifiedRequest, error) {
	// Ensure client region and service match the caller's expected values.
	if req.Credential.Region != region {
		return nil, fmt.Errorf("%w: incorrect region %q; expected %q", ErrMalformedAuthorization, req.Credential.Region, region)
	} else if req.Credential.Service != service {
		return nil, fmt.Errorf("%w: incorrect service %q; expected %q", ErrMalformedAuthorization, req.Credential.Service, service)
	}

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
	// One (key, value) pair per value; the raw forms drive ordering.
	type queryParam struct{ key, value string }
	params := make([]queryParam, 0, len(req.Canonical.Query))
	for key, values := range req.Canonical.Query {
		for _, value := range values {
			params = append(params, queryParam{key, value})
		}
	}

	sort.Slice(params, func(i, j int) bool {
		// Key then value as separate fields, never the joined "k=v": '=' outranks the value
		// bytes, so a key that is a prefix of another would sort wrong.
		if params[i].key != params[j].key {
			return params[i].key < params[j].key
		}

		// Order on raw decoded bytes as AWS does (the SDK sorts req.URL.Query() before
		// encoding); encoded order differs once a byte drops below the unreserved range, so
		// "é" sorts after "a" but its "%C3%A9" encoding before it.
		return params[i].value < params[j].value
	})

	// Encode once, now that ordering is settled.
	pairs := make([]string, len(params))
	for i, p := range params {
		pairs[i] = uriEncode(p.key) + "=" + uriEncode(p.value)
	}

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
		req.Canonical.ContentHash,
	}, "\n")
	canonicalSum := sha256.Sum256([]byte(canonicalRequest))

	return hex.EncodeToString(canonicalSum[:])
}

// buildStringToSign returns the SigV4 string-to-sign for the given canonical-request hash.
func (req *SignedRequest) buildStringToSign() string {
	// String-to-sign over the credential scope and canonical request hash.
	scope := req.Credential.Date + "/" + req.Credential.Region + "/" + req.Credential.Service + "/" + AmzScopeTerminator

	return strings.Join([]string{
		string(AlgorithmV4),
		req.Timestamp.Format(AmzTimeFormat),
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
	key = hmacSHA256(key, AmzScopeTerminator)

	return key
}

// hmacSHA256 returns the HMAC-SHA256 of data under key.
func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))

	return mac.Sum(nil)
}
