package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"
)

// canonicalRequest reconstructs the client's SigV4 canonical request from the
// wire form. S3 uses single URI-path encoding (the canonical URI is the
// already-percent-encoded wire path, not re-escaped); other services keep the
// SDK default double-encoding.
func (req *Request) canonicalRequest(payloadHash string) string {
	// TODO: METHOD\ncanonicalURI\ncanonicalQuery\ncanonicalHeaders\n\n
	// signedHeaders\npayloadHash — single-encode the S3 canonical URI so
	// '='-key signatures verify.
	panic("sigv4: canonicalRequest not implemented")
}

// stringToSign builds the SigV4 string-to-sign from the canonical request
// hash, signing time, and credential scope.
func stringToSign(t time.Time, scope Scope, canonicalHash string) string {
	return strings.Join([]string{
		algorithm,
		t.UTC().Format(timeFormat),
		credentialScopeString(t, scope),
		canonicalHash,
	}, "\n")
}

// credentialScopeString renders "<yyyymmdd>/<region>/<service>/aws4_request".
func credentialScopeString(t time.Time, scope Scope) string {
	return t.UTC().Format(dateFormat) + "/" + scope.Region + "/" + scope.Service + "/aws4_request"
}

// deriveSigningKey derives the date/region/service/aws4_request signing-key
// chain from the secret. Shared by header, presigned, and chunk verification.
func deriveSigningKey(secret string, t time.Time, scope Scope) []byte {
	kDate := hmacSum([]byte("AWS4"+secret), t.UTC().Format(dateFormat))
	kRegion := hmacSum(kDate, scope.Region)
	kService := hmacSum(kRegion, scope.Service)
	return hmacSum(kService, "aws4_request")
}

func hmacSum(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

func hmacHex(key []byte, data string) string {
	return hex.EncodeToString(hmacSum(key, data))
}

func hashHex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
