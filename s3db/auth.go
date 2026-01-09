package s3db

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/mulgadc/predastore/auth"
)

const (
	// DefaultRegion is the default AWS region for s3db
	DefaultRegion = "us-east-1"

	// DefaultService is the service name for s3db signing
	DefaultService = "s3db"
)

// SignRequest signs an HTTP request using AWS Signature V4
// Uses the auth.GenerateAuthHeaderReq function for consistency with S3 API
func SignRequest(req *http.Request, accessKey, secretKey, region, service string) error {
	timestamp := time.Now().UTC().Format(auth.TimeFormat)
	return auth.GenerateAuthHeaderReq(accessKey, secretKey, timestamp, region, service, req)
}

// ValidateSignature validates an AWS Signature V4 authorization header
// Returns the access key if valid, or an error if invalid
func ValidateSignature(c *fiber.Ctx, credentials map[string]string, region, service string) (string, error) {
	authHeader := c.Get("Authorization")
	if authHeader == "" {
		return "", fmt.Errorf("missing Authorization header")
	}

	// Parse authorization header
	// Format: AWS4-HMAC-SHA256 Credential=ACCESS/DATE/REGION/SERVICE/aws4_request, SignedHeaders=..., Signature=...
	parts := strings.Split(authHeader, ", ")
	if len(parts) != 3 {
		return "", fmt.Errorf("invalid Authorization header format")
	}

	// Parse credential
	credPart := strings.TrimPrefix(parts[0], "AWS4-HMAC-SHA256 Credential=")
	creds := strings.Split(credPart, "/")
	if len(creds) != 5 {
		return "", fmt.Errorf("invalid credential scope")
	}
	accessKey, date, reqRegion, reqService := creds[0], creds[1], creds[2], creds[3]

	// Lookup secret key
	secretKey, ok := credentials[accessKey]
	if !ok {
		return "", fmt.Errorf("invalid access key")
	}

	// Parse signed headers and signature
	signedHeaders := strings.TrimPrefix(parts[1], "SignedHeaders=")
	signature := strings.TrimPrefix(parts[2], "Signature=")

	// Get timestamp
	timestamp := c.Get("X-Amz-Date")
	if timestamp == "" {
		return "", fmt.Errorf("missing X-Amz-Date header")
	}

	// Build canonical URI
	canonicalURI := string(c.Request().URI().Path())
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalURI = auth.UriEncode(canonicalURI, false)

	// Build canonical query string
	query := c.Queries()
	queryUrl := url.Values{}
	for k, v := range query {
		queryUrl[k] = []string{v}
	}
	for key := range queryUrl {
		sort.Strings(queryUrl[key])
	}
	canonicalQueryString := strings.Replace(queryUrl.Encode(), "+", "%20", -1)

	// Build canonical headers
	headers := strings.Split(signedHeaders, ";")
	sort.Strings(headers)

	canonicalHeaders := ""
	for _, header := range headers {
		canonicalHeaders += fmt.Sprintf("%s:%s\n", header, strings.TrimSpace(c.Get(header)))
	}

	// Calculate payload hash
	payloadHash := auth.HashSHA256(string(c.Body()))

	// Build canonical request
	canonicalRequest := fmt.Sprintf(
		"%s\n%s\n%s\n%s\n%s\n%s",
		c.Method(),
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	)

	// Hash canonical request
	hashedCanonicalRequest := auth.HashSHA256(canonicalRequest)

	// Create string to sign - use the region from the request for validation
	scope := fmt.Sprintf("%s/%s/%s/aws4_request", date, reqRegion, reqService)
	stringToSign := fmt.Sprintf(
		"AWS4-HMAC-SHA256\n%s\n%s\n%s",
		timestamp,
		scope,
		hashedCanonicalRequest,
	)

	// Derive signing key and calculate expected signature
	signingKey := auth.GetSigningKey(secretKey, date, reqRegion, reqService)
	expectedSig := auth.HmacSHA256Hex(signingKey, stringToSign)

	// Compare signatures
	if expectedSig != signature {
		return "", fmt.Errorf("signature mismatch")
	}

	return accessKey, nil
}
