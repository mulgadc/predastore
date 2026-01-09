package s3

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/mulgadc/predastore/auth"
)

const (
	// TimeFormat is the full-width form to be used in the X-Amz-Date header.
	TimeFormat = auth.TimeFormat

	// ShortTimeFormat is the shortened form used in credential scope.
	ShortTimeFormat = auth.ShortTimeFormat
)

// Permission map for methods mapped to actions
// GET /<bucket>/<key> -> s3:GetObject
// PUT /<bucket>/<key> -> s3:PutObject
// DELETE /<bucket>/<key> -> s3:DeleteObject
// POST /<bucket>/<key> -> s3:PutObject
// GET /<bucket> -> s3:ListBucket
// PUT /<bucket> -> s3:PutObject
// GET / -> s3:ListBucket

type Permissions struct {
	GetObject    bool
	PutObject    bool
	DeleteObject bool
	ListBucket   bool
}
type PermissionMap struct {
	Method            string
	Path              string
	AllowPublicBucket bool
	RequireAuth       bool
	Permissions       Permissions
}

var permissionMap = []PermissionMap{
	{
		Method:            "GET",
		Path:              "/bucket",
		AllowPublicBucket: true,
		RequireAuth:       false,
		Permissions:       Permissions{GetObject: true},
	},
	{
		Method:            "GET",
		Path:              "/bucket/*",
		AllowPublicBucket: true,
		RequireAuth:       false,
		Permissions:       Permissions{GetObject: true},
	},

	{
		Method:            "HEAD",
		Path:              "/bucket/*",
		AllowPublicBucket: true,
		RequireAuth:       false,
		Permissions:       Permissions{GetObject: true},
	},

	{
		Method:            "PUT",
		Path:              "/bucket/*",
		AllowPublicBucket: false,
		RequireAuth:       true,
		Permissions:       Permissions{PutObject: true},
	},
	{
		Method:            "DELETE",
		Path:              "/bucket/*",
		AllowPublicBucket: false,
		RequireAuth:       true,
	},
	{
		Method:            "POST",
		Path:              "/bucket/*",
		AllowPublicBucket: false,
		RequireAuth:       true,
		Permissions:       Permissions{PutObject: true},
	},
	{
		Method:            "GET",
		Path:              "/",
		AllowPublicBucket: true,
		RequireAuth:       false,
		Permissions:       Permissions{ListBucket: true},
	},
}

// GenerateAuthHeaderReq signs an HTTP request using AWS Signature V4
// This is a wrapper around auth.GenerateAuthHeaderReq for backwards compatibility
func GenerateAuthHeaderReq(accessKey, secretKey, timestamp, region, service string, req *http.Request) error {
	return auth.GenerateAuthHeaderReq(accessKey, secretKey, timestamp, region, service, req)
}

func (s3 *Config) validatePublicBucketPolicy(bucket string) error {

	// Loop through the buckets and check if the bucket has a public policy for public access
	for _, v := range s3.Buckets {
		if v.Name == bucket {
			if v.Public {
				return nil
			}
		}
	}

	return errors.New("NotPublicBucket")

}

func (s3 *Config) validatePublicBucketPermission(method, path string) error {

	pathParts := strings.Split(path, "/")

	// Lookup the permission map
	for _, permission := range permissionMap {
		slog.Debug("Checking permission", "method", permission.Method, "path", permission.Path)

		if permission.AllowPublicBucket && permission.Method == method {

			permissionMapPath := strings.Split(permission.Path, "/")
			slog.Debug("Permission map path", "permissionMapPath", permissionMapPath, "len", len(permissionMapPath))
			slog.Debug("Path parts", "pathParts", pathParts, "len", len(pathParts))

			requestBucket := pathParts[1]

			// Check if a bucket is specified, or root to ListBucket request
			if len(permissionMapPath) == 2 && len(pathParts) == 2 && permission.AllowPublicBucket {

				err := s3.validatePublicBucketPolicy(requestBucket)
				if err != nil {
					slog.Debug("Bucket does not have a public policy", "error", err)
					return errors.New("AccessDenied")
				}

				slog.Debug("Allowing public bucket access for listing")
				return nil
			}

			// Check if the bucket is public and a GET request for an object
			if permission.AllowPublicBucket && len(permissionMapPath) == 3 && len(pathParts) >= 3 {
				slog.Debug("Allowing public bucket access for object")
				return nil
			}

			//break

		}
	}

	return errors.New("AccessDenied")
}

func (s3 *Config) SigV4AuthMiddleware(c *fiber.Ctx) error {

	// Check route, if authentication is required
	path := c.Path()
	//segments := strings.Split(path, "/")

	// Get the method
	method := c.Method()

	// Lookup the permission map if route requires authentication

	authHeader := c.Get("Authorization")

	slog.Debug("Authorization header", "authHeader", authHeader)

	// Confirm if the requested resource is public
	publicBucketAccess := s3.validatePublicBucketPermission(method, path)

	// Allow public bucket access if no auth header is provided (--no-sign-request using the AWS CLI)
	if publicBucketAccess == nil && authHeader == "" {
		slog.Debug("Allowing public bucket access with no auth header")
		return c.Next()
	}

	// If the resource is not public and no auth header is provided, return access denied
	if authHeader == "" {
		slog.Debug("Missing Authorization header")
		return errors.New("AccessDenied")
	}

	// Example header:
	// AWS4-HMAC-SHA256 Credential=EXAMPLEACCESSKEY/20250414/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=...
	parts := strings.Split(authHeader, ", ")
	if len(parts) != 3 {
		slog.Debug("Invalid Authorization header format")
		return errors.New("AccessDenied")
	}

	// Parse credential
	creds := strings.Split(strings.TrimPrefix(parts[0], "AWS4-HMAC-SHA256 Credential="), "/")
	if len(creds) != 5 {
		slog.Debug("Invalid credential scope")
		return errors.New("AccessDenied")
	}
	accessKey, date, region, svc := creds[0], creds[1], creds[2], creds[3]

	var secretKey string
	// Loop through the auth config and find the secret key
	for _, auth := range s3.Auth {
		if auth.AccessKeyID == accessKey {
			secretKey = auth.SecretAccessKey
		}
	}

	if secretKey == "" {
		slog.Debug("Invalid access key")
		return errors.New("AccessDenied")
	}

	signedHeaders := strings.TrimPrefix(parts[1], "SignedHeaders=")
	signature := strings.TrimPrefix(parts[2], "Signature=")
	// Construct canonical request
	//canonicalURI := c.OriginalURL()

	// https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html:
	// if input has no path, "/" is used

	canonicalURI := string(c.Request().URI().Path())

	if len(canonicalURI) == 0 {
		canonicalURI = "/"
	}

	// Next the canonicalQueryString which needs to be sorted
	canonicalURI = UriEncode(canonicalURI, false)
	slog.Debug("Canonical URI", "canonicalURI", canonicalURI)

	query := c.Queries()
	queryUrl := url.Values{}

	// Create using url.Values
	for k, v := range query {
		queryUrl[k] = []string{v}
	}

	for key := range queryUrl {
		sort.Strings(queryUrl[key])
	}

	canonicalQueryString := strings.Replace(queryUrl.Encode(), "+", "%20", -1)

	slog.Debug("Canonical query string", "canonicalQueryString", canonicalQueryString)

	// Encode the canonical URI
	// S3 requires disabling path encoding, however other AWS services may require it
	// canonicalURI = url.PathEscape(canonicalURI)
	//canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-date:%s\n", c.Hostname(), c.Get("x-amz-date"))

	// Loop through the headers and build the canonical headers
	headers := strings.Split(signedHeaders, ";")

	// Required to be sorted as per spec
	sort.Strings(headers)

	slog.Debug("Headers", "headers", headers)

	canonicalHeaders := ""
	for _, header := range headers {
		//canonicalHeaders += fmt.Sprintf("%s:%s\n", header, c.Get(header))

		// Trim header value
		canonicalHeaders += fmt.Sprintf("%s:%s\n", header, strings.TrimSpace(c.Get(header)))
		slog.Debug("Canonical header", "header", header, "value", c.Get(header))
	}

	// Read payload body
	// Validate the transfer payload encoding
	var payloadHash string
	payloadEncoding := c.Get("X-Amz-Content-SHA256")

	if payloadEncoding == "STREAMING-UNSIGNED-PAYLOAD-TRAILER" || payloadEncoding == "UNSIGNED-PAYLOAD" {
		payloadHash = payloadEncoding //hashSHA256("")

	} else {
		payloadHash = hashSHA256(string(c.Body()))
	}

	canonicalRequest := fmt.Sprintf(
		"%s\n%s\n%s\n%s\n%s\n%s",
		c.Method(),
		//c.Path(),
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	)

	// Hash it
	hashedCanonicalRequest := hashSHA256(canonicalRequest)

	// Create string to sign
	timestamp := c.Get("x-amz-date")
	scope := fmt.Sprintf("%s/%s/%s/aws4_request", date, s3.Region, svc)

	stringToSign := fmt.Sprintf(
		"AWS4-HMAC-SHA256\n%s\n%s\n%s",
		timestamp,
		scope,
		hashedCanonicalRequest,
	)

	// Derive signing key
	signingKey := getSigningKey(secretKey, date, region, svc)

	// Calculate signature
	expectedSig := hmacSHA256Hex(signingKey, stringToSign)

	// Compare
	if expectedSig != signature {
		slog.Debug("Invalid signature", "expected", expectedSig, "actual", signature)
		return errors.New("AccessDenied")
	}

	// Store authenticated user info in context for use by handlers
	c.Locals(string(ContextKeyAccessKeyID), accessKey)
	// Also store account ID if available
	for _, auth := range s3.Auth {
		if auth.AccessKeyID == accessKey {
			c.Locals(string(ContextKeyAccountID), auth.AccountID)
			break
		}
	}

	return c.Next()
}

// HashSHA256 returns the hex-encoded SHA256 hash of the input
// Wrapper around auth.HashSHA256 for backwards compatibility
func HashSHA256(s string) string { return auth.HashSHA256(s) }

// HmacSHA256 returns the HMAC-SHA256 of data using the given key
// Wrapper around auth.HmacSHA256 for backwards compatibility
func HmacSHA256(key []byte, data string) []byte { return auth.HmacSHA256(key, data) }

// HmacSHA256Hex returns the hex-encoded HMAC-SHA256 of data using the given key
// Wrapper around auth.HmacSHA256Hex for backwards compatibility
func HmacSHA256Hex(key []byte, data string) string { return auth.HmacSHA256Hex(key, data) }

// GetSigningKey derives the signing key for AWS Signature V4
// Wrapper around auth.GetSigningKey for backwards compatibility
func GetSigningKey(secret, date, region, service string) []byte {
	return auth.GetSigningKey(secret, date, region, service)
}

// Lowercase aliases for internal use
func hashSHA256(s string) string                                { return auth.HashSHA256(s) }
func hmacSHA256(key []byte, data string) []byte                 { return auth.HmacSHA256(key, data) }
func hmacSHA256Hex(key []byte, data string) string              { return auth.HmacSHA256Hex(key, data) }
func getSigningKey(secret, date, region, service string) []byte { return auth.GetSigningKey(secret, date, region, service) }

// UriEncode follows AWS's specific requirements for canonical URI encoding
func UriEncode(input string, encodeSlash bool) string {
	var builder strings.Builder
	builder.Grow(len(input) * 3) // Pre-allocate space for worst case

	for _, b := range []byte(input) {
		// AWS's unreserved characters
		if (b >= 'A' && b <= 'Z') ||
			(b >= 'a' && b <= 'z') ||
			(b >= '0' && b <= '9') ||
			b == '-' || b == '.' || b == '_' || b == '~' {
			builder.WriteByte(b)
		} else if b == '/' && !encodeSlash {
			builder.WriteByte(b)
		} else {
			// URI encode everything else
			builder.WriteString(fmt.Sprintf("%%%02X", b))
		}
	}

	return builder.String()
}

// CanonicalQueryString creates the canonical query string according to AWS specs
func CanonicalQueryString(queryParams map[string][]string) string {
	// 1. Sort parameter names in ascending order
	keys := make([]string, 0, len(queryParams))
	for k := range queryParams {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 2. Build canonical query string
	var pairs []string
	for _, key := range keys {
		values := queryParams[key]
		sort.Strings(values) // Sort values for each key

		// URI encode both key and values
		encodedKey := UriEncode(key, true)
		for _, v := range values {
			encodedValue := UriEncode(v, true)
			pairs = append(pairs, fmt.Sprintf("%s=%s", encodedKey, encodedValue))
		}
	}

	return strings.Join(pairs, "&")
}
