package sigv4_test

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mulgadc/predastore/pkg/sigv4"
)

// Worked SigV4 examples published by AWS S3, used here as fixed test vectors:
//   https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
//   https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
// Every example shares these pinned credentials and signing time. The timestamp
// is fixed in 2013, so WithTime pins Parse's clock-skew/presign-age reference to
// the signing time; the secret is the '/' variant the S3 doc actually signs with.
const (
	exampleAccessKeyID = "AKIAIOSFODNN7EXAMPLE"
	exampleSecret      = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	exampleRegion      = "us-east-1"
	exampleService     = "s3"
	exampleHost        = "examplebucket.s3.amazonaws.com"
	exampleScope       = "20130524/us-east-1/s3/aws4_request"
	emptyPayloadHash   = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

var exampleTime = time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)

func TestParseVerifyAWSExamples(t *testing.T) {
	tests := []struct {
		name          string
		method        string
		uri           string
		query         map[string]string
		headers       map[string]string
		signedHeaders []string
		body          string
		signature     string
	}{
		{
			name:   "GET Object with Range",
			method: http.MethodGet,
			uri:    "/test.txt",
			headers: map[string]string{
				"host":                 exampleHost,
				"range":                "bytes=0-9",
				"x-amz-content-sha256": emptyPayloadHash,
				"x-amz-date":           "20130524T000000Z",
			},
			signedHeaders: []string{"host", "range", "x-amz-content-sha256", "x-amz-date"},
			signature:     "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
		},
		{
			name:   "PUT Object with signed payload",
			method: http.MethodPut,
			uri:    "/test%24file.text",
			headers: map[string]string{
				"date":                 "Fri, 24 May 2013 00:00:00 GMT",
				"host":                 exampleHost,
				"x-amz-content-sha256": "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
				"x-amz-date":           "20130524T000000Z",
				"x-amz-storage-class":  "REDUCED_REDUNDANCY",
			},
			signedHeaders: []string{"date", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-storage-class"},
			body:          "Welcome to Amazon S3.",
			signature:     "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
		},
		{
			name:          "GET Bucket Lifecycle",
			method:        http.MethodGet,
			uri:           "/",
			query:         map[string]string{"lifecycle": ""},
			headers:       map[string]string{"host": exampleHost, "x-amz-content-sha256": emptyPayloadHash, "x-amz-date": "20130524T000000Z"},
			signedHeaders: []string{"host", "x-amz-content-sha256", "x-amz-date"},
			signature:     "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543",
		},
		{
			name:          "GET Bucket List Objects",
			method:        http.MethodGet,
			uri:           "/",
			query:         map[string]string{"max-keys": "2", "prefix": "J"},
			headers:       map[string]string{"host": exampleHost, "x-amz-content-sha256": emptyPayloadHash, "x-amz-date": "20130524T000000Z"},
			signedHeaders: []string{"host", "x-amz-content-sha256", "x-amz-date"},
			signature:     "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7",
		},
		{
			name:   "Presigned GET Object",
			method: http.MethodGet,
			uri:    "/test.txt",
			query: map[string]string{
				"X-Amz-Algorithm":     "AWS4-HMAC-SHA256",
				"X-Amz-Credential":    "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request",
				"X-Amz-Date":          "20130524T000000Z",
				"X-Amz-Expires":       "86400",
				"X-Amz-SignedHeaders": "host",
			},
			headers:       map[string]string{"host": exampleHost},
			signedHeaders: []string{"host"},
			signature:     "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Presigned requests carry the signature in the query string; header-authed
			// requests carry it in the Authorization header.
			presigned := tc.query["X-Amz-Algorithm"] != ""

			values := url.Values{}
			for key, value := range tc.query {
				values.Set(key, value)
			}
			if presigned {
				values.Set("X-Amz-Signature", tc.signature)
			}

			rawURL := "https://" + tc.headers["host"] + tc.uri
			if encoded := values.Encode(); encoded != "" {
				rawURL += "?" + encoded
			}

			newBody := func() io.Reader {
				if tc.body == "" {
					return http.NoBody
				}
				return strings.NewReader(tc.body)
			}

			req, err := http.NewRequest(tc.method, rawURL, newBody())
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			// host is delivered via req.Host, not the header map.
			req.Host = tc.headers["host"]
			for name, value := range tc.headers {
				if name == "host" {
					continue
				}
				req.Header.Set(name, value)
			}
			if !presigned {
				req.Header.Set("Authorization", "AWS4-HMAC-SHA256 "+
					"Credential="+exampleAccessKeyID+"/"+exampleScope+","+
					"SignedHeaders="+strings.Join(tc.signedHeaders, ";")+","+
					"Signature="+tc.signature)
			}

			signed, err := sigv4.Parse(req, exampleRegion, exampleService, sigv4.WithTime(exampleTime))
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}

			if _, err := signed.Verify(exampleSecret); err != nil {
				t.Fatalf("Verify rejected the AWS-published signature: %v", err)
			}

			// A different secret yields a different signature and must be rejected.
			if _, err := signed.Verify("wrong" + exampleSecret); !errors.Is(err, sigv4.ErrSignatureMismatch) {
				t.Fatalf("wrong secret: got %v, want ErrSignatureMismatch", err)
			}
		})
	}
}

// Parse must reject a request that leaves a header requiring a signature out of
// SignedHeaders: host, Content-MD5, or any x-amz-* header (except content-sha256).
func TestParseRejectsUnsignedHeaders(t *testing.T) {
	tests := []struct {
		name          string
		signedHeaders string
		mutate        func(*http.Request)
	}{
		{
			name:          "unsigned x-amz-* header",
			signedHeaders: "host;x-amz-date",
			mutate:        func(r *http.Request) { r.Header.Set("X-Amz-Meta-Foo", "bar") },
		},
		{
			name:          "unsigned Content-MD5",
			signedHeaders: "host;x-amz-date",
			mutate:        func(r *http.Request) { r.Header.Set("Content-Md5", "q2gWkQ==") },
		},
		{
			name:          "host not signed",
			signedHeaders: "x-amz-date",
			mutate:        func(r *http.Request) {},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, "https://"+exampleHost+"/test.txt", nil)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}
			req.Host = exampleHost
			req.Header.Set("X-Amz-Date", "20130524T000000Z")
			req.Header.Set("X-Amz-Content-Sha256", emptyPayloadHash)
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 "+
				"Credential="+exampleAccessKeyID+"/"+exampleScope+","+
				"SignedHeaders="+tc.signedHeaders+","+
				"Signature=deadbeef")
			tc.mutate(req)

			if _, err := sigv4.Parse(req, exampleRegion, exampleService, sigv4.WithTime(exampleTime)); !errors.Is(err, sigv4.ErrUnsignedHeader) {
				t.Fatalf("got %v, want ErrUnsignedHeader", err)
			}
		})
	}
}
