package sigv4_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mulgadc/predastore/pkg/sigv4"
)

// Known-answer tests: every worked example AWS publishes for SigV4, checked against the
// exact signature in the docs. These pin canonicalization and signing-key derivation to
// AWS's own fixtures, independent of any signer.
//
//	https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
//	https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
//	https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
//	https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming-trailers.html
const (
	katAccessKeyID = "AKIAIOSFODNN7EXAMPLE"
	katSecret      = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	katScope       = "20130524/us-east-1/s3/aws4_request"
)

// katTime is the signing instant every AWS example shares (Fri, 24 May 2013 00:00:00 GMT);
// WithTime pins Parse's clock to it so the fixed 2013 signatures stay valid.
var katTime = time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)

func TestKnownAnswers(t *testing.T) {
	tests := []struct {
		name          string
		method        string
		host          string
		uri           string
		query         map[string]string
		headers       map[string]string
		signedHeaders []string
		contentLength int64 // signed content-length header, when non-zero
		signature     string
	}{
		{
			name:   "GET Object with Range",
			method: http.MethodGet,
			host:   "examplebucket.s3.amazonaws.com",
			uri:    "/test.txt",
			headers: map[string]string{
				"range":                "bytes=0-9",
				"x-amz-content-sha256": sigv4.EmptyPayload,
				"x-amz-date":           "20130524T000000Z",
			},
			signedHeaders: []string{"host", "range", "x-amz-content-sha256", "x-amz-date"},
			signature:     "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
		},
		{
			name:   "PUT Object",
			method: http.MethodPut,
			host:   "examplebucket.s3.amazonaws.com",
			uri:    "/test%24file.text",
			headers: map[string]string{
				"date":                 "Fri, 24 May 2013 00:00:00 GMT",
				"x-amz-content-sha256": "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
				"x-amz-date":           "20130524T000000Z",
				"x-amz-storage-class":  "REDUCED_REDUNDANCY",
			},
			signedHeaders: []string{"date", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-storage-class"},
			signature:     "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
		},
		{
			name:          "GET Bucket Lifecycle",
			method:        http.MethodGet,
			host:          "examplebucket.s3.amazonaws.com",
			uri:           "/",
			query:         map[string]string{"lifecycle": ""},
			headers:       map[string]string{"x-amz-content-sha256": sigv4.EmptyPayload, "x-amz-date": "20130524T000000Z"},
			signedHeaders: []string{"host", "x-amz-content-sha256", "x-amz-date"},
			signature:     "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543",
		},
		{
			name:          "GET Bucket (List Objects)",
			method:        http.MethodGet,
			host:          "examplebucket.s3.amazonaws.com",
			uri:           "/",
			query:         map[string]string{"max-keys": "2", "prefix": "J"},
			headers:       map[string]string{"x-amz-content-sha256": sigv4.EmptyPayload, "x-amz-date": "20130524T000000Z"},
			signedHeaders: []string{"host", "x-amz-content-sha256", "x-amz-date"},
			signature:     "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7",
		},
		{
			name:   "Presigned GET Object",
			method: http.MethodGet,
			host:   "examplebucket.s3.amazonaws.com",
			uri:    "/test.txt",
			query: map[string]string{
				"X-Amz-Algorithm":     "AWS4-HMAC-SHA256",
				"X-Amz-Credential":    katAccessKeyID + "/" + katScope,
				"X-Amz-Date":          "20130524T000000Z",
				"X-Amz-Expires":       "86400",
				"X-Amz-SignedHeaders": "host",
			},
			signedHeaders: []string{"host"},
			signature:     "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404",
		},
		{
			name:   "Streaming seed (PAYLOAD)",
			method: http.MethodPut,
			host:   "s3.amazonaws.com",
			uri:    "/examplebucket/chunkObject.txt",
			headers: map[string]string{
				"content-encoding":             "aws-chunked",
				"x-amz-content-sha256":         "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
				"x-amz-date":                   "20130524T000000Z",
				"x-amz-decoded-content-length": "66560",
				"x-amz-storage-class":          "REDUCED_REDUNDANCY",
			},
			signedHeaders: []string{"content-encoding", "content-length", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length", "x-amz-storage-class"},
			contentLength: 66824,
			signature:     "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",
		},
		{
			name:   "Streaming seed (PAYLOAD-TRAILER)",
			method: http.MethodPut,
			host:   "s3.amazonaws.com",
			uri:    "/examplebucket/chunkObject.txt",
			headers: map[string]string{
				"content-encoding":             "aws-chunked",
				"x-amz-content-sha256":         "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
				"x-amz-date":                   "20130524T000000Z",
				"x-amz-decoded-content-length": "66560",
				"x-amz-storage-class":          "REDUCED_REDUNDANCY",
				"x-amz-trailer":                "x-amz-checksum-crc32c",
			},
			signedHeaders: []string{"content-encoding", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-decoded-content-length", "x-amz-storage-class", "x-amz-trailer"},
			signature:     "106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Presigned fixtures carry the algorithm — and the signature — in the query.
			presigned := tc.query["X-Amz-Algorithm"] != ""

			values := url.Values{}
			for key, value := range tc.query {
				values.Set(key, value)
			}
			if presigned {
				values.Set("X-Amz-Signature", tc.signature)
			}

			rawURL := "https://" + tc.host + tc.uri
			if encoded := values.Encode(); encoded != "" {
				rawURL += "?" + encoded
			}

			req, err := http.NewRequest(tc.method, rawURL, nil)
			if err != nil {
				t.Fatalf("build request: %v", err)
			}

			req.Host = tc.host
			for name, value := range tc.headers {
				req.Header.Set(name, value)
			}
			if tc.contentLength != 0 {
				req.ContentLength = tc.contentLength
			}
			// Header-auth fixtures carry the signature in the Authorization header instead.
			if !presigned {
				req.Header.Set("Authorization", "AWS4-HMAC-SHA256 "+
					"Credential="+katAccessKeyID+"/"+katScope+","+
					"SignedHeaders="+strings.Join(tc.signedHeaders, ";")+","+
					"Signature="+tc.signature)
			}

			signed, err := sigv4.Parse(req, sigv4.WithTime(katTime))
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}

			if _, err := signed.Verify(katSecret, "us-east-1", "s3"); err != nil {
				t.Fatalf("Verify rejected the AWS-published signature: %v", err)
			}
		})
	}
}
