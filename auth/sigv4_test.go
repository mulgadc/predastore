package auth_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mulgadc/predastore/auth"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// Shared credentials pinned from the AWS SigV4 streaming worked
// examples on:
//   - https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
//   - https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming-trailers.html
const (
	exAccessKeyID = "AKIAIOSFODNN7EXAMPLE"
	exSecret      = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	exRegion      = "us-east-1"
	exService     = "s3"
)

// exTime is the published request timestamp shared by every worked
// example: 20130524T000000Z = "Fri, 24 May 2013 00:00:00 GMT".
var exTime = time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)

// TestProp_SignParseVerifyRoundtrip asserts our package is internally
// consistent: any request signed by SignReq is accepted by
// ParseReq+Verify, and the wire Authorization header is preserved
// across the Verify call (which mutates and restores it internally).
// Inputs are restricted to spec-valid territory — header names that
// don't contain the SigV4 separators ';' and ',', and credential-scope
// fields free of '/' (extra scope parts), ' ', and ',' (Authorization
// top-level separator). Outside that space SigV4's textual format is
// genuinely ambiguous and no parser could recover the original
// components unambiguously.
func TestProp_SignParseVerifyRoundtrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		method := rapid.SampledFrom([]string{
			http.MethodGet, http.MethodPut, http.MethodPost,
			http.MethodHead, http.MethodDelete, http.MethodPatch,
			http.MethodOptions,
		}).Draw(t, "method")
		path := rapid.String().Draw(t, "path")
		rawQuery := rapid.String().Draw(t, "rawQuery")
		host := rapid.String().Draw(t, "host")
		body := rapid.SliceOfN(rapid.Byte(), 0, 8192).Draw(t, "body")
		headers := rapid.MapOf(rapid.String(), rapid.SliceOf(rapid.String())).
			Filter(func(m map[string][]string) bool {
				for k := range m {
					if strings.ContainsAny(k, ";,") {
						return false
					}
					switch http.CanonicalHeaderKey(k) {
					case "Authorization", "X-Amz-Date":
						return false
					}
				}
				return true
			}).Draw(t, "headers")
		credSafe := func(s string) bool { return s != "" && !strings.ContainsAny(s, "/, ") }
		accessKey := rapid.String().Filter(credSafe).Draw(t, "accessKey")
		secret := rapid.String().Draw(t, "secret")
		region := rapid.String().Filter(credSafe).Draw(t, "region")
		service := rapid.String().Filter(credSafe).Draw(t, "service")

		signTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

		sum := sha256.Sum256(body)
		payloadHash := hex.EncodeToString(sum[:])

		req, err := http.NewRequest(method, "http://h/", bytes.NewReader(body))
		require.NoError(t, err)
		req.URL.Path = path
		req.URL.RawQuery = rawQuery
		req.Host = host
		for k, vs := range headers {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
		require.NoError(t, auth.SignReq(
			req, accessKey, secret, payloadHash, service, region,
			auth.WithTime(signTime),
		))
		authHdr := req.Header.Get("Authorization")

		sig, err := auth.ParseReq(req)
		require.NoError(t, err, "Parse")
		require.NoError(t,
			sig.Verify(secret, service, region, auth.WithTime(signTime)),
			"Verify",
		)
		require.Equal(t, authHdr, req.Header.Get("Authorization"),
			"Verify must preserve Authorization header")
	})
}

// TestProp_TamperingFailsVerification asserts that any single mutation
// applied to a successfully signed request causes Parse+Verify to
// reject. Each trial signs a valid random request, picks one mutation
// from a fixed catalog, applies it, then expects either Parse or Verify
// to return an error. Acceptable rejection sites: Parse (for
// structurally-detected tampering like a missing required SignedHeader)
// or Verify (for crypto-detected tampering like a body-hash change).
func TestProp_TamperingFailsVerification(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		method := rapid.SampledFrom([]string{
			http.MethodGet, http.MethodPut, http.MethodPost,
			http.MethodHead, http.MethodDelete, http.MethodPatch,
			http.MethodOptions,
		}).Draw(t, "method")
		path := rapid.String().Draw(t, "path")
		rawQuery := rapid.String().Draw(t, "rawQuery")
		host := rapid.String().Draw(t, "host")
		body := rapid.SliceOfN(rapid.Byte(), 0, 8192).Draw(t, "body")
		headers := rapid.MapOf(rapid.String(), rapid.SliceOf(rapid.String())).
			Filter(func(m map[string][]string) bool {
				for k := range m {
					if strings.ContainsAny(k, ";,") {
						return false
					}
					switch http.CanonicalHeaderKey(k) {
					case "Authorization", "X-Amz-Date", "X-Tamper-Probe":
						return false
					}
				}
				return true
			}).Draw(t, "headers")
		credSafe := func(s string) bool { return s != "" && !strings.ContainsAny(s, "/, ") }
		accessKey := rapid.String().Filter(credSafe).Draw(t, "accessKey")
		secret := rapid.String().Draw(t, "secret")
		region := rapid.String().Filter(credSafe).Draw(t, "region")
		service := rapid.String().Filter(credSafe).Draw(t, "service")

		signTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

		sum := sha256.Sum256(body)
		payloadHash := hex.EncodeToString(sum[:])

		req, err := http.NewRequest(method, "http://h/", bytes.NewReader(body))
		require.NoError(t, err)
		req.URL.Path = path
		req.URL.RawQuery = rawQuery
		req.Host = host
		for k, vs := range headers {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}

		require.NoError(t, auth.SignReq(
			req, accessKey, secret, payloadHash, service, region,
			auth.WithTime(signTime),
		))

		mutation := rapid.SampledFrom([]string{
			"flip-signature",
			"change-method",
			"tamper-path",
			"tamper-query",
			"tamper-host",
			"tamper-content-sha",
			"add-header",
			"drop-host-from-signed-headers",
		}).Draw(t, "mutation")

		switch mutation {
		case "flip-signature":
			// Last char of Signature= is hex; flip to a guaranteed-different hex char.
			h := req.Header.Get("Authorization")
			last := h[len(h)-1]
			swap := byte('0')
			if last == '0' {
				swap = '1'
			}
			req.Header.Set("Authorization", h[:len(h)-1]+string(swap))
		case "change-method":
			if req.Method == http.MethodGet {
				req.Method = http.MethodPost
			} else {
				req.Method = http.MethodGet
			}
		case "tamper-path":
			req.URL.Path = req.URL.Path + "/tampered"
		case "tamper-query":
			if req.URL.RawQuery == "" {
				req.URL.RawQuery = "tampered=1"
			} else {
				req.URL.RawQuery = req.URL.RawQuery + "&tampered=1"
			}
		case "tamper-host":
			req.Host = req.Host + ".tampered"
		case "tamper-content-sha":
			// All-zero hash will never match a real body hash (2^-256 collision).
			req.Header.Set("X-Amz-Content-Sha256",
				"0000000000000000000000000000000000000000000000000000000000000000")
		case "add-header":
			// Header name was filtered out of random gen, so this is always new.
			req.Header.Set("X-Tamper-Probe", "1")
		case "drop-host-from-signed-headers":
			// Remove the "host" entry from the wire Authorization's
			// SignedHeaders list. Hits ErrMissingSignedHeader via ParseReq.
			ah := req.Header.Get("Authorization")
			start := strings.Index(ah, "SignedHeaders=") + len("SignedHeaders=")
			end := strings.Index(ah[start:], ", ")
			listEnd := start + end
			list := ah[start:listEnd]
			var kept []string
			for h := range strings.SplitSeq(list, ";") {
				if h != "host" {
					kept = append(kept, h)
				}
			}
			req.Header.Set("Authorization", ah[:start]+strings.Join(kept, ";")+ah[listEnd:])
		}

		sig, perr := auth.ParseReq(req)
		if perr != nil {
			return // Parse rejection is a valid rejection.
		}
		verr := sig.Verify(secret, service, region, auth.WithTime(signTime))
		require.Error(t, verr, "mutation %q should be rejected", mutation)
	})
}

// TestStreamingSentinels confirms the streaming X-Amz-Content-Sha256
// sentinels round-trip through Sign + Parse + Verify. Byte-equivalence
// with the docs' published signature values is the SDK's
// responsibility; this test only checks that our wrapper forwards the
// sentinel string through unchanged and that Verify accepts the
// resulting request. Per-chunk signatures live with the chunk decoder,
// not this package.
func TestStreamingSentinels(t *testing.T) {
	tests := []struct {
		name       string
		sentinel   string
		extraHdr   map[string]string
		contentLen int64
	}{
		{
			name:     "Payload",
			sentinel: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
			extraHdr: map[string]string{
				"X-Amz-Storage-Class":          "REDUCED_REDUNDANCY",
				"Content-Encoding":             "aws-chunked",
				"X-Amz-Decoded-Content-Length": "66560",
			},
			// Docs example signs Content-Length: 66824.
			contentLen: 66824,
		},
		{
			name:     "PayloadTrailer",
			sentinel: "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
			extraHdr: map[string]string{
				"X-Amz-Storage-Class":          "REDUCED_REDUNDANCY",
				"Content-Encoding":             "aws-chunked",
				"X-Amz-Decoded-Content-Length": "66560",
				"X-Amz-Trailer":                "x-amz-checksum-crc32c",
			},
			// Trailer example omits content-length from SignedHeaders.
			contentLen: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut,
				"/examplebucket/chunkObject.txt", nil)
			req.Host = "s3.amazonaws.com"
			for k, v := range tt.extraHdr {
				req.Header.Set(k, v)
			}
			req.ContentLength = tt.contentLen

			require.NoError(t, auth.SignReq(req,
				exAccessKeyID, exSecret,
				tt.sentinel, exService, exRegion,
				auth.WithTime(exTime)))

			sig, err := auth.ParseReq(req)
			require.NoError(t, err)
			require.NoError(t, sig.Verify(exSecret, exService, exRegion,
				auth.WithTime(exTime)))
		})
	}
}
