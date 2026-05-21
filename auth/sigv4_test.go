package auth_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
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
			sig.Verify(secret, service, region,
				auth.WithTime(signTime),
				auth.WithBodyHash(payloadHash)),
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

		// Headers added post-signing (Accept-Encoding, Content-Length, ...)
		// are tolerated by Verify per TestVerify_TransportAddedHeaders, so
		// "add-header" is not a tampering mutation.
		mutation := rapid.SampledFrom([]string{
			"flip-signature",
			"change-method",
			"tamper-path",
			"tamper-query",
			"tamper-host",
			"tamper-content-sha",
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
		verr := sig.Verify(secret, service, region,
			auth.WithTime(signTime),
			auth.WithBodyHash(payloadHash))
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

// TestVerify_WithBodyHash pins WithBodyHash's three contracts:
//  1. matching hex hash accepts;
//  2. mismatching hex hash returns ErrBodyHashMismatch;
//  3. sentinel payload headers (UNSIGNED-PAYLOAD, STREAMING-*) skip
//     the check regardless of what WithBodyHash carries.
func TestVerify_WithBodyHash(t *testing.T) {
	body := []byte("hello world")
	sum := sha256.Sum256(body)
	bodyHashHex := hex.EncodeToString(sum[:])
	wrongHashHex := strings.Repeat("0", 64)

	tests := []struct {
		name        string
		payloadHash string // header value at sign time
		verifyOpt   string // WithBodyHash arg
		wantErrIs   error  // nil = accept
	}{
		{"matching hash accepts", bodyHashHex, bodyHashHex, nil},
		{"wrong hash rejects", bodyHashHex, wrongHashHex, auth.ErrBodyHashMismatch},
		{"UNSIGNED-PAYLOAD skips check", "UNSIGNED-PAYLOAD", wrongHashHex, nil},
		{"STREAMING-PAYLOAD skips check", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD", wrongHashHex, nil},
		{"STREAMING-PAYLOAD-TRAILER skips check", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER", wrongHashHex, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "/bucket/key", bytes.NewReader(body))
			req.Host = "s3.amazonaws.com"
			require.NoError(t, auth.SignReq(req,
				exAccessKeyID, exSecret, tt.payloadHash, exService, exRegion,
				auth.WithTime(exTime)))

			sig, err := auth.ParseReq(req)
			require.NoError(t, err)

			err = sig.Verify(exSecret, exService, exRegion,
				auth.WithTime(exTime),
				auth.WithBodyHash(tt.verifyOpt))

			if tt.wantErrIs == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErrIs)
			}
		})
	}
}

// TestVerify_MissingContentSHAFallback exercises the non-S3 SDK path:
// EC2/IAM/ELBv2 clients sign with a payload hash in the canonical
// request but omit X-Amz-Content-Sha256 from the wire. Verify must
// accept these when WithBodyHash supplies the same hash, and still
// reject when no bodyHash is provided.
func TestVerify_MissingContentSHAFallback(t *testing.T) {
	body := []byte("Action=DescribeInstances&Version=2016-11-15")
	sum := sha256.Sum256(body)
	bodyHashHex := hex.EncodeToString(sum[:])

	// Sign without setting X-Amz-Content-Sha256 to mirror non-S3 SDK
	// clients (EC2 etc.): the SDK passes payloadHash into the canonical
	// request but does not emit it as a wire header.
	signNoHeader := func() *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		req.Host = "ec2.amazonaws.com"
		require.NoError(t, v4.NewSigner().SignHTTP(
			context.Background(),
			aws.Credentials{AccessKeyID: exAccessKeyID, SecretAccessKey: exSecret},
			req, bodyHashHex, "ec2", exRegion, exTime,
		))
		require.Empty(t, req.Header.Get("X-Amz-Content-Sha256"))
		return req
	}

	t.Run("accepts when WithBodyHash supplies the hash", func(t *testing.T) {
		sig, err := auth.ParseReq(signNoHeader())
		require.NoError(t, err)
		require.NoError(t, sig.Verify(exSecret, "ec2", exRegion,
			auth.WithTime(exTime), auth.WithBodyHash(bodyHashHex)))
	})

	t.Run("rejects when no bodyHash is supplied", func(t *testing.T) {
		sig, err := auth.ParseReq(signNoHeader())
		require.NoError(t, err)
		err = sig.Verify(exSecret, "ec2", exRegion, auth.WithTime(exTime))
		require.ErrorIs(t, err, auth.ErrMissingContentSHA)
	})

	t.Run("rejects when bodyHash diverges from what client signed", func(t *testing.T) {
		sig, err := auth.ParseReq(signNoHeader())
		require.NoError(t, err)
		err = sig.Verify(exSecret, "ec2", exRegion,
			auth.WithTime(exTime),
			auth.WithBodyHash(strings.Repeat("0", 64)))
		require.ErrorIs(t, err, auth.ErrSignatureMismatch)
	})
}

// TestVerify_TransportAddedHeaders pins the contract that headers
// added after signing by the HTTP transport (Accept-Encoding,
// Content-Length, ...) must not cause re-signing to over-include
// them in SignedHeaders. boto3/aws-cli sign content-type;host;
// x-amz-date but the wire request gains Accept-Encoding etc. before
// the server reads it.
func TestVerify_TransportAddedHeaders(t *testing.T) {
	body := []byte("Action=DescribeInstances")
	sum := sha256.Sum256(body)
	bodyHashHex := hex.EncodeToString(sum[:])

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Host = "ec2.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	require.NoError(t, v4.NewSigner().SignHTTP(
		context.Background(),
		aws.Credentials{AccessKeyID: exAccessKeyID, SecretAccessKey: exSecret},
		req, bodyHashHex, "ec2", exRegion, exTime,
	))

	// Simulate transport-added headers post-signing.
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Content-Length", "23")
	req.Header.Set("User-Agent", "aws-cli/2.0")

	sig, err := auth.ParseReq(req)
	require.NoError(t, err)
	require.NoError(t, sig.Verify(exSecret, "ec2", exRegion,
		auth.WithTime(exTime),
		auth.WithBodyHash(bodyHashHex)))

	// Transport-added headers must still be on the request after Verify.
	require.Equal(t, "identity", req.Header.Get("Accept-Encoding"))
	require.Equal(t, "23", req.Header.Get("Content-Length"))
	require.Equal(t, "aws-cli/2.0", req.Header.Get("User-Agent"))
}
