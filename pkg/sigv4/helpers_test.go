package sigv4_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/mulgadc/predastore/pkg/sigv4"
	"pgregory.net/rapid"
)

// The sigv4 property tests use aws-sdk-go-v2's own SigV4 signer as an independent oracle: the
// SDK produces the requests a real client would send, and sigv4 must accept exactly those
// (TestVerifyAcceptsOracle) and reject any tampered variant (the fault tests). Both suites
// draw their requests from the same reqGen, so every widening of the input space lands in
// both at once. This file holds everything shared across the test files.

const (
	oracleAKID   = "AKIAIOSFODNN7EXAMPLE"
	oracleSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	oracleHost   = "bucket.example.com"
)

// oracleTime is the fixed signing instant; tests pin Parse's clock to it via WithTime so the
// credential date, X-Amz-Date, and skew checks are deterministic.
var oracleTime = time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

// s3Opts matches sigv4's URI handling: S3 single-encodes the path, other services double-encode.
func s3Opts(service string) []func(*v4.SignerOptions) {
	if service == "s3" {
		return []func(*v4.SignerOptions){func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true }}
	}

	return nil
}

// parseVerify runs the full sigv4 pipeline and returns the first error, so a test can match it
// against the sentinel it expects.
func parseVerify(req *http.Request, region, service string, now time.Time) error {
	signed, err := sigv4.Parse(req, sigv4.WithTime(now))
	if err != nil {
		return err
	}

	_, err = signed.Verify(oracleSecret, region, service)

	return err
}

// genRequest is a valid request shape drawn by reqGen, ready to be signed.
type genRequest struct {
	// New returns a fresh, unsigned request of this shape on every call, so a caller can sign and
	// mutate one copy without disturbing the next.
	New func() *http.Request

	Region    string
	Service   string // "s3" | "execute-api"
	Presigned bool
	Unsigned  bool // s3 only: sign UNSIGNED-PAYLOAD rather than the body hash
}

// harshRunes stresses percent-encoding and the unreserved-set boundary: unreserved chars, the
// reserved/sub-delims that must be escaped, and multibyte UTF-8. '/' is intentionally absent —
// it is a path separator controlled via segment joins, and %2F-in-a-key is a distinct S3
// nuance better left to a dedicated test.
var harshRunes = []rune("aZ9-_.~ +%=&@:?#!$'()*,;é你☃")

// harsh draws a string of length [minLen,maxLen] over harshRunes.
func harsh(minLen, maxLen int) *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		out := make([]rune, rapid.IntRange(minLen, maxLen).Draw(t, "n"))
		for i := range out {
			out[i] = harshRunes[rapid.IntRange(0, len(harshRunes)-1).Draw(t, "r")]
		}

		return string(out)
	})
}

// reqGen draws a valid, harsh request. Every draw must verify clean — that is exactly
// TestVerifyAcceptsOracle, the invariant the fault tests build on.
func reqGen() *rapid.Generator[genRequest] {
	return rapid.Custom(func(t *rapid.T) genRequest {
		service := rapid.SampledFrom([]string{"s3", "execute-api"}).Draw(t, "service")
		presigned := rapid.Bool().Draw(t, "presigned")
		region := rapid.SampledFrom([]string{"us-east-1", "eu-west-1", "ap-southeast-2"}).Draw(t, "region")

		// Presigned URLs are GET-only here; header auth exercises every method.
		method := http.MethodGet
		if !presigned {
			method = rapid.SampledFrom([]string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodHead}).Draw(t, "method")
		}

		// 1–3 segment object-key path. Each segment leads with an alphanumeric so it never
		// collapses to "." or ".." (which URL parsing folds, breaking the presigned rebuild).
		seg := rapid.Custom(func(t *rapid.T) string {
			return rapid.StringMatching(`[a-zA-Z0-9]`).Draw(t, "lead") + harsh(0, 7).Draw(t, "tail")
		})
		path := rapid.Custom(func(t *rapid.T) string {
			return "/" + strings.Join(rapid.SliceOfN(seg, 1, 3).Draw(t, "segs"), "/")
		}).Draw(t, "path")

		// Harsh query with empty and repeated values.
		rawQuery := rapid.Custom(func(t *rapid.T) url.Values {
			v := url.Values{}
			for i := 0; i < rapid.IntRange(0, 3).Draw(t, "nKeys"); i++ {
				key := harsh(1, 6).Draw(t, "qKey")
				// A key shadowing the presigned X-Amz-* parameters would invalidate the baseline.
				if strings.HasPrefix(strings.ToLower(key), "x-amz-") {
					key = "u" + key
				}

				for j := 0; j < rapid.IntRange(1, 2).Draw(t, "nVals"); j++ {
					v.Add(key, harsh(0, 8).Draw(t, "qVal")) // length 0 → empty value (?acl style)
				}
			}

			return v
		}).Draw(t, "query").Encode()

		// Presigned URLs sign only host: any extra signed header is lost on the rebuild from
		// the signed URI, and bodies are out of the presigned path.
		header := http.Header{}
		var body []byte
		if !presigned {
			// Signed headers: x-amz-meta-* plus common standard headers, single- and multi-valued.
			header = rapid.Custom(func(t *rapid.T) http.Header {
				h := http.Header{}
				for i := 0; i < rapid.IntRange(0, 3).Draw(t, "nHdr"); i++ {
					name := rapid.SampledFrom([]string{"meta", "Content-Type", "Content-Encoding", "Content-Language", "Cache-Control"}).Draw(t, "name")
					if name == "meta" {
						name = "X-Amz-Meta-" + rapid.StringMatching(`[A-Za-z0-9]{1,8}`).Draw(t, "meta")
					}

					for j := 0; j < rapid.IntRange(1, 2).Draw(t, "nVals"); j++ {
						// Wrap and internally separate values with ASCII spaces to exercise canonical
						// collapsing. Only 0x20: the SDK's StripExcessSpaces collapses spaces alone,
						// so tabs would diverge from sigv4's strings.Fields.
						pad := func(label string) string { return strings.Repeat(" ", rapid.IntRange(0, 2).Draw(t, label)) }
						tokens := rapid.SliceOfN(harsh(1, 6), 1, 3).Draw(t, "tokens")
						h.Add(name, pad("lead")+strings.Join(tokens, "  ")+pad("trail"))
					}
				}

				return h
			}).Draw(t, "header")
			body = rapid.SliceOfN(rapid.Byte(), 0, 64).Draw(t, "body")
		}

		// UNSIGNED-PAYLOAD is s3-only: presigned s3 always uses it, header-auth s3 sometimes.
		unsigned := service == "s3" && (presigned || rapid.Bool().Draw(t, "unsigned"))

		expires := 0
		if presigned {
			expires = rapid.IntRange(1, int(sigv4.MaxPresignAge/time.Second)).Draw(t, "expires")
		}

		return genRequest{
			Region:    region,
			Service:   service,
			Presigned: presigned,
			Unsigned:  unsigned,
			New: func() *http.Request {
				u := &url.URL{Scheme: "https", Host: oracleHost, Path: path, RawQuery: rawQuery}
				req, err := http.NewRequest(method, u.String(), bytes.NewReader(body))
				if err != nil {
					t.Fatalf("build request: %v", err)
				}

				// Clone so a fault mutating one instance's headers can't leak into the next.
				req.Header = header.Clone()
				if req.Header == nil {
					req.Header = http.Header{}
				}

				// PresignHTTP signs the expiry window but leaves the caller to set it.
				if presigned {
					q := req.URL.Query()
					q.Set("X-Amz-Expires", strconv.Itoa(expires))
					req.URL.RawQuery = q.Encode()
				}

				return req
			},
		}
	})
}

// sign returns an SDK-signed request for gr, ready for Parse — the independent oracle the tests
// verify against.
func sign(t *rapid.T, gr genRequest) *http.Request {
	req := gr.New()
	creds := aws.Credentials{AccessKeyID: oracleAKID, SecretAccessKey: oracleSecret}

	if gr.Presigned {
		// S3 presigns the UNSIGNED-PAYLOAD sentinel; other services sign the empty-body hash.
		payloadHash := sigv4.EmptyPayload
		if gr.Service == "s3" {
			payloadHash = string(sigv4.UnsignedPayload)
		}

		signedURI, _, err := v4.NewSigner().PresignHTTP(context.Background(), creds, req, payloadHash, gr.Service, gr.Region, oracleTime, s3Opts(gr.Service)...)
		if err != nil {
			t.Fatalf("PresignHTTP: %v", err)
		}

		// Rebuild from the signed URI, as a server receives it: auth in the query, no headers.
		out, err := http.NewRequest(http.MethodGet, signedURI, nil)
		if err != nil {
			t.Fatalf("rebuild presigned request: %v", err)
		}

		return out
	}

	// Header auth signs in place, so first reconstruct the payload hash the client would send.
	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	_ = req.Body.Close()

	// An s3 unsigned body signs the sentinel; everything else signs the body's SHA-256.
	payloadHash := string(sigv4.UnsignedPayload)
	if !gr.Unsigned {
		sum := sha256.Sum256(body)
		payloadHash = hex.EncodeToString(sum[:])
	}

	// Restore the body ReadAll drained, and set x-amz-content-sha256 so signer and Parse agree.
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	if err := v4.NewSigner().SignHTTP(context.Background(), creds, req, payloadHash, gr.Service, gr.Region, oracleTime, s3Opts(gr.Service)...); err != nil {
		t.Fatalf("SignHTTP: %v", err)
	}

	return req
}
