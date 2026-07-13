package sigv4_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/mulgadc/predastore/pkg/sigv4"
	"pgregory.net/rapid"
)

// These tests use aws-sdk-go-v2's own SigV4 signer as an independent oracle: the SDK
// produces requests a real client would send, and sigv4 must accept exactly those. That
// makes the round-trip a differential test rather than a mirror of Verify's own logic.

const (
	oracleAKID   = "AKIAIOSFODNN7EXAMPLE"
	oracleSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	oracleHost   = "bucket.example.com"
)

// oracleTime is the fixed signing instant; tests pin Parse's clock to it via WithTime so
// the credential date, X-Amz-Date, and skew checks are deterministic.
var oracleTime = time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

// fataler is the slice of testing.TB that the signing helpers need, satisfied by both
// *testing.T and *rapid.T so the helpers work in table and property tests alike.
type fataler interface {
	Fatalf(format string, args ...any)
}

// signHeader signs req with the SDK and returns it ready for Parse. payloadHash is the
// x-amz-content-sha256 value; empty means hash body. It is set as a header (as a real S3
// client does) so the SDK signs it and Parse finds it. The body is attached so a non-S3
// Parse can hash it (S3 ignores the body and reads the header verbatim).
func signHeader(tb fataler, method, rawURL string, body []byte, hdrs map[string]string, region, service, payloadHash string) *http.Request {
	req, err := http.NewRequest(method, rawURL, bytes.NewReader(body))
	if err != nil {
		tb.Fatalf("build request: %v", err)
	}
	for name, value := range hdrs {
		req.Header.Set(name, value)
	}
	if payloadHash == "" {
		sum := sha256.Sum256(body)
		payloadHash = hex.EncodeToString(sum[:])
	}
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.ContentLength = int64(len(body))

	if err := v4.NewSigner().SignHTTP(context.Background(), oracleCreds(), req, payloadHash, service, region, oracleTime, s3Opts(service)...); err != nil {
		tb.Fatalf("SignHTTP: %v", err)
	}
	return req
}

// presign signs a presigned-URL GET with the SDK and returns a request rebuilt from the
// signed URI, as a server would receive it.
func presign(tb fataler, rawURL string, expires int, region, service string) *http.Request {
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		tb.Fatalf("build request: %v", err)
	}
	query := req.URL.Query()
	query.Set("X-Amz-Expires", strconv.Itoa(expires))
	req.URL.RawQuery = query.Encode()

	signedURI, _, err := v4.NewSigner().PresignHTTP(context.Background(), oracleCreds(), req, string(sigv4.UnsignedPayload), service, region, oracleTime, s3Opts(service)...)
	if err != nil {
		tb.Fatalf("PresignHTTP: %v", err)
	}
	out, err := http.NewRequest(http.MethodGet, signedURI, nil)
	if err != nil {
		tb.Fatalf("rebuild presigned request: %v", err)
	}
	return out
}

func oracleCreds() aws.Credentials {
	return aws.Credentials{AccessKeyID: oracleAKID, SecretAccessKey: oracleSecret}
}

// s3Opts matches sigv4's URI handling: S3 single-encodes the path, other services double-encode.
func s3Opts(service string) []func(*v4.SignerOptions) {
	if service == "s3" {
		return []func(*v4.SignerOptions){func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true }}
	}
	return nil
}

// parseVerify runs the full sigv4 pipeline and returns the first error, so a test can match
// it against the sentinel it expects.
func parseVerify(req *http.Request, region, service string, now time.Time) error {
	signed, err := sigv4.Parse(req, region, service, sigv4.WithTime(now))
	if err != nil {
		return err
	}
	_, err = signed.Verify(oracleSecret)
	return err
}

// TestVerifyAcceptsOracle is the round-trip property: any request the SDK signs
// must verify. Randomizing method, path segments (incl. spaces), multi-value queries, extra
// signed headers, service, and payload mode exercises canonicalization far past the fixed
// vectors.
func TestVerifyAcceptsOracle(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		method := rapid.SampledFrom([]string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodHead}).Draw(t, "method")
		service := rapid.SampledFrom([]string{"s3", "execute-api"}).Draw(t, "service")
		region := rapid.SampledFrom([]string{"us-east-1", "eu-west-1", "ap-southeast-2"}).Draw(t, "region")

		// Leading alphanumeric keeps segments off "." and ".." (which URL parsing may fold).
		seg := rapid.StringMatching(`[a-zA-Z0-9][a-zA-Z0-9 ._~-]{0,7}`)
		segs := rapid.SliceOfN(seg, 1, 3).Draw(t, "segments")
		u := &url.URL{Scheme: "https", Host: oracleHost, Path: "/" + strings.Join(segs, "/")}

		query := url.Values{}
		keys := rapid.SliceOfN(rapid.StringMatching(`q[0-9]`), 0, 3).Draw(t, "queryKeys")
		for i, key := range keys {
			vals := rapid.SliceOfN(rapid.StringMatching(`[a-zA-Z0-9]{1,8}`), 1, 2).Draw(t, fmt.Sprintf("queryVals%d", i))
			for _, v := range vals {
				query.Add(key, v)
			}
		}
		u.RawQuery = query.Encode()

		hdrs := map[string]string{}
		nhdr := rapid.IntRange(0, 2).Draw(t, "headerCount")
		for i := range nhdr {
			name := "X-Amz-Meta-" + rapid.StringMatching(`[a-zA-Z0-9]{1,8}`).Draw(t, fmt.Sprintf("headerName%d", i))
			hdrs[name] = rapid.StringMatching(`[a-zA-Z0-9]{1,8}`).Draw(t, fmt.Sprintf("headerValue%d", i))
		}

		body := rapid.SliceOfN(rapid.Byte(), 0, 64).Draw(t, "body")
		// UNSIGNED-PAYLOAD is an S3-only sentinel. On a non-S3 service Parse hashes the
		// body regardless, so a request the SDK signs with the sentinel is correctly a
		// mismatch there — restrict it to S3 so the oracle stays realistic.
		useUnsigned := rapid.Bool().Draw(t, "unsignedPayload")
		payloadHash := ""
		if service == "s3" && useUnsigned {
			payloadHash = string(sigv4.UnsignedPayload)
		}

		req := signHeader(t, method, u.String(), body, hdrs, region, service, payloadHash)
		if err := parseVerify(req, region, service, oracleTime); err != nil {
			t.Fatalf("SDK-signed request rejected: %v (url=%s)", err, u.String())
		}
	})
}

// TestVerifyAcceptsAddedHeader is the negative control for the fault suite: a header the
// client never signed (added in transit) must not affect verification.
func TestVerifyAcceptsAddedHeader(t *testing.T) {
	req := signHeader(t, http.MethodGet, "https://"+oracleHost+"/obj", nil, nil, "us-east-1", "s3", "")
	req.Header.Set("Accept", "application/xml")
	if err := parseVerify(req, "us-east-1", "s3", oracleTime); err != nil {
		t.Fatalf("verification rejected a request over an unsigned added header: %v", err)
	}
}
