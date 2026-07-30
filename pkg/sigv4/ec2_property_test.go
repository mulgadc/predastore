package sigv4_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httputil"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/mulgadc/predastore/pkg/sigv4"
	"pgregory.net/rapid"
)

// ec2Runes covers the same stress set as harshRunes plus the literal bytes a base64-encoded
// blob field contributes ('+','/','=') and raw '%', which a naive double-decode would mangle.
var ec2Runes = []rune("aZ9-_.~ +/=%&@:?#!$'()*,;é你☃\x00\x01")

func ec2Harsh(minLen, maxLen int) *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		out := make([]rune, rapid.IntRange(minLen, maxLen).Draw(t, "n"))
		for i := range out {
			out[i] = ec2Runes[rapid.IntRange(0, len(ec2Runes)-1).Draw(t, "r")]
		}
		return string(out)
	})
}

// ec2FormBody draws a form-urlencoded body shaped like the query-protocol requests EC2 (and
// every other AWS "query" service: IAM, STS, SQS, ELB...) sends: an Action, a handful of
// params, and one deliberately oversized value to stand in for ImportKeyPair's base64 blob.
func ec2FormBody() *rapid.Generator[[]byte] {
	return rapid.Custom(func(t *rapid.T) []byte {
		v := url.Values{}
		v.Set("Action", "ImportKeyPair")
		v.Set("Version", "2016-11-15")
		nParams := rapid.IntRange(0, 4).Draw(t, "nParams")
		for range nParams {
			key := rapid.StringMatching(`[A-Za-z][A-Za-z0-9]{0,10}`).Draw(t, "key")
			val := ec2Harsh(0, 20).Draw(t, "val")
			v.Set(key, val)
		}
		// A large base64-ish value, standing in for PublicKeyMaterial on a big RSA key.
		big := ec2Harsh(0, 3000).Draw(t, "bigVal")
		v.Set("PublicKeyMaterial", big)
		return []byte(v.Encode())
	})
}

// wireRoundTrip serializes req exactly as it would go out on the wire, then reads it back the
// way a server does: Host and Content-Length land on the struct, not the header map. This is
// the fidelity gap between "sign a Go *http.Request in-process" and "verify what a server saw".
func wireRoundTrip(t *rapid.T, req *http.Request) *http.Request {
	dump, err := httputil.DumpRequestOut(req, true)
	if err != nil {
		t.Fatalf("DumpRequestOut: %v", err)
	}
	out, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(dump)))
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	return out
}

// TestEC2QueryProtocolOracle covers the query-protocol shape reqGen never generates: a
// form-encoded POST with no X-Amz-Content-Sha256 header. Every draw is signed by the SDK's
// own v4.Signer and pushed through a wire round-trip before verification.
func TestEC2QueryProtocolOracle(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		body := ec2FormBody().Draw(t, "body")
		region := rapid.SampledFrom([]string{"us-east-1", "eu-west-1", "ap-southeast-2"}).Draw(t, "region")

		req, err := http.NewRequest(http.MethodPost, "https://ec2."+region+".amazonaws.com/", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.ContentLength = int64(len(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		creds := aws.Credentials{AccessKeyID: oracleAKID, SecretAccessKey: oracleSecret}

		// Query-protocol clients sign the real body hash but never set
		// X-Amz-Content-Sha256 (that header is S3-specific); mirror that exactly.
		bodySum := sha256.Sum256(body)
		sum := hex.EncodeToString(bodySum[:])
		if err := v4.NewSigner().SignHTTP(context.Background(), creds, req, sum, "ec2", region, oracleTime); err != nil {
			t.Fatalf("SignHTTP: %v", err)
		}

		// http.ReadRequest (inside wireRoundTrip) already gives a live Body reader and a
		// Content-Length populated from the dumped wire bytes, exactly as a real server sees it.
		serverReq := wireRoundTrip(t, req)

		signed, err := sigv4.Parse(serverReq, sigv4.WithTime(oracleTime))
		if err != nil {
			t.Fatalf("Parse: %v (body=%q)", err, body)
		}

		if _, err := signed.Verify(oracleSecret, region, "ec2"); err != nil {
			t.Fatalf("Verify: %v (body=%q, SignedHeaders=%v)", err, body, signed.Canonical.SignedHeaders)
		}
	})
}
