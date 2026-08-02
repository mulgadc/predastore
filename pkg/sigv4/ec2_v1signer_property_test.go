package sigv4_test

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v1sigv4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/mulgadc/predastore/pkg/sigv4"
	"pgregory.net/rapid"
)

// TestEC2QueryProtocolOracle_V1Signer is TestEC2QueryProtocolOracle's counterpart using the
// terraform AWS provider's own signing stack (aws-sdk-go v1-lineage, github.com/aws/aws-sdk-go/
// aws/signer/v4) instead of aws-sdk-go-v2. mulga-zjy2g's ImportKeyPair SignatureDoesNotMatch
// bug was never reproduced against aws-sdk-go-v2 despite extensive property-fuzzing; this is
// the one signing stack that was never diffed against sigv4's canonicalisation. Same
// form-encoded POST shape, same harsh PublicKeyMaterial-like oversized value, same wire
// round-trip before verification.
func TestEC2QueryProtocolOracle_V1Signer(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		body := ec2FormBody().Draw(t, "body")
		region := rapid.SampledFrom([]string{"us-east-1", "eu-west-1", "ap-southeast-2"}).Draw(t, "region")

		req, err := http.NewRequest(http.MethodPost, "https://ec2."+region+".amazonaws.com/", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.ContentLength = int64(len(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		creds := credentials.NewStaticCredentials(oracleAKID, oracleSecret, "")
		signer := v1sigv4.NewSigner(creds)

		// v1's Signer.Sign reads body itself to compute the payload hash and does not touch
		// X-Amz-Content-Sha256 for a non-S3 service — the same query-protocol shape as the v2 oracle.
		if _, err := signer.Sign(req, bytes.NewReader(body), "ec2", region, oracleTime); err != nil {
			t.Fatalf("v1 Sign: %v", err)
		}

		serverReq := wireRoundTrip(t, req)

		signed, err := sigv4.Parse(serverReq, sigv4.WithTime(oracleTime))
		if err != nil {
			t.Fatalf("Parse: %v (body=%q)", err, body)
		}

		if _, err := signed.Verify(oracleSecret, region, "ec2"); err != nil {
			t.Fatalf("Verify: %v (body=%q, SignedHeaders=%v, CanonicalRequest=%q)",
				err, body, signed.Canonical.SignedHeaders, signed.CanonicalRequest())
		}
	})
}
