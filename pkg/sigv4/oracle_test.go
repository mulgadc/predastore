package sigv4_test

import (
	"testing"

	"pgregory.net/rapid"
)

// TestVerifyAcceptsOracle is the round-trip property: whatever the SDK signs, sigv4 must verify.
// reqGen spans both auth modes and harsh encodings, far past the fixed KAT vectors.
func TestVerifyAcceptsOracle(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		gr := reqGen().Draw(t, "req")
		if err := parseVerify(sign(t, gr), gr.Region, gr.Service, oracleTime); err != nil {
			t.Fatalf("SDK-signed request rejected: %v", err)
		}
	})
}
