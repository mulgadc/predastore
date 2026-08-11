package auth

import (
	"testing"
	"time"

	"github.com/mulgadc/predastore/pkg/masterkey"
)

// Re-exports of the session fixtures for the external auth_test package, which
// drives the real provider through the gate's middleware and so cannot
// reach the unexported helpers. export_test.go is compiled into the test
// binary alone, so none of this is package API.

const (
	TestSessionAKID    = testSessionAKID
	TestSessionAccount = testSessionAccount
	TestSessionRoleARN = testSessionRoleARN
	AllowAllS3Policy   = allowAllS3Policy
	DenyAllS3Policy    = denyAllS3Policy
)

func LoadTestKey(t *testing.T) *masterkey.Key { return loadTestKey(t) }

func NewSessionProvider(k *masterkey.Key, sessions, users, roles, policies map[string][]byte) *NATSIAMProvider {
	return newSessionProvider(k, sessions, users, roles, policies)
}

func UserSessionFixture(t *testing.T, k *masterkey.Key, secret string, expiresAt time.Time) (sessions, users, policies map[string][]byte) {
	return userSessionFixture(t, k, secret, expiresAt)
}

func AssumedRoleSession(t *testing.T, k *masterkey.Key, secret, roleARN string, expiresAt time.Time) map[string][]byte {
	return assumedRoleSession(t, k, secret, roleARN, expiresAt)
}

func RoleWithPolicy(t *testing.T, policyName, policyDoc string) (roles, policies map[string][]byte) {
	return roleWithPolicy(t, policyName, policyDoc)
}
