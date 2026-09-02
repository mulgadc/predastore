//test:in-package — conditionKeys is unexported, and the gate exists to compare
// what it emits with the evaluator's registries and with the other door's set.

package gate

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/iampolicy"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/stretchr/testify/assert"
)

// The AWS gateway's key set, mirrored from requestConditionKeys. It is written
// out rather than imported because the two doors are separate services; the
// gateway's own gate asserts the same list, so a key added there without adding
// it here fails on that side.
var gatewayDoorKeys = []string{
	iampolicy.KeySecureTransport,
	iampolicy.KeyUsername,
	iampolicy.KeyPrincipalAccount,
	iampolicy.KeySourceIP,
}

// s3:prefix is the only key this door adds over the gateway's set: it is the one
// piece of request context that exists on the S3 data plane and nowhere else.
var s3GateExtraKeys = []string{iampolicy.KeyS3Prefix}

// Every operator the evaluator implements, so "is this key usable in a policy"
// is asked of the whole allowlist rather than one operator of it.
var allOperators = []string{
	iampolicy.OpStringEquals,
	iampolicy.OpStringLike,
	iampolicy.OpIPAddress,
	iampolicy.OpBool,
}

// emittedKeys drives conditionKeys with everything a request can carry, over the
// actions and principal types that vary the result, and returns the union.
func emittedKeys(t *testing.T) map[string]string {
	t.Helper()

	creds := map[string]*auth.CredentialResult{
		"user":         {AccountID: "000000000001", UserName: "alice", PrincipalType: "user"},
		"assumed-role": {AccountID: "000000000001", UserName: "session", PrincipalType: "assumed-role"},
	}

	union := make(map[string]string)
	for name, cred := range creds {
		for _, action := range []string{"s3:ListBucket", "s3:GetObject"} {
			r := httptest.NewRequest(http.MethodGet, "/reports?prefix=home/", nil)
			r.TLS = &tls.ConnectionState{}
			r.RemoteAddr = "192.0.2.10:41288"
			for key := range conditionKeys(r, action, cred) {
				union[key] = name + "/" + action
			}
		}
	}
	return union
}

// A key this door supplies that no policy can name is carried to the evaluator
// and dropped there, which is the silent half of a registry disagreement.
func TestConditionKeys_EveryEmittedKeyIsUsableInAPolicy(t *testing.T) {
	for key, source := range emittedKeys(t) {
		supported := false
		for _, op := range allOperators {
			if iampolicy.SupportedCondition(op, key) {
				supported = true
				break
			}
		}
		_, unresolvable := iampolicy.UnsupportedVariable("${" + key + "}")
		assert.True(t, supported || !unresolvable,
			"conditionKeys emits %q for %s, but the evaluator neither enforces a condition on it nor "+
				"substitutes it: a policy naming it can never fire", key, source)
	}
}

// The S3 gate must resolve everything the AWS gateway does, plus s3:prefix. A
// key present at one door and not the other makes the same policy document mean
// different things, which is the disagreement this pair of gates exists to stop.
func TestConditionKeys_IsTheGatewaySetPlusS3Prefix(t *testing.T) {
	emitted := make([]string, 0, len(gatewayDoorKeys)+len(s3GateExtraKeys))
	for key := range emittedKeys(t) {
		emitted = append(emitted, key)
	}

	assert.ElementsMatch(t, append(append([]string{}, gatewayDoorKeys...), s3GateExtraKeys...), emitted,
		"the S3 gate key set changed: update the mirror in spinifex's "+
			"gateway/conditionkeys_completeness_test.go and the door table in bluebottle's door_test.go")
}
