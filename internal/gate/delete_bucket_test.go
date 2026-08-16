package gate

//test:in-package — this drives the whole server so the middleware's ownership
// decision and the handler's own guard are exercised together, and the stub
// credential provider and fake meta store it needs are unexported.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/iampolicy"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// keyCreator is the access key that created owner-bucket. Nothing signs with
// it: every case here is a principal other than the creator, which is the
// whole subject of the test.
const keyCreator = "AKIACREATOR"

// deleteBucketServer declares one config bucket and holds two tenant buckets
// in state, one per account, so a delete that crosses an account boundary is
// visible rather than merely unproven.
func deleteBucketServer(t *testing.T) *Server {
	t.Helper()
	return newTestGate(t, Config{
		Region: "ap-southeast-2",
		Buckets: []handlers.BucketConfig{{
			Name: "predastore", Region: "ap-southeast-2", AccountID: acctSys,
		}},
		Meta: newFakeMeta(t,
			model.BucketMetadata{Name: "owner-bucket", Region: "ap-southeast-2", AccountID: acctOwner, OwnerID: keyCreator},
			model.BucketMetadata{Name: "other-bucket", Region: "ap-southeast-2", AccountID: acctOther, OwnerID: keyCreator},
			model.BucketMetadata{Name: "predastore", Region: "ap-southeast-2", AccountID: acctSys, OwnerID: keyCreator},
		),
		CredProv: &stubCredProvider{creds: map[string]*auth.CredentialResult{
			keyOwner:  {SecretAccessKey: secret, AccountID: acctOwner, PolicyDocuments: []iampolicy.PolicyDocument{allowAllPolicy}},
			keyOther:  {SecretAccessKey: secret, AccountID: acctOther, PolicyDocuments: []iampolicy.PolicyDocument{allowAllPolicy}},
			keyConfig: {SecretAccessKey: secret, AccountID: acctSys, SkipPolicyCheck: true},
		}},
	})
}

func deleteBucket(t *testing.T, server *Server, bucket, accessKey string) (int, string) {
	t.Helper()
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, signedReq(t, http.MethodDelete, "/"+bucket, accessKey))
	return rr.Code, rr.Body.String()
}

// This is what blocked the account-teardown S3 reaper: spinifex signs with the
// config service credential, which can never be the key that created a
// tenant's bucket, so no tenant bucket could be removed at all.
func TestDeleteBucketAllowsAServiceAccount(t *testing.T) {
	server := deleteBucketServer(t)

	status, body := deleteBucket(t, server, "owner-bucket", keyConfig)

	require.Equal(t, http.StatusNoContent, status, body)
}

// Ownership is an account property. A bucket created by one user must not be
// undeletable by every other user in the same account.
func TestDeleteBucketAllowsAnotherUserInTheSameAccount(t *testing.T) {
	server := deleteBucketServer(t)

	status, body := deleteBucket(t, server, "owner-bucket", keyOwner)

	require.Equal(t, http.StatusNoContent, status, body)
}

// Widening who may delete must not widen it across accounts. The middleware is
// the gate, and this proves it still holds with the handler's check gone.
func TestDeleteBucketStillRefusesAnotherAccount(t *testing.T) {
	server := deleteBucketServer(t)

	status, body := deleteBucket(t, server, "owner-bucket", keyOther)

	require.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "AccessDenied")
}

// The config-declared buckets hold the deployment's own state — volume
// metadata, AMIs, key material — and are shared by every account. A service
// credential is exactly the caller the ownership check waves through, so the
// guard has to be explicit rather than a side effect of an owner comparison.
func TestDeleteBucketRefusesAConfigDeclaredBucketEvenForAServiceAccount(t *testing.T) {
	server := deleteBucketServer(t)

	status, body := deleteBucket(t, server, "predastore", keyConfig)

	require.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "AccessDenied")
}

func TestDeleteBucketReportsAMissingBucket(t *testing.T) {
	server := deleteBucketServer(t)

	status, body := deleteBucket(t, server, "never-existed", keyConfig)

	require.Equal(t, http.StatusNotFound, status)
	assert.Contains(t, body, "NoSuchBucket")
}
