package gate

//test:in-package — the owner override is only meaningful behind the auth
// middleware, and the stub credential provider and fake meta store that let a
// service account and an ordinary caller be told apart are unexported.

import (
	"encoding/xml"
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

// listBucketsServer holds one bucket for each of three accounts, so a listing
// that leaks across accounts is visible rather than merely unproven.
func listBucketsServer(t *testing.T) *Server {
	t.Helper()
	return newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta: newFakeMeta(t,
			model.BucketMetadata{Name: "owner-bucket", Region: "ap-southeast-2", AccountID: acctOwner},
			model.BucketMetadata{Name: "other-bucket", Region: "ap-southeast-2", AccountID: acctOther},
			model.BucketMetadata{Name: "predastore", Region: "ap-southeast-2", AccountID: acctSys},
		),
		CredProv: &stubCredProvider{creds: map[string]*auth.CredentialResult{
			keyOwner:  {SecretAccessKey: secret, AccountID: acctOwner, PolicyDocuments: []iampolicy.PolicyDocument{allowAllPolicy}},
			keyConfig: {SecretAccessKey: secret, AccountID: acctSys, SkipPolicyCheck: true},
		}},
	})
}

// listBuckets signs GET / as accessKey, optionally asking for another owner,
// and returns the status and the bucket names in the response.
func listBuckets(t *testing.T, server *Server, accessKey, requestedOwner string) (int, []string) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if requestedOwner != "" {
		req.Header.Set(handlers.OwnerAccountHeader, requestedOwner)
	}
	signTestReq(t, req, nil, accessKey, secret, "ap-southeast-2", "s3")

	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		return rr.Code, nil
	}

	var result handlers.ListBucketsResult
	require.NoError(t, xml.NewDecoder(rr.Body).Decode(&result))

	names := make([]string, 0, len(result.Buckets))
	for _, bucket := range result.Buckets {
		names = append(names, bucket.Name)
	}
	return rr.Code, names
}

// This is the whole point of the change: spinifex holds a service credential
// that can already open any named bucket, but could not discover which buckets
// a tenant owns, so account teardown had nothing to delete.
func TestListBucketsServiceAccountCanListAnotherAccountsBuckets(t *testing.T) {
	server := listBucketsServer(t)

	status, names := listBuckets(t, server, keyConfig, acctOwner)

	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, []string{"owner-bucket"}, names)
}

// Without the header a service account is still just an account, so it sees
// the platform buckets and nothing else.
func TestListBucketsServiceAccountWithoutAnOwnerSeesOnlyItsOwn(t *testing.T) {
	server := listBucketsServer(t)

	status, names := listBuckets(t, server, keyConfig, "")

	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, []string{"predastore"}, names)
}

// The gate on the service credential is the entire security property. An
// ordinary caller sending the same header must learn nothing at all about the
// account it asked for — so the header is ignored rather than refused, which
// would itself distinguish a real account from a made-up one.
func TestListBucketsIgnoresTheOwnerHeaderFromAnOrdinaryCaller(t *testing.T) {
	server := listBucketsServer(t)

	status, names := listBuckets(t, server, keyOwner, acctOther)

	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, []string{"owner-bucket"}, names)
	assert.NotContains(t, names, "other-bucket")
}

func TestListBucketsOrdinaryCallerSeesOnlyItsOwn(t *testing.T) {
	server := listBucketsServer(t)

	status, names := listBuckets(t, server, keyOwner, "")

	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, []string{"owner-bucket"}, names)
}

// There is no value that means "every account". A wildcard would turn one
// compromised service credential into a listing of the whole cluster, and it
// is refused by the same check that catches a typo.
func TestListBucketsRefusesAnOwnerThatIsNotAnAccountID(t *testing.T) {
	server := listBucketsServer(t)

	for _, requested := range []string{"*", "0000000000001", "00000000004a", "arn:aws:iam::000000000001:root"} {
		t.Run(requested, func(t *testing.T) {
			status, _ := listBuckets(t, server, keyConfig, requested)
			assert.Equal(t, http.StatusBadRequest, status)
		})
	}
}

// Defence in depth behind the middleware's own check: a request that reached
// the handler with no account must not fall through to an unfiltered scan. The
// nil meta client proves it refuses before reading anything.
func TestListBucketsFailsClosedWithoutAnAccount(t *testing.T) {
	rr := httptest.NewRecorder()
	handlers.ListBuckets(nil).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/", nil))

	require.Equal(t, http.StatusForbidden, rr.Code)
}

// A malformed owner that returned an empty listing would be read by account
// teardown as "this account owns no buckets", and the account would be deleted
// with its data left behind. Refusing is the difference between the two.
func TestListBucketsRefusesRatherThanReturningEmptyForABadOwner(t *testing.T) {
	server := listBucketsServer(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(handlers.OwnerAccountHeader, "not-an-account")
	signTestReq(t, req, nil, keyConfig, secret, "ap-southeast-2", "s3")

	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	require.Equal(t, http.StatusBadRequest, rr.Code)

	var result handlers.S3Error
	require.NoError(t, xml.NewDecoder(rr.Body).Decode(&result))
	assert.Equal(t, string(model.ErrInvalidArgument), result.Code)
}

// An account with no buckets answers with an empty listing rather than an
// error, so teardown can tell "nothing to do" from "could not tell".
func TestListBucketsReturnsEmptyForAnAccountWithNoBuckets(t *testing.T) {
	server := listBucketsServer(t)

	status, names := listBuckets(t, server, keyConfig, "000000000099")

	require.Equal(t, http.StatusOK, status)
	assert.Empty(t, names)
}
