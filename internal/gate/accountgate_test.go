package gate

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/iampolicy"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gatedCredProvider answers account-status questions from a table and records who was asked.
type gatedCredProvider struct {
	stubCredProvider

	mu     sync.Mutex
	status map[string]error
	asked  []string
}

var _ auth.AccountGate = (*gatedCredProvider)(nil)

func (p *gatedCredProvider) CheckAccountActive(_ context.Context, accountID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.asked = append(p.asked, accountID)
	return p.status[accountID]
}

func accountGateServer(t *testing.T, status map[string]error) (*Server, *gatedCredProvider) {
	t.Helper()
	prov := &gatedCredProvider{
		status: status,
		stubCredProvider: stubCredProvider{creds: map[string]*auth.CredentialResult{
			keyOwner:  {SecretAccessKey: secret, AccountID: acctOwner, PolicyDocuments: []iampolicy.PolicyDocument{allowAllPolicy}},
			keyOther:  {SecretAccessKey: secret, AccountID: acctOther, PolicyDocuments: []iampolicy.PolicyDocument{allowAllPolicy}},
			keyConfig: {SecretAccessKey: secret, AccountID: acctSys, SkipPolicyCheck: true},
		}},
	}
	server := newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta: newFakeMeta(t,
			model.BucketMetadata{Name: "owner-bucket", Region: "ap-southeast-2", AccountID: acctOwner},
			model.BucketMetadata{Name: "public-bucket", Region: "ap-southeast-2", AccountID: acctOwner, Public: true},
		),
		Buckets:  []handlers.BucketConfig{{Name: "public-bucket", Region: "ap-southeast-2", AccountID: acctOwner, Public: true}},
		CredProv: prov,
	})
	return server, prov
}

func TestAccountGate_CallerNotActiveIsRefusedWithAccountProblem(t *testing.T) {
	server, _ := accountGateServer(t, map[string]error{acctOwner: auth.ErrAccountNotActive})
	status, nextCalled, body := runMiddleware(t, server, signedReq(t, http.MethodGet, "/owner-bucket", keyOwner))
	assert.False(t, nextCalled)
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "AccountProblem")
}

func TestAccountGate_UnreadableStatusIsInternalErrorNotAVerdict(t *testing.T) {
	server, _ := accountGateServer(t, map[string]error{acctOwner: errors.New("kv down")})
	status, nextCalled, body := runMiddleware(t, server, signedReq(t, http.MethodGet, "/owner-bucket", keyOwner))
	assert.False(t, nextCalled)
	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Contains(t, body, "InternalError")
	assert.NotContains(t, body, "AccountProblem")
}

func TestAccountGate_ActiveCallerPasses(t *testing.T) {
	server, prov := accountGateServer(t, nil)
	status, nextCalled, _ := runMiddleware(t, server, signedReq(t, http.MethodGet, "/owner-bucket", keyOwner))
	assert.True(t, nextCalled)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, []string{acctOwner}, prov.asked, "a same-account request asks about its account once")
}

// A public bucket lets another account read it, but not once the owner is closed.
func TestAccountGate_OwnerNotActiveRefusesAnOtherwisePermittedRead(t *testing.T) {
	server, _ := accountGateServer(t, map[string]error{acctOwner: auth.ErrAccountNotActive})
	status, nextCalled, body := runMiddleware(t, server, signedReq(t, http.MethodGet, "/public-bucket", keyOther))
	assert.False(t, nextCalled)
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "AllAccessDisabled")
}

func TestAccountGate_ActiveOwnerStillServesAPublicBucket(t *testing.T) {
	server, prov := accountGateServer(t, nil)
	status, nextCalled, _ := runMiddleware(t, server, signedReq(t, http.MethodGet, "/public-bucket", keyOther))
	assert.True(t, nextCalled)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, []string{acctOther, acctOwner}, prov.asked)
}

// An unsigned read of a public bucket must not keep a closed account's data reachable.
func TestAccountGate_UnsignedPublicReadRefusedWhenOwnerNotActive(t *testing.T) {
	server, _ := accountGateServer(t, map[string]error{acctOwner: auth.ErrAccountNotActive})
	req := httptest.NewRequest(http.MethodGet, "/public-bucket", nil)
	status, nextCalled, body := runMiddleware(t, server, req)
	assert.False(t, nextCalled)
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "AllAccessDisabled")
}

func TestAccountGate_UnsignedPublicReadServedWhenOwnerActive(t *testing.T) {
	server, prov := accountGateServer(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/public-bucket", nil)
	status, nextCalled, _ := runMiddleware(t, server, req)
	assert.True(t, nextCalled)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, []string{acctOwner}, prov.asked)
}

func TestAccountGate_UnsignedPublicReadOwnerUnreadableIsInternalError(t *testing.T) {
	server, _ := accountGateServer(t, map[string]error{acctOwner: errors.New("kv down")})
	req := httptest.NewRequest(http.MethodGet, "/public-bucket", nil)
	status, nextCalled, _ := runMiddleware(t, server, req)
	assert.False(t, nextCalled)
	assert.Equal(t, http.StatusInternalServerError, status)
}

func TestAccountGate_OwnerUnreadableIsInternalError(t *testing.T) {
	server, _ := accountGateServer(t, map[string]error{acctOwner: errors.New("kv down")})
	status, nextCalled, body := runMiddleware(t, server, signedReq(t, http.MethodGet, "/public-bucket", keyOther))
	assert.False(t, nextCalled)
	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Contains(t, body, "InternalError")
}

// A request that ownership already refuses must not reveal that the owner is closed.
func TestAccountGate_RefusedCrossAccountRequestNeverLearnsOwnerState(t *testing.T) {
	server, prov := accountGateServer(t, map[string]error{acctOwner: auth.ErrAccountNotActive})
	status, _, body := runMiddleware(t, server, signedReq(t, http.MethodGet, "/owner-bucket", keyOther))
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "AccessDenied")
	assert.NotContains(t, body, "AllAccessDisabled")
	assert.Equal(t, []string{acctOther}, prov.asked, "the owner's status is never consulted")
}

// Config-defined service accounts (the S3 reaper's platform key) bypass the gate as they bypass policy.
func TestAccountGate_ServiceAccountIsNeverAsked(t *testing.T) {
	server, prov := accountGateServer(t, map[string]error{
		acctSys: auth.ErrAccountNotActive, acctOwner: auth.ErrAccountNotActive,
	})
	status, nextCalled, _ := runMiddleware(t, server, signedReq(t, http.MethodGet, "/owner-bucket", keyConfig))
	assert.True(t, nextCalled)
	assert.Equal(t, http.StatusOK, status)
	assert.Empty(t, prov.asked)
}

// The gate trusts the account only of a verified signature.
func TestAccountGate_RunsAfterSignatureVerification(t *testing.T) {
	server, prov := accountGateServer(t, map[string]error{acctOwner: auth.ErrAccountNotActive})
	req := signedReq(t, http.MethodGet, "/owner-bucket", keyOwner)
	req.Header.Set("Authorization", req.Header.Get("Authorization")[:len(req.Header.Get("Authorization"))-4]+"0000")
	status, _, body := runMiddleware(t, server, req)
	assert.Equal(t, http.StatusForbidden, status)
	assert.Contains(t, body, "SignatureDoesNotMatch")
	assert.Empty(t, prov.asked, "an unverified request is never used to probe account status")
}

func TestAccountGate_ProviderWithoutAnAccountDirectoryChangesNothing(t *testing.T) {
	server := ownershipServer(t)
	status, nextCalled, _ := runMiddleware(t, server, signedReq(t, http.MethodGet, "/owner-bucket", keyOwner))
	assert.True(t, nextCalled)
	assert.Equal(t, http.StatusOK, status)
}

func TestRespondAccountGateError_Mapping(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		owner  bool
		status int
		code   string
	}{
		{"caller", auth.ErrAccountNotActive, false, http.StatusForbidden, "AccountProblem"},
		{"owner", auth.ErrAccountNotActive, true, http.StatusForbidden, "AllAccessDisabled"},
		{"unreadable caller", errors.New("x"), false, http.StatusInternalServerError, "InternalError"},
		{"unreadable owner", errors.New("x"), true, http.StatusInternalServerError, "InternalError"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			handlers.RespondAccountGateError(rr, httptest.NewRequest(http.MethodGet, "/b", nil), c.err, c.owner)
			require.Equal(t, c.status, rr.Code)
			assert.Contains(t, rr.Body.String(), c.code)
		})
	}
}
