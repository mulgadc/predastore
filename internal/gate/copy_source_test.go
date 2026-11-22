package gate

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

const (
	keySecretDenied = "AKIANOSECRET"
	keyNoVersions   = "AKIANOVERSIONS"
)

// copyServer serves two accounts' buckets, and two callers in the owner's account
// whose identity policies narrow what they may read.
func copyServer(t *testing.T) *Server {
	t.Helper()
	denySecret := iampolicy.PolicyDocument{Version: iampolicy.Version2012, Statement: []iampolicy.Statement{
		allowAllPolicy.Statement[0],
		{Effect: "Deny", Action: iampolicy.StringOrArr{"s3:GetObject"}, Resource: iampolicy.StringOrArr{"arn:aws:s3:::owner-bucket/secret/*"}},
	}}
	noVersions := iampolicy.PolicyDocument{Version: iampolicy.Version2012, Statement: []iampolicy.Statement{{
		Effect: "Allow", Action: iampolicy.StringOrArr{"s3:GetObject", "s3:PutObject"}, Resource: iampolicy.StringOrArr{"*"},
	}}}
	return newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta: newFakeMeta(t,
			model.BucketMetadata{Name: "owner-bucket", Region: "ap-southeast-2", AccountID: acctOwner},
			model.BucketMetadata{Name: "other-bucket", Region: "ap-southeast-2", AccountID: acctOther},
			model.BucketMetadata{Name: "public-bucket", Region: "ap-southeast-2", AccountID: acctOwner, Public: true},
		),
		CredProv: &stubCredProvider{creds: map[string]*auth.CredentialResult{
			keyOwner:        {SecretAccessKey: secret, AccountID: acctOwner, PolicyDocuments: []iampolicy.PolicyDocument{allowAllPolicy}},
			keyOther:        {SecretAccessKey: secret, AccountID: acctOther, PolicyDocuments: []iampolicy.PolicyDocument{allowAllPolicy}},
			keyConfig:       {SecretAccessKey: secret, AccountID: acctSys, SkipPolicyCheck: true},
			keySecretDenied: {SecretAccessKey: secret, AccountID: acctOwner, PolicyDocuments: []iampolicy.PolicyDocument{denySecret}},
			keyNoVersions:   {SecretAccessKey: secret, AccountID: acctOwner, PolicyDocuments: []iampolicy.PolicyDocument{noVersions}},
		}},
	})
}

// copyReq is CopyObject, or UploadPartCopy when part is set, of source into dest.
func copyReq(t *testing.T, dest, source string, part bool, accessKey string, endpoint *iampolicy.PolicyDocument) *http.Request {
	t.Helper()
	if part {
		dest += "?partNumber=1&uploadId=u"
	}
	req := httptest.NewRequest(http.MethodPut, dest, nil)
	req.Header.Set("X-Amz-Copy-Source", source)
	if endpoint != nil {
		h := testHeader()
		req.RemoteAddr = h.Source.String()
		req = req.WithContext(withEndpoint(req.Context(), h, endpoint))
	}
	signTestReq(t, req, nil, accessKey, secret, "ap-southeast-2", "s3")
	return req
}

// A copy reads its source, so the source is authorized as a read, as on AWS:
// identity policy, the source bucket's ownership and, through an endpoint, the
// endpoint policy. Both copy operations are held the same way.
func TestCopySource_IsAuthorizedAsARead(t *testing.T) {
	denySourceDoc, err := iampolicy.ParseEndpointPolicy([]byte(`{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"*"},` +
		`{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::owner-bucket/secret/*"}]}`))
	require.NoError(t, err)
	server := copyServer(t)

	for name, tc := range map[string]struct {
		dest, source, key string
		endpoint          *iampolicy.PolicyDocument
		allowed           bool
	}{
		"own source":                      {"/owner-bucket/b", "/owner-bucket/secret/a", keyOwner, nil, true},
		"another account's source":        {"/other-bucket/b", "/owner-bucket/open/a", keyOther, nil, false},
		"another account's public source": {"/other-bucket/b", "/public-bucket/a", keyOther, nil, true},
		"identity policy denies source":   {"/owner-bucket/b", "/owner-bucket/secret/a", keySecretDenied, nil, false},
		"identity policy allows source":   {"/owner-bucket/b", "/owner-bucket/open/a", keySecretDenied, nil, true},
		"versioned source needs GetObjectVersion": {
			"/owner-bucket/b", "/owner-bucket/open/a?versionId=v1", keyNoVersions, nil, false},
		"current source needs GetObject only": {"/owner-bucket/b", "/owner-bucket/open/a", keyNoVersions, nil, true},
		"endpoint policy denies source":       {"/owner-bucket/b", "/owner-bucket/secret/a", keyOwner, &denySourceDoc, false},
		"endpoint policy denies a service account's source": {
			"/owner-bucket/b", "/owner-bucket/secret/a", keyConfig, &denySourceDoc, false},
		"endpoint policy allows source": {"/owner-bucket/b", "/owner-bucket/open/a", keyOwner, &denySourceDoc, true},
	} {
		for _, part := range []bool{false, true} {
			status, nextCalled, body := runMiddleware(t, server, copyReq(t, tc.dest, tc.source, part, tc.key, tc.endpoint))
			assert.Equal(t, tc.allowed, nextCalled, "%s (part copy %v): %d %s", name, part, status, body)
			if !tc.allowed {
				assert.Equal(t, http.StatusForbidden, status, "%s (part copy %v)", name, part)
			}
		}
	}
}

// The middleware and the handler read the source from one parser, so the
// source authorized is the source copied.
func TestCopySource_MalformedSourceIsLeftToTheHandler(t *testing.T) {
	_, _, _, err := handlers.ParseCopySource("/no-key")
	require.Error(t, err)
	_, nextCalled, _ := runMiddleware(t, copyServer(t), copyReq(t, "/owner-bucket/b", "/no-key", false, keyOwner, nil))
	assert.True(t, nextCalled)
}

// Each multipart operation AWS names on its own is decided by that action, at
// the identity and endpoint checks, so a deny on it is not escaped through the
// object action it once shared, and a deny on that object action no longer
// reaches it.
func TestMultipart_OperationsAuthorizeAsTheirOwnActions(t *testing.T) {
	for _, op := range []struct{ method, path, action, shared string }{
		{http.MethodGet, "/owner-bucket/k?uploadId=u", "s3:ListMultipartUploadParts", "s3:GetObject"},
		{http.MethodDelete, "/owner-bucket/k?uploadId=u", "s3:AbortMultipartUpload", "s3:DeleteObject"},
		{http.MethodGet, "/owner-bucket?uploads", "s3:ListBucketMultipartUploads", "s3:ListBucket"},
	} {
		for name, tc := range map[string]struct {
			denied   string
			endpoint bool
			allowed  bool
		}{
			"identity deny on the action":        {op.action, false, false},
			"endpoint deny on the action":        {op.action, true, false},
			"identity deny on the object action": {op.shared, false, true},
		} {
			identity := iampolicy.PolicyDocument{Version: iampolicy.Version2012, Statement: []iampolicy.Statement{allowAllPolicy.Statement[0]}}
			req := httptest.NewRequest(op.method, op.path, nil)
			if tc.endpoint {
				doc, err := iampolicy.ParseEndpointPolicy([]byte(`{"Version":"2012-10-17","Statement":[` +
					`{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"*"},` +
					`{"Effect":"Deny","Principal":"*","Action":"` + tc.denied + `","Resource":"*"}]}`))
				require.NoError(t, err)
				h := testHeader()
				req.RemoteAddr = h.Source.String()
				req = req.WithContext(withEndpoint(req.Context(), h, &doc))
			} else {
				identity.Statement = append(identity.Statement, iampolicy.Statement{
					Effect: "Deny", Action: iampolicy.StringOrArr{tc.denied}, Resource: iampolicy.StringOrArr{"*"}})
			}
			server := newTestGate(t, Config{
				Region: "ap-southeast-2",
				Meta:   newFakeMeta(t, model.BucketMetadata{Name: "owner-bucket", Region: "ap-southeast-2", AccountID: acctOwner}),
				CredProv: &stubCredProvider{creds: map[string]*auth.CredentialResult{
					keyOwner: {SecretAccessKey: secret, AccountID: acctOwner, PolicyDocuments: []iampolicy.PolicyDocument{identity}},
				}},
			})
			signTestReq(t, req, nil, keyOwner, secret, "ap-southeast-2", "s3")
			status, nextCalled, body := runMiddleware(t, server, req)
			assert.Equal(t, tc.allowed, nextCalled, "%s %s, %s: %d %s", op.method, op.path, name, status, body)
		}
	}
}
