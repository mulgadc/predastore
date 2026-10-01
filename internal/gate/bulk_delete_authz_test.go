package gate

//test:in-package — drives the whole server so the middleware's per-key
// authorizer and the batch handler are exercised together, through the stub
// credential provider and fake meta store.

import (
	"bytes"
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

// bulkDelete batch-deletes keys from owner-bucket as a principal holding
// statements, and returns the keys deleted and the keys refused.
func bulkDelete(t *testing.T, statements []iampolicy.Statement, keys ...string) (deleted, denied []string) {
	t.Helper()
	server := newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta: newFakeMeta(t,
			model.BucketMetadata{Name: "owner-bucket", Region: "ap-southeast-2", AccountID: acctOwner, OwnerID: keyCreator}),
		CredProv: &stubCredProvider{creds: map[string]*auth.CredentialResult{
			keyOwner: {SecretAccessKey: secret, AccountID: acctOwner, PolicyDocuments: []iampolicy.PolicyDocument{{
				Version:   "2012-10-17",
				Statement: statements,
			}}},
		}},
	})

	var body bytes.Buffer
	body.WriteString("<Delete>")
	for _, key := range keys {
		body.WriteString("<Object><Key>" + key + "</Key></Object>")
	}
	body.WriteString("</Delete>")
	req := httptest.NewRequest(http.MethodPost, "/owner-bucket?delete=", bytes.NewReader(body.Bytes()))
	signTestReq(t, req, body.Bytes(), keyOwner, secret, "ap-southeast-2", "s3")

	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var result handlers.DeleteResult
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &result))
	for _, d := range result.Deleted {
		deleted = append(deleted, d.Key)
	}
	for _, e := range result.Errors {
		require.Equal(t, string(model.ErrAccessDenied), e.Code, e.Key)
		denied = append(denied, e.Key)
	}
	return deleted, denied
}

// An exclusion deeper than the bucket must hold for a batch as it does for a
// single delete; authorizing the batch once against bucket/* would grant it.
func TestBulkDelete_AllowNotResourceRefusesTheExcludedKey(t *testing.T) {
	deleted, denied := bulkDelete(t, []iampolicy.Statement{{
		Effect:      "Allow",
		Action:      iampolicy.StringOrArr{"s3:*"},
		NotResource: iampolicy.StringOrArr{"arn:aws:s3:::owner-bucket/secret/*"},
	}}, "public/a", "secret/b")

	assert.Equal(t, []string{"public/a"}, deleted)
	assert.Equal(t, []string{"secret/b"}, denied)
}

// A Deny fencing a prefix must reach the keys under it inside a batch, even
// beside an Allow on the whole bucket.
func TestBulkDelete_ScopedDenyRefusesTheFencedKey(t *testing.T) {
	deleted, denied := bulkDelete(t, []iampolicy.Statement{
		allowAllPolicy.Statement[0],
		{
			Effect:   "Deny",
			Action:   iampolicy.StringOrArr{"s3:DeleteObject"},
			Resource: iampolicy.StringOrArr{"arn:aws:s3:::owner-bucket/secret/*"},
		},
	}, "public/a", "secret/b")

	assert.Equal(t, []string{"public/a"}, deleted)
	assert.Equal(t, []string{"secret/b"}, denied)
}

// A grant on part of the bucket deletes that part of a batch, as AWS does,
// rather than refusing the whole request.
func TestBulkDelete_PrefixGrantDeletesOnlyThatPrefix(t *testing.T) {
	deleted, denied := bulkDelete(t, []iampolicy.Statement{{
		Effect:   "Allow",
		Action:   iampolicy.StringOrArr{"s3:DeleteObject"},
		Resource: iampolicy.StringOrArr{"arn:aws:s3:::owner-bucket/public/*"},
	}}, "public/a", "secret/b")

	assert.Equal(t, []string{"public/a"}, deleted)
	assert.Equal(t, []string{"secret/b"}, denied)
}
