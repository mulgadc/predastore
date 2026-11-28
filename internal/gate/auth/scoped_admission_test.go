package auth_test

import (
	"net/http"
	"testing"

	"errors"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A scoped config key is admitted like any other principal on an IAM-connected
// gate: an active account record passes, a missing record is refused with
// AccountProblem, an unavailable directory degrades to InternalError, and the
// policy-less system key is untouched by all of it.
func TestScopedConfigKeyAccountAdmission(t *testing.T) {
	activeRecord := []byte(`{"account_id":"` + auth.TestSessionAccount + `","status":"ACTIVE"}`)

	build := func(t *testing.T, accounts map[string][]byte, getErr error) http.Handler {
		t.Helper()
		iam := auth.NewAdmissionTestProvider(t, accounts, getErr)
		config, err := auth.NewConfigProvider([]auth.Entry{
			{
				AccessKeyID:     "AKIASCOPE",
				SecretAccessKey: "secret",
				AccountID:       auth.TestSessionAccount,
				Policy:          []auth.PolicyRule{{Bucket: charPrivateBucket, Actions: []string{"s3:GetObject", "s3:ListBucket"}}},
			},
			{
				AccessKeyID:     "AKIAROOT",
				SecretAccessKey: "secret",
				AccountID:       auth.TestSessionAccount,
			},
		})
		require.NoError(t, err)
		return charGate(t, auth.NewChainProvider(iam, config))
	}

	t.Run("active record passes", func(t *testing.T) {
		h := build(t, map[string][]byte{auth.TestSessionAccount: activeRecord}, nil)
		assert.Equal(t, replyOK, headPrivate(t, h, "AKIASCOPE", "secret"))
	})

	t.Run("missing record refuses the scoped key", func(t *testing.T) {
		h := build(t, map[string][]byte{}, nil)
		reply := headPrivate(t, h, "AKIASCOPE", "secret")
		assert.Equal(t, http.StatusForbidden, reply.Status)
		assert.Equal(t, "AccountProblem", reply.Code)
	})

	t.Run("unavailable directory degrades to InternalError", func(t *testing.T) {
		h := build(t, map[string][]byte{auth.TestSessionAccount: activeRecord}, errors.New("kv down"))
		reply := headPrivate(t, h, "AKIASCOPE", "secret")
		assert.Equal(t, http.StatusInternalServerError, reply.Status)
	})

	t.Run("the policy-less key is unaffected by the account gate", func(t *testing.T) {
		h := build(t, nil, errors.New("kv down"))
		assert.Equal(t, replyOK, headPrivate(t, h, "AKIAROOT", "secret"))
	})
}
