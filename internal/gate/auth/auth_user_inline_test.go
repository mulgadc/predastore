package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResolveUserPolicies_UserInlineAllow proves a user's own inline policy
// resolves and authorizes on the S3 data path — the user-inline linchpin that
// keeps the S3 decision in lockstep with spinifex's GetUserPolicies.
func TestResolveUserPolicies_UserInlineAllow(t *testing.T) {
	users := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName:       "alice",
			AccountID:      inlineTestAccount,
			InlinePolicies: map[string]string{"AllowS3": allowAllS3Policy},
		}),
	}
	p := newGroupProvider(users, map[string][]byte{}, nil)

	docs, _, err := p.resolveUserPolicies(context.Background(), inlineTestAccount, "alice")
	require.NoError(t, err)
	require.Len(t, docs, 1, "user-inline Allow must resolve")
	assert.True(t, allowed("s3:ListBucket", "arn:aws:s3:::any", docs),
		"user-inline Allow must be honoured")
}

// aws:userid comes back from the record the policies were resolved from, so the
// door supplies it without a second lookup. A record predating the field yields
// an empty ID, which omits the key rather than supplying one that matches
// nothing.
func TestResolveUserPolicies_ReturnsTheUserID(t *testing.T) {
	withID := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName: "alice", UserID: "AIDAALICE", AccountID: inlineTestAccount,
		}),
	}
	_, userID, err := newGroupProvider(withID, map[string][]byte{}, nil).
		resolveUserPolicies(context.Background(), inlineTestAccount, "alice")
	require.NoError(t, err)
	assert.Equal(t, "AIDAALICE", userID)

	legacy := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName: "alice", AccountID: inlineTestAccount,
		}),
	}
	_, userID, err = newGroupProvider(legacy, map[string][]byte{}, nil).
		resolveUserPolicies(context.Background(), inlineTestAccount, "alice")
	require.NoError(t, err)
	assert.Empty(t, userID)
}

// TestResolveUserPolicies_UserInlineDenyOverridesManagedAllow proves a user's
// direct managed Allow and its own inline Deny are evaluated together, with the
// inline Deny winning — the standard IAM deny-wins outcome. A user-inline Deny
// dropped on the S3 path would be a split-brain over-permit.
func TestResolveUserPolicies_UserInlineDenyOverridesManagedAllow(t *testing.T) {
	users := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName:         "alice",
			AccountID:        inlineTestAccount,
			AttachedPolicies: []string{directAllowARN},
			InlinePolicies:   map[string]string{"DenyS3": denyAllS3Policy},
		}),
	}
	policies := map[string][]byte{
		inlineTestAccount + ".DirectAllowS3": mustMarshal(t, iamPolicy{
			PolicyName: "DirectAllowS3", PolicyDocument: allowAllS3Policy,
		}),
	}
	p := newGroupProvider(users, policies, nil)

	docs, _, err := p.resolveUserPolicies(context.Background(), inlineTestAccount, "alice")
	require.NoError(t, err)
	require.Len(t, docs, 2, "direct Allow and user-inline Deny must both resolve")
	assert.False(t, allowed("s3:ListBucket", "arn:aws:s3:::any", docs),
		"user-inline Deny must override direct Allow (deny-wins)")
}

// TestResolveUserPolicies_UserInlineMalformedFailsClosed proves a corrupt inline
// document on the user fails closed rather than silently dropping it, matching the
// group/role inline handling via the shared resolveInlinePolicies helper.
func TestResolveUserPolicies_UserInlineMalformedFailsClosed(t *testing.T) {
	users := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName:       "alice",
			AccountID:      inlineTestAccount,
			InlinePolicies: map[string]string{"Broken": "{not json"},
		}),
	}
	p := newGroupProvider(users, map[string][]byte{}, nil)

	_, _, err := p.resolveUserPolicies(context.Background(), inlineTestAccount, "alice")
	assert.Error(t, err, "a malformed user inline document must fail closed")
}
