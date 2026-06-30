// Copyright 2025 Mulga Defense Corporation (MDC). All rights reserved.
// Use of this source code is governed by an Apache 2.0 license
// that can be found in the LICENSE file.

package s3

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- fake nats.JetStreamContext (embeds the interface; only KeyValue is exercised) ---

// fakeJS lets resolveGroupPolicies drive the lazy groups-bucket open without a
// live NATS connection: KeyValue returns the configured error.
type fakeJS struct {
	nats.JetStreamContext

	kvErr error
}

func (j *fakeJS) KeyValue(bucket string) (nats.KeyValue, error) { return nil, j.kvErr }

// --- group resolution helpers ---

const (
	groupManagedARN = "arn:aws:iam::" + inlineTestAccount + ":policy/GroupAllowS3"
	directAllowARN  = "arn:aws:iam::" + inlineTestAccount + ":policy/DirectAllowS3"
)

// newGroupProvider wires a NATSIAMProvider to fake users/policies/groups buckets
// for the AKIA group-resolution path. A nil groups map leaves the groups bucket
// unwired (groupsReady false), mirroring a provider that has not yet opened it.
func newGroupProvider(users, policies, groups map[string][]byte) *NATSIAMProvider {
	p := &NATSIAMProvider{
		cache:          make(map[string]*cachedCredential),
		done:           make(chan struct{}),
		usersBucket:    &fakeKV{data: users},
		policiesBucket: &fakeKV{data: policies},
		bucketsReady:   true,
	}
	if groups != nil {
		p.groupsBucket = &fakeKV{data: groups}
		p.groupsReady = true
	}
	return p
}

// TestResolveUserPolicies_GroupManagedAllow proves a managed policy attached to a
// user's group resolves and authorizes on the S3 data path, with no direct grant.
func TestResolveUserPolicies_GroupManagedAllow(t *testing.T) {
	users := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName:  "alice",
			AccountID: inlineTestAccount,
			Groups:    []string{"Engineers"},
		}),
	}
	groups := map[string][]byte{
		inlineTestAccount + ".Engineers": mustMarshal(t, iamGroup{
			GroupName:        "Engineers",
			AccountID:        inlineTestAccount,
			AttachedPolicies: []string{groupManagedARN},
		}),
	}
	policies := map[string][]byte{
		inlineTestAccount + ".GroupAllowS3": mustMarshal(t, iamPolicy{
			PolicyName: "GroupAllowS3", PolicyDocument: allowAllS3Policy,
		}),
	}
	p := newGroupProvider(users, policies, groups)

	docs, err := p.resolveUserPolicies(inlineTestAccount, "alice")
	require.NoError(t, err)
	require.Len(t, docs, 1, "group-managed Allow must resolve")
	assert.True(t, evaluateS3Access("s3:ListBucket", "arn:aws:s3:::any", docs),
		"group-managed Allow must be honoured")
}

// TestResolveUserPolicies_GroupInlineAllow proves an inline policy embedded in a
// user's group resolves and authorizes — the inline-via-group linchpin.
func TestResolveUserPolicies_GroupInlineAllow(t *testing.T) {
	users := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName: "alice", AccountID: inlineTestAccount, Groups: []string{"Engineers"},
		}),
	}
	groups := map[string][]byte{
		inlineTestAccount + ".Engineers": mustMarshal(t, iamGroup{
			GroupName:      "Engineers",
			AccountID:      inlineTestAccount,
			InlinePolicies: map[string]string{"AllowS3": allowAllS3Policy},
		}),
	}
	p := newGroupProvider(users, map[string][]byte{}, groups)

	docs, err := p.resolveUserPolicies(inlineTestAccount, "alice")
	require.NoError(t, err)
	require.Len(t, docs, 1, "group-inline Allow must resolve")
	assert.True(t, evaluateS3Access("s3:ListBucket", "arn:aws:s3:::any", docs),
		"group-inline Allow must be honoured")
}

// TestResolveUserPolicies_GroupDenyOverridesDirectAllow proves direct and
// group-inline documents are evaluated together: a group Deny overrides a direct
// Allow, the standard IAM deny-wins outcome across direct + group.
func TestResolveUserPolicies_GroupDenyOverridesDirectAllow(t *testing.T) {
	users := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName:         "alice",
			AccountID:        inlineTestAccount,
			AttachedPolicies: []string{directAllowARN},
			Groups:           []string{"Restricted"},
		}),
	}
	groups := map[string][]byte{
		inlineTestAccount + ".Restricted": mustMarshal(t, iamGroup{
			GroupName:      "Restricted",
			AccountID:      inlineTestAccount,
			InlinePolicies: map[string]string{"DenyS3": denyAllS3Policy},
		}),
	}
	policies := map[string][]byte{
		inlineTestAccount + ".DirectAllowS3": mustMarshal(t, iamPolicy{
			PolicyName: "DirectAllowS3", PolicyDocument: allowAllS3Policy,
		}),
	}
	p := newGroupProvider(users, policies, groups)

	docs, err := p.resolveUserPolicies(inlineTestAccount, "alice")
	require.NoError(t, err)
	require.Len(t, docs, 2, "direct Allow and group Deny must both resolve")
	assert.False(t, evaluateS3Access("s3:ListBucket", "arn:aws:s3:::any", docs),
		"group Deny must override direct Allow (deny-wins)")
}

// TestResolveUserPolicies_CombineDirectAndGroup proves a direct managed policy and
// a group managed policy both contribute to the resolved set.
func TestResolveUserPolicies_CombineDirectAndGroup(t *testing.T) {
	users := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName:         "alice",
			AccountID:        inlineTestAccount,
			AttachedPolicies: []string{directAllowARN},
			Groups:           []string{"Engineers"},
		}),
	}
	groups := map[string][]byte{
		inlineTestAccount + ".Engineers": mustMarshal(t, iamGroup{
			GroupName:        "Engineers",
			AccountID:        inlineTestAccount,
			AttachedPolicies: []string{groupManagedARN},
		}),
	}
	policies := map[string][]byte{
		inlineTestAccount + ".DirectAllowS3": mustMarshal(t, iamPolicy{
			PolicyName: "DirectAllowS3", PolicyDocument: allowAllS3Policy,
		}),
		inlineTestAccount + ".GroupAllowS3": mustMarshal(t, iamPolicy{
			PolicyName: "GroupAllowS3", PolicyDocument: allowAllS3Policy,
		}),
	}
	p := newGroupProvider(users, policies, groups)

	docs, err := p.resolveUserPolicies(inlineTestAccount, "alice")
	require.NoError(t, err)
	assert.Len(t, docs, 2, "direct and group managed policies must both resolve")
}

// TestResolveUserPolicies_MissingGroupSkipped proves a membership to a group with
// no KV record is inert: the user's direct policies still resolve with no error.
func TestResolveUserPolicies_MissingGroupSkipped(t *testing.T) {
	users := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName:         "alice",
			AccountID:        inlineTestAccount,
			AttachedPolicies: []string{directAllowARN},
			Groups:           []string{"Ghost"}, // no record under inlineTestAccount.Ghost
		}),
	}
	policies := map[string][]byte{
		inlineTestAccount + ".DirectAllowS3": mustMarshal(t, iamPolicy{
			PolicyName: "DirectAllowS3", PolicyDocument: allowAllS3Policy,
		}),
	}
	p := newGroupProvider(users, policies, map[string][]byte{})

	docs, err := p.resolveUserPolicies(inlineTestAccount, "alice")
	require.NoError(t, err, "a missing group must be skipped, not fail")
	require.Len(t, docs, 1, "the user's direct policy must still resolve")
	assert.True(t, evaluateS3Access("s3:ListBucket", "arn:aws:s3:::any", docs))
}

// TestResolveUserPolicies_GroupInlineMalformedFailsClosed proves a corrupt inline
// document within a resolvable group fails closed rather than silently dropping it.
func TestResolveUserPolicies_GroupInlineMalformedFailsClosed(t *testing.T) {
	users := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName: "alice", AccountID: inlineTestAccount, Groups: []string{"BadGroup"},
		}),
	}
	groups := map[string][]byte{
		inlineTestAccount + ".BadGroup": mustMarshal(t, iamGroup{
			GroupName:      "BadGroup",
			AccountID:      inlineTestAccount,
			InlinePolicies: map[string]string{"Broken": "{not json"},
		}),
	}
	p := newGroupProvider(users, map[string][]byte{}, groups)

	_, err := p.resolveUserPolicies(inlineTestAccount, "alice")
	assert.Error(t, err, "a malformed group inline document must fail closed")
}

// TestResolveUserPolicies_GroupManagedMissingFailsClosed proves a group managed
// attachment pointing at a non-existent policy record fails closed.
func TestResolveUserPolicies_GroupManagedMissingFailsClosed(t *testing.T) {
	users := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName: "alice", AccountID: inlineTestAccount, Groups: []string{"Engineers"},
		}),
	}
	groups := map[string][]byte{
		inlineTestAccount + ".Engineers": mustMarshal(t, iamGroup{
			GroupName:        "Engineers",
			AccountID:        inlineTestAccount,
			AttachedPolicies: []string{groupManagedARN}, // no GroupAllowS3 policy record
		}),
	}
	p := newGroupProvider(users, map[string][]byte{}, groups)

	_, err := p.resolveUserPolicies(inlineTestAccount, "alice")
	assert.Error(t, err, "an unresolvable group managed policy must fail closed")
}

// TestResolveUserPolicies_GroupsBucketAbsent proves that when the groups bucket
// has not been created (groups-v1 not deployed), group resolution is skipped and
// the user keeps their direct grants with no error.
func TestResolveUserPolicies_GroupsBucketAbsent(t *testing.T) {
	users := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName:         "alice",
			AccountID:        inlineTestAccount,
			AttachedPolicies: []string{directAllowARN},
			Groups:           []string{"Engineers"},
		}),
	}
	policies := map[string][]byte{
		inlineTestAccount + ".DirectAllowS3": mustMarshal(t, iamPolicy{
			PolicyName: "DirectAllowS3", PolicyDocument: allowAllS3Policy,
		}),
	}
	// groups map nil → groupsReady false; js returns ErrBucketNotFound on open.
	p := newGroupProvider(users, policies, nil)
	p.js = &fakeJS{kvErr: nats.ErrBucketNotFound}

	docs, err := p.resolveUserPolicies(inlineTestAccount, "alice")
	require.NoError(t, err, "an absent groups bucket must skip group resolution, not fail")
	require.Len(t, docs, 1, "the user's direct policy must still resolve")
	assert.True(t, evaluateS3Access("s3:ListBucket", "arn:aws:s3:::any", docs))
	assert.False(t, p.groupsReady, "a failed open must not mark the groups bucket ready")
}

// TestResolveUserPolicies_GroupsBucketInfraFault proves a non-not-found error
// opening the groups bucket fails closed rather than silently dropping a Deny.
func TestResolveUserPolicies_GroupsBucketInfraFault(t *testing.T) {
	users := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName: "alice", AccountID: inlineTestAccount, Groups: []string{"Engineers"},
		}),
	}
	p := newGroupProvider(users, map[string][]byte{}, nil)
	p.js = &fakeJS{kvErr: errors.New("nats: connection closed")}

	_, err := p.resolveUserPolicies(inlineTestAccount, "alice")
	assert.Error(t, err, "a groups-bucket infra fault must fail closed")
}

// TestResolveUserPolicies_NoGroupsUnchanged proves a user with no groups never
// dereferences the groups bucket: resolution succeeds with the groups bucket
// unwired (nil) and groupsReady false.
func TestResolveUserPolicies_NoGroupsUnchanged(t *testing.T) {
	users := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName:         "alice",
			AccountID:        inlineTestAccount,
			AttachedPolicies: []string{directAllowARN},
		}),
	}
	policies := map[string][]byte{
		inlineTestAccount + ".DirectAllowS3": mustMarshal(t, iamPolicy{
			PolicyName: "DirectAllowS3", PolicyDocument: allowAllS3Policy,
		}),
	}
	p := newGroupProvider(users, policies, nil) // groupsBucket nil, groupsReady false, js nil

	docs, err := p.resolveUserPolicies(inlineTestAccount, "alice")
	require.NoError(t, err)
	require.Len(t, docs, 1, "a no-groups user resolves direct policies only")
	assert.True(t, evaluateS3Access("s3:ListBucket", "arn:aws:s3:::any", docs))
}

// TestResolveUserPolicies_MultipleGroups proves the per-group loop accumulates
// grants across more than one group and that a missing group in a NON-last
// position is skipped without aborting the groups that follow it. Single-group
// fixtures only ever exercise the skip as the final iteration, so this guards the
// skip path against a continue→break (or other early-exit) regression.
func TestResolveUserPolicies_MultipleGroups(t *testing.T) {
	users := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName:  "alice",
			AccountID: inlineTestAccount,
			Groups:    []string{"Ghost", "Engineers", "Auditors"}, // Ghost has no KV record
		}),
	}
	groups := map[string][]byte{
		inlineTestAccount + ".Engineers": mustMarshal(t, iamGroup{
			GroupName:        "Engineers",
			AccountID:        inlineTestAccount,
			AttachedPolicies: []string{groupManagedARN},
		}),
		inlineTestAccount + ".Auditors": mustMarshal(t, iamGroup{
			GroupName:      "Auditors",
			AccountID:      inlineTestAccount,
			InlinePolicies: map[string]string{"AllowS3": allowAllS3Policy},
		}),
	}
	policies := map[string][]byte{
		inlineTestAccount + ".GroupAllowS3": mustMarshal(t, iamPolicy{
			PolicyName: "GroupAllowS3", PolicyDocument: allowAllS3Policy,
		}),
	}
	p := newGroupProvider(users, policies, groups)

	docs, err := p.resolveUserPolicies(inlineTestAccount, "alice")
	require.NoError(t, err, "a missing group mid-list must be skipped, not abort resolution")
	require.Len(t, docs, 2,
		"the managed grant from Engineers and the inline grant from Auditors must both resolve past the skipped Ghost")
	assert.True(t, evaluateS3Access("s3:ListBucket", "arn:aws:s3:::any", docs))
}

// TestResolveUserPolicies_GroupKVError proves a non-NotFound fault on a per-group
// Get (the groups bucket is already open) fails closed, rather than being swallowed
// like a missing group — which would risk silently dropping a group Deny.
func TestResolveUserPolicies_GroupKVError(t *testing.T) {
	users := map[string][]byte{
		inlineTestAccount + ".alice": mustMarshal(t, iamUser{
			UserName: "alice", AccountID: inlineTestAccount, Groups: []string{"Engineers"},
		}),
	}
	// Non-nil groups map → groupsReady true; override the bucket to fault on Get.
	p := newGroupProvider(users, map[string][]byte{}, map[string][]byte{})
	p.groupsBucket = &fakeKV{getErr: errors.New("nats: connection closed")}

	_, err := p.resolveUserPolicies(inlineTestAccount, "alice")
	require.Error(t, err, "a per-group KV fault must fail closed, not be skipped")
	assert.False(t, errors.Is(err, nats.ErrKeyNotFound),
		"an infra fault must not be mistaken for a missing group")
}

// TestLookupSession_UserGroupPoliciesResolve exercises the ASIA GetSessionToken
// user path end-to-end through the public LookupCredentials entry point — the
// second caller of resolveUserPolicies. A user whose ONLY S3 grant is inherited
// from a group must be authorized on the session path, not just the AKIA path,
// guarding against a future fork that resolves session users differently.
func TestLookupSession_UserGroupPoliciesResolve(t *testing.T) {
	k := loadTestKey(t)
	const secret = "session-secret-value"
	cred := sessionCredential{
		AccessKeyID:     testSessionAKID,
		SecretEncrypted: encryptSessionSecret(t, k.AEAD, secret),
		AccountID:       testSessionAccount,
		PrincipalType:   "user",
		SessionName:     testSessionUser,
		ExpiresAt:       time.Now().UTC().Add(time.Hour),
	}
	sessions := map[string][]byte{testSessionAKID: mustMarshal(t, cred)}
	users := map[string][]byte{
		testSessionAccount + "." + testSessionUser: mustMarshal(t, iamUser{
			UserName:  testSessionUser,
			AccountID: testSessionAccount,
			Groups:    []string{"Engineers"}, // no direct policies: the only grant is via the group
		}),
	}
	groups := map[string][]byte{
		testSessionAccount + ".Engineers": mustMarshal(t, iamGroup{
			GroupName:        "Engineers",
			AccountID:        testSessionAccount,
			AttachedPolicies: []string{groupManagedARN},
		}),
	}
	policies := map[string][]byte{
		testSessionAccount + ".GroupAllowS3": mustMarshal(t, iamPolicy{
			PolicyName: "GroupAllowS3", PolicyDocument: allowAllS3Policy,
		}),
	}
	p := newSessionProvider(k, sessions, users, nil, policies)
	p.groupsBucket = &fakeKV{data: groups}
	p.groupsReady = true

	res, err := p.LookupCredentials(testSessionAKID)
	require.NoError(t, err)
	require.Len(t, res.PolicyDocuments, 1, "the group-inherited policy must resolve on the user-session path")
	assert.True(t, evaluateS3Access("s3:ListBucket", "arn:aws:s3:::any", res.PolicyDocuments),
		"a user-session whose only grant is via a group must be authorized")
}

// TestIamGroup_UnmarshalsSpinifexJSON pins predastore's iamGroup JSON tags against a
// representative spinifex Group record. Extra fields (group_id, arn, path, tags) must
// be ignored; a tag drift from spinifex's struct would silently break group resolution
// in production while the marshal-roundtrip fixtures in this file stayed green.
func TestIamGroup_UnmarshalsSpinifexJSON(t *testing.T) {
	raw := `{
		"group_name": "Engineers",
		"group_id": "AGPAEXAMPLE",
		"account_id": "000000000001",
		"arn": "arn:aws:iam::000000000001:group/Engineers",
		"path": "/",
		"created_at": "2026-01-01T00:00:00Z",
		"attached_policies": ["arn:aws:iam::000000000001:policy/GroupAllowS3"],
		"inline_policies": {"AllowS3": "{\"Version\":\"2012-10-17\"}"},
		"tags": []
	}`
	var group iamGroup
	require.NoError(t, json.Unmarshal([]byte(raw), &group))
	assert.Equal(t, "Engineers", group.GroupName)
	assert.Equal(t, "000000000001", group.AccountID)
	assert.Equal(t, []string{"arn:aws:iam::000000000001:policy/GroupAllowS3"}, group.AttachedPolicies)
	assert.Equal(t, map[string]string{"AllowS3": `{"Version":"2012-10-17"}`}, group.InlinePolicies)
}

// TestIamUser_GroupsUnmarshalsSpinifexJSON pins the new iamUser.Groups tag against a
// spinifex User record so a `groups` tag drift cannot silently sever group membership.
func TestIamUser_GroupsUnmarshalsSpinifexJSON(t *testing.T) {
	raw := `{
		"user_name": "alice",
		"user_id": "AIDAEXAMPLE",
		"account_id": "000000000001",
		"arn": "arn:aws:iam::000000000001:user/alice",
		"path": "/",
		"access_keys": ["AKIAEXAMPLE"],
		"tags": [],
		"attached_policies": ["arn:aws:iam::000000000001:policy/DirectAllowS3"],
		"groups": ["Engineers", "Auditors"]
	}`
	var user iamUser
	require.NoError(t, json.Unmarshal([]byte(raw), &user))
	assert.Equal(t, []string{"Engineers", "Auditors"}, user.Groups)
	assert.Equal(t, []string{"arn:aws:iam::000000000001:policy/DirectAllowS3"}, user.AttachedPolicies)
}
