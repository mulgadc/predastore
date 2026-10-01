package auth

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- fake KV watcher and JetStream, so ensureBuckets wires real watchers ---

type fakeWatcher struct {
	jetstream.KeyWatcher

	updates chan jetstream.KeyValueEntry
}

func (w *fakeWatcher) Updates() <-chan jetstream.KeyValueEntry { return w.updates }
func (w *fakeWatcher) Stop() error                             { return nil }

func (k *fakeKV) WatchAll(context.Context, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	k.watcher = &fakeWatcher{updates: make(chan jetstream.KeyValueEntry, 1)}
	return k.watcher, nil
}

// put writes a record and publishes it to the bucket's watcher, as a KV Put does.
func (k *fakeKV) put(key string, val []byte) {
	k.data[key] = val
	k.watcher.updates <- fakeKVEntry{key: key, val: val}
}

type fakeJetStream struct {
	jetstream.JetStream

	buckets map[string]*fakeKV
}

func (j *fakeJetStream) KeyValue(_ context.Context, name string) (jetstream.KeyValue, error) {
	if b, ok := j.buckets[name]; ok {
		return b, nil
	}
	return nil, jetstream.ErrBucketNotFound
}

const (
	cacheTestAKID    = "AKIACACHETEST0000001"
	cacheTestAccount = "000000000002"
	cacheTestUser    = "bob"
	cacheTestGroup   = "Readers"
	cacheTestPolicy  = "S3Access"
)

var cacheTestPolicyARN = "arn:aws:iam::" + cacheTestAccount + ":policy/" + cacheTestPolicy

type cacheFixture struct {
	t        *testing.T
	p        *NATSIAMProvider
	keys     *fakeKV
	users    *fakeKV
	groups   *fakeKV
	policies *fakeKV
}

// newCacheFixture builds a provider for an AKIA user whose S3 grant comes from
// one managed policy, attached either directly or through a group.
func newCacheFixture(t *testing.T, viaGroup bool) *cacheFixture {
	t.Helper()
	k := loadTestKey(t)
	user := iamUser{UserName: cacheTestUser, AccountID: cacheTestAccount}
	group := iamGroup{GroupName: cacheTestGroup, AccountID: cacheTestAccount}
	if viaGroup {
		user.Groups = []string{cacheTestGroup}
		group.AttachedPolicies = []string{cacheTestPolicyARN}
	} else {
		user.AttachedPolicies = []string{cacheTestPolicyARN}
	}

	f := &cacheFixture{
		t: t,
		users: &fakeKV{data: map[string][]byte{
			cacheTestAccount + "." + cacheTestUser: mustMarshal(t, user),
		}},
		groups: &fakeKV{data: map[string][]byte{
			cacheTestAccount + "." + cacheTestGroup: mustMarshal(t, group),
		}},
		policies: &fakeKV{data: map[string][]byte{}},
	}
	f.policies.data[cacheTestAccount+"."+cacheTestPolicy] = f.policyRecord(allowAllS3Policy)

	f.keys = &fakeKV{data: map[string][]byte{
		cacheTestAKID: mustMarshal(t, iamAccessKey{
			AccessKeyID:     cacheTestAKID,
			SecretAccessKey: encryptSessionSecret(t, k.AEAD, "secret"),
			UserName:        cacheTestUser,
			AccountID:       cacheTestAccount,
			Status:          "Active",
		}),
	}}

	js := &fakeJetStream{buckets: map[string]*fakeKV{
		"spinifex-iam-access-keys": f.keys,
		kvBucketUsers:              f.users,
		kvBucketRoles:              {data: map[string][]byte{}},
		kvBucketPolicies:           f.policies,
		kvBucketGroups:             f.groups,
	}}
	f.p = &NATSIAMProvider{
		js:         js,
		key:        k,
		bucketName: "spinifex-iam-access-keys",
		cache:      make(map[string]*cachedCredential),
		done:       make(chan struct{}),
	}
	t.Cleanup(f.p.Close)
	return f
}

func (f *cacheFixture) policyRecord(doc string) []byte {
	return mustMarshal(f.t, iamPolicy{PolicyName: cacheTestPolicy, ARN: cacheTestPolicyARN, PolicyDocument: doc})
}

func (f *cacheFixture) canGetObject() bool {
	f.t.Helper()
	res, err := f.p.LookupCredentials(cacheTestAKID)
	require.NoError(f.t, err)
	return allowed("s3:GetObject", "arn:aws:s3:::b/k", res.PolicyDocuments)
}

// requireRevoked asserts the change reaches the next lookup well inside the
// 60s cache TTL, i.e. it was delivered by a watcher rather than by expiry.
func (f *cacheFixture) requireRevoked() {
	f.t.Helper()
	require.Eventually(f.t, func() bool { return !f.canGetObject() }, 2*time.Second, 10*time.Millisecond)
}

func TestNATSIAMProvider_PolicyDetachEvictsCachedCredential(t *testing.T) {
	f := newCacheFixture(t, false)
	require.True(t, f.canGetObject())
	require.Len(t, f.p.cache, 1, "the AKIA lookup is cached")

	f.users.put(cacheTestAccount+"."+cacheTestUser, mustMarshal(t, iamUser{
		UserName: cacheTestUser, AccountID: cacheTestAccount,
	}))
	f.requireRevoked()
}

func TestNATSIAMProvider_PolicyDocumentChangeEvictsCachedCredential(t *testing.T) {
	f := newCacheFixture(t, false)
	require.True(t, f.canGetObject())

	f.policies.put(cacheTestAccount+"."+cacheTestPolicy, f.policyRecord(denyAllS3Policy))
	f.requireRevoked()
}

func TestNATSIAMProvider_GroupPolicyDetachEvictsCachedCredential(t *testing.T) {
	f := newCacheFixture(t, true)
	// The first lookup opens the groups bucket, which flushes the cache mid-lookup,
	// so only the second lookup leaves an entry for the group watcher to evict.
	require.True(t, f.canGetObject())
	require.True(t, f.canGetObject())
	require.Len(t, f.p.cache, 1, "the group-path lookup is cached")

	f.groups.put(cacheTestAccount+"."+cacheTestGroup, mustMarshal(t, iamGroup{
		GroupName: cacheTestGroup, AccountID: cacheTestAccount,
	}))
	f.requireRevoked()
}

func TestNATSIAMProvider_AccessKeyDeactivationEvictsCachedCredential(t *testing.T) {
	f := newCacheFixture(t, false)
	require.True(t, f.canGetObject())
	require.Len(t, f.p.cache, 1, "the AKIA lookup is cached")

	var ak iamAccessKey
	require.NoError(t, json.Unmarshal(f.keys.data[cacheTestAKID], &ak))
	ak.Status = "Inactive"
	f.keys.put(cacheTestAKID, mustMarshal(t, ak))

	require.Eventually(t, func() bool {
		_, err := f.p.LookupCredentials(cacheTestAKID)
		return err != nil
	}, 2*time.Second, 10*time.Millisecond)
}

func TestNATSIAMProvider_InvalidationDuringLookupIsNotCachedOver(t *testing.T) {
	f := newCacheFixture(t, false)
	// The invalidation lands while the lookup reads the user record, as a KV
	// change racing an in-flight lookup would.
	f.users.onGet = func() {
		f.p.mu.Lock()
		f.p.flushCacheLocked("")
		f.p.mu.Unlock()
	}

	res, err := f.p.LookupCredentials(cacheTestAKID)
	require.NoError(t, err)
	assert.NotEmpty(t, res.PolicyDocuments)
	assert.Empty(t, f.p.cache, "a result built across an invalidation must not be cached")

	f.users.onGet = nil
	_, err = f.p.LookupCredentials(cacheTestAKID)
	require.NoError(t, err)
	assert.Len(t, f.p.cache, 1, "an undisturbed lookup is cached")
}
