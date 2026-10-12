package auth

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Test-only seams for the external CURRENT-behaviour characterisation tests.
// They drive the real NATSIAMProvider (lazy bucket open, real watchers, real
// cache) over the fake KV buckets the package's own tests already use.

const (
	CharAKID      = "AKIACHARACTERISE0001"
	CharSecret    = "char-secret-value"
	CharUser      = "carol"
	CharUserID    = "AIDACAROLORIGINAL01"
	CharPolicy    = "CharS3"
	CharPolicyARN = "arn:aws:iam::" + testSessionAccount + ":policy/" + CharPolicy
	CacheTTL      = cacheTTL
)

// CharFixture is an AKIA principal in TestSessionAccount whose S3 grant comes
// from one directly attached managed policy.
type CharFixture struct {
	t        *testing.T
	Provider *NATSIAMProvider
	keys     *fakeKV
	users    *fakeKV
	policies *fakeKV
	sessions *fakeKV
}

// NewCharFixture builds the provider with every IAM bucket present but not yet
// opened, so the first lookup takes the production lazy-open path.
func NewCharFixture(t *testing.T, policyDoc string) *CharFixture {
	t.Helper()
	k := loadTestKey(t)
	f := &CharFixture{
		t:        t,
		keys:     &fakeKV{data: map[string][]byte{}},
		users:    &fakeKV{data: map[string][]byte{}},
		policies: &fakeKV{data: map[string][]byte{}},
		sessions: &fakeKV{data: map[string][]byte{}},
	}
	f.policies.data[testSessionAccount+"."+CharPolicy] = mustMarshal(t, iamPolicy{
		PolicyName: CharPolicy, ARN: CharPolicyARN, PolicyDocument: policyDoc,
	})
	f.users.data[testSessionAccount+"."+CharUser] = mustMarshal(t, iamUser{
		UserName: CharUser, UserID: CharUserID, AccountID: testSessionAccount,
		AttachedPolicies: []string{CharPolicyARN},
	})
	f.keys.data[CharAKID] = mustMarshal(t, iamAccessKey{
		AccessKeyID: CharAKID, SecretAccessKey: encryptSessionSecret(t, k.AEAD, CharSecret),
		UserName: CharUser, AccountID: testSessionAccount, Status: "Active",
	})
	js := &fakeJetStream{buckets: map[string]*fakeKV{
		"spinifex-iam-access-keys": f.keys,
		kvBucketUsers:              f.users,
		kvBucketRoles:              {data: map[string][]byte{}},
		kvBucketPolicies:           f.policies,
		kvBucketSessionCredentials: f.sessions,
	}}
	f.Provider = &NATSIAMProvider{
		js: js, key: k, bucketName: "spinifex-iam-access-keys",
		cache: make(map[string]*cachedCredential), done: make(chan struct{}),
	}
	t.Cleanup(f.Provider.Close)
	return f
}

// publish notifies the bucket's watcher, as a KV write or delete marker does.
// A bucket not yet opened has no watcher, so there is nothing to notify.
func publish(kv *fakeKV, key string) {
	if kv.watcher != nil {
		kv.watcher.updates <- fakeKVEntry{key: key}
	}
}

// SetKeyStatus rewrites the access key record with status and notifies.
func (f *CharFixture) SetKeyStatus(status string) {
	f.t.Helper()
	rec := iamAccessKey{}
	mustUnmarshalInto(f.t, f.keys.data[CharAKID], &rec)
	rec.Status = status
	f.keys.data[CharAKID] = mustMarshal(f.t, rec)
	publish(f.keys, CharAKID)
}

// SetKeyAccount rewrites the access key record's account_id and notifies.
func (f *CharFixture) SetKeyAccount(accountID string) {
	f.t.Helper()
	rec := iamAccessKey{}
	mustUnmarshalInto(f.t, f.keys.data[CharAKID], &rec)
	rec.AccountID = accountID
	f.keys.data[CharAKID] = mustMarshal(f.t, rec)
	publish(f.keys, CharAKID)
}

// ResealKeyUnderForeignMasterKey re-encrypts the stored secret under a master
// key the provider does not hold, as a master-key mismatch would leave it.
func (f *CharFixture) ResealKeyUnderForeignMasterKey() {
	f.t.Helper()
	other := loadTestKey(f.t)
	rec := iamAccessKey{}
	mustUnmarshalInto(f.t, f.keys.data[CharAKID], &rec)
	rec.SecretAccessKey = encryptSessionSecret(f.t, other.AEAD, CharSecret)
	f.keys.data[CharAKID] = mustMarshal(f.t, rec)
	publish(f.keys, CharAKID)
}

// DeleteKey removes the access key; notify=false models a watcher that never
// delivered the delete, leaving only the cache TTL to revoke it.
func (f *CharFixture) DeleteKey(notify bool) {
	delete(f.keys.data, CharAKID)
	if notify {
		publish(f.keys, CharAKID)
	}
}

// DeleteUser removes the IAM user record and notifies.
func (f *CharFixture) DeleteUser() {
	key := testSessionAccount + "." + CharUser
	delete(f.users.data, key)
	publish(f.users, key)
}

// RecreateUser writes a new user record under the same name with a new unique
// ID and the given attached policy ARNs, and notifies.
func (f *CharFixture) RecreateUser(userID string, attached ...string) {
	f.t.Helper()
	key := testSessionAccount + "." + CharUser
	f.users.data[key] = mustMarshal(f.t, iamUser{
		UserName: CharUser, UserID: userID, AccountID: testSessionAccount,
		AttachedPolicies: attached,
	})
	publish(f.users, key)
}

// DeletePolicy removes the managed policy record without detaching it.
func (f *CharFixture) DeletePolicy() {
	key := testSessionAccount + "." + CharPolicy
	delete(f.policies.data, key)
	publish(f.policies, key)
}

// FailBucket makes every Get on the named bucket return err ("keys", "users",
// "policies" or "sessions"); nil restores it.
func (f *CharFixture) FailBucket(name string, err error) {
	map[string]*fakeKV{"keys": f.keys, "users": f.users, "policies": f.policies, "sessions": f.sessions}[name].getErr = err
}

// CachedEntries is the number of credentials currently cached.
func (f *CharFixture) CachedEntries() int {
	f.Provider.mu.RLock()
	defer f.Provider.mu.RUnlock()
	return len(f.Provider.cache)
}

// ExpireCache ages every cached credential past its TTL, standing in for the
// 60s elapsing.
func (f *CharFixture) ExpireCache() {
	f.Provider.mu.Lock()
	defer f.Provider.mu.Unlock()
	for _, c := range f.Provider.cache {
		c.expiresAt = time.Now().Add(-time.Second)
	}
}

// NewUnreachableIAMProvider is a provider whose JetStream cannot open any
// bucket because the NATS connection is closed.
func NewUnreachableIAMProvider(t *testing.T) *NATSIAMProvider {
	t.Helper()
	return &NATSIAMProvider{
		js: &fakeJS{kvErr: nats.ErrConnectionClosed}, key: loadTestKey(t),
		bucketName: "spinifex-iam-access-keys",
		cache:      make(map[string]*cachedCredential), done: make(chan struct{}),
	}
}

// NewBootstrappingIAMProvider is a provider whose IAM buckets do not exist yet.
func NewBootstrappingIAMProvider(t *testing.T) *NATSIAMProvider {
	t.Helper()
	return &NATSIAMProvider{
		js: &fakeJetStream{buckets: map[string]*fakeKV{}}, key: loadTestKey(t),
		bucketName: "spinifex-iam-access-keys",
		cache:      make(map[string]*cachedCredential), done: make(chan struct{}),
	}
}

// ErrKVUnavailable stands in for a JetStream fault that is not a not-found.
var ErrKVUnavailable = jetstream.ErrJetStreamNotEnabled

func mustUnmarshalInto(t *testing.T, b []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("unmarshal fixture record: %v", err)
	}
}
