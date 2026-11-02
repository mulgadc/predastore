package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const gateAccount = "000000000007"

func accountRecord(status string) []byte {
	return []byte(`{"account_id":"` + gateAccount + `","status":"` + status + `"}`)
}

// newGateProvider takes the production lazy-open path: the accounts bucket is attached on
// the first check, as it is when predastore starts before IAM has data.
func newGateProvider(t *testing.T, accounts *fakeKV) *NATSIAMProvider {
	t.Helper()
	buckets := map[string]*fakeKV{}
	if accounts != nil {
		buckets[kvBucketAccounts] = accounts
	}
	p := &NATSIAMProvider{
		js:    &fakeJetStream{buckets: buckets},
		cache: make(map[string]*cachedCredential),
		done:  make(chan struct{}),
	}
	t.Cleanup(p.Close)
	return p
}

func TestCheckAccountActive_Status(t *testing.T) {
	cases := []struct {
		name string
		data map[string][]byte
		want error
	}{
		{"active", map[string][]byte{gateAccount: accountRecord("ACTIVE")}, nil},
		{"suspended", map[string][]byte{gateAccount: accountRecord("SUSPENDED")}, ErrAccountNotActive},
		{"terminating", map[string][]byte{gateAccount: accountRecord("TERMINATING")}, ErrAccountNotActive},
		{"missing record", map[string][]byte{}, ErrAccountNotActive},
		{"empty status", map[string][]byte{gateAccount: accountRecord("")}, ErrAccountNotActive},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := newGateProvider(t, &fakeKV{data: c.data})
			err := p.CheckAccountActive(context.Background(), gateAccount)
			if c.want == nil {
				require.NoError(t, err)
				return
			}
			assert.ErrorIs(t, err, c.want)
		})
	}
}

func TestCheckAccountActive_NoAccountIDIsRefused(t *testing.T) {
	p := newGateProvider(t, &fakeKV{data: map[string][]byte{gateAccount: accountRecord("ACTIVE")}})
	assert.ErrorIs(t, p.CheckAccountActive(context.Background(), ""), ErrAccountNotActive)
}

func TestCheckAccountActive_MissingBucketRefusesAndNeverCreatesIt(t *testing.T) {
	// fakeJetStream embeds a nil interface, so any attempt to create a bucket would panic.
	p := newGateProvider(t, nil)
	assert.ErrorIs(t, p.CheckAccountActive(context.Background(), gateAccount), ErrAccountNotActive)
}

func TestCheckAccountActive_ReadErrorIsNotAVerdict(t *testing.T) {
	p := newGateProvider(t, &fakeKV{data: map[string][]byte{}, getErr: errors.New("kv down")})
	err := p.CheckAccountActive(context.Background(), gateAccount)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrAccountNotActive)
}

func TestCheckAccountActive_UndecodableRecordIsNotAVerdict(t *testing.T) {
	p := newGateProvider(t, &fakeKV{data: map[string][]byte{gateAccount: []byte("{")}})
	err := p.CheckAccountActive(context.Background(), gateAccount)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrAccountNotActive)
}

// There is no cache and no watcher: a change written by another client is seen by the
// very next check.
func TestCheckAccountActive_StatusChangeIsSeenOnTheNextRequest(t *testing.T) {
	kv := &fakeKV{data: map[string][]byte{gateAccount: accountRecord("ACTIVE")}}
	p := newGateProvider(t, kv)
	require.NoError(t, p.CheckAccountActive(context.Background(), gateAccount))

	kv.data[gateAccount] = accountRecord("TERMINATING")
	assert.ErrorIs(t, p.CheckAccountActive(context.Background(), gateAccount), ErrAccountNotActive)
}

func TestCheckAccountActive_ReadsOnEveryCall(t *testing.T) {
	reads := 0
	kv := &fakeKV{data: map[string][]byte{gateAccount: accountRecord("ACTIVE")}, onGet: func() { reads++ }}
	p := newGateProvider(t, kv)
	for range 3 {
		require.NoError(t, p.CheckAccountActive(context.Background(), gateAccount))
	}
	assert.Equal(t, 3, reads)
}

func TestProtectedAccount(t *testing.T) {
	assert.True(t, ProtectedAccount("000000000000"))
	assert.True(t, ProtectedAccount("000000000001"))
	assert.False(t, ProtectedAccount(""))
	assert.False(t, ProtectedAccount(gateAccount))
}

func TestChainProviderDelegatesAccountChecksToIAM(t *testing.T) {
	iam := newGateProvider(t, &fakeKV{data: map[string][]byte{gateAccount: accountRecord("TERMINATING")}})
	chain := NewChainProvider(iam, NewConfigProvider(nil))
	assert.ErrorIs(t, chain.CheckAccountActive(context.Background(), gateAccount), ErrAccountNotActive)
}

var _ jetstream.KeyValue = (*fakeKV)(nil)
