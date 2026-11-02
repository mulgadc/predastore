package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const gateAccount = "000000000007"

func accountRecord(status string) []byte {
	return []byte(`{"account_id":"` + gateAccount + `","status":"` + status + `"}`)
}

// newGateProvider takes the production lazy-open path: the accounts bucket is attached on
// the first check and watched, as it is when predastore starts before IAM has data.
func newGateProvider(t *testing.T, accounts *fakeKV) *NATSIAMProvider {
	t.Helper()
	buckets := map[string]*fakeKV{}
	if accounts != nil {
		buckets[kvBucketAccounts] = accounts
	}
	p := &NATSIAMProvider{
		js:            &fakeJetStream{buckets: buckets},
		cache:         make(map[string]*cachedCredential),
		accountActive: make(map[string]time.Time),
		done:          make(chan struct{}),
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

func TestCheckAccountActive_CachesOnlyActive(t *testing.T) {
	reads := 0
	kv := &fakeKV{data: map[string][]byte{gateAccount: accountRecord("ACTIVE")}, onGet: func() { reads++ }}
	p := newGateProvider(t, kv)
	require.NoError(t, p.CheckAccountActive(context.Background(), gateAccount))
	require.NoError(t, p.CheckAccountActive(context.Background(), gateAccount))
	assert.Equal(t, 1, reads, "a warm ACTIVE answer is not re-read")

	reads = 0
	kv.data[gateAccount] = accountRecord("SUSPENDED")
	p.mu.Lock()
	clear(p.accountActive)
	p.mu.Unlock()
	assert.ErrorIs(t, p.CheckAccountActive(context.Background(), gateAccount), ErrAccountNotActive)
	assert.ErrorIs(t, p.CheckAccountActive(context.Background(), gateAccount), ErrAccountNotActive)
	assert.Equal(t, 2, reads, "a refusal is never cached")
}

func TestCheckAccountActive_TTLBoundsAStaleActive(t *testing.T) {
	kv := &fakeKV{data: map[string][]byte{gateAccount: accountRecord("ACTIVE")}}
	p := newGateProvider(t, kv)
	require.NoError(t, p.CheckAccountActive(context.Background(), gateAccount))

	// The watcher missed this change; only the TTL can end the stale ACTIVE.
	kv.data[gateAccount] = accountRecord("TERMINATING")
	require.NoError(t, p.CheckAccountActive(context.Background(), gateAccount), "still inside the TTL")

	p.mu.Lock()
	p.accountActive[gateAccount] = time.Now().Add(-time.Second)
	p.mu.Unlock()
	assert.ErrorIs(t, p.CheckAccountActive(context.Background(), gateAccount), ErrAccountNotActive)
}

func TestCheckAccountActive_WatcherEvictsOnChange(t *testing.T) {
	kv := &fakeKV{data: map[string][]byte{gateAccount: accountRecord("ACTIVE")}}
	p := newGateProvider(t, kv)
	require.NoError(t, p.CheckAccountActive(context.Background(), gateAccount))

	kv.put(gateAccount, accountRecord("TERMINATING"))

	require.Eventually(t, func() bool {
		return errors.Is(p.CheckAccountActive(context.Background(), gateAccount), ErrAccountNotActive)
	}, 2*time.Second, 5*time.Millisecond, "the change must evict before the TTL")
}

func TestCheckAccountActive_ClosedWatcherFlushesTheCache(t *testing.T) {
	kv := &fakeKV{data: map[string][]byte{gateAccount: accountRecord("ACTIVE")}}
	p := newGateProvider(t, kv)
	require.NoError(t, p.CheckAccountActive(context.Background(), gateAccount))

	close(kv.watcher.updates)

	require.Eventually(t, func() bool {
		p.mu.RLock()
		defer p.mu.RUnlock()
		return len(p.accountActive) == 0
	}, 2*time.Second, 5*time.Millisecond)
}

// A change that lands while a read is in flight has already invalidated that read's answer.
func TestCheckAccountActive_ChangeDuringReadIsNotCached(t *testing.T) {
	var p *NATSIAMProvider
	kv := &fakeKV{data: map[string][]byte{gateAccount: accountRecord("ACTIVE")}}
	reads := 0
	kv.onGet = func() {
		reads++
		if reads == 1 {
			p.mu.Lock()
			p.invalidateAccountLocked(gateAccount)
			p.mu.Unlock()
		}
	}
	p = newGateProvider(t, kv)
	require.NoError(t, p.CheckAccountActive(context.Background(), gateAccount))
	require.NoError(t, p.CheckAccountActive(context.Background(), gateAccount))
	assert.Equal(t, 2, reads, "the raced answer was not cached, so the next check read again")
}

func TestChainProviderDelegatesAccountChecksToIAM(t *testing.T) {
	iam := newGateProvider(t, &fakeKV{data: map[string][]byte{gateAccount: accountRecord("TERMINATING")}})
	chain := NewChainProvider(iam, NewConfigProvider(nil))
	assert.ErrorIs(t, chain.CheckAccountActive(context.Background(), gateAccount), ErrAccountNotActive)
}

var _ jetstream.KeyValue = (*fakeKV)(nil)
