package handlers

import (
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mustEpochs is the minter every write-path fixture needs. Node 1 unless the
// test is about two gates, in which case it wants two of these. It takes no
// testing.T because the write benchmarks build a fixture too.
func mustEpochs(node config.NodeID) *EpochMinter {
	m, err := NewEpochMinter(node)
	if err != nil {
		panic(err)
	}
	return m
}

// frozen pins the minter's clock so a test can hold a millisecond open for as
// long as it likes, which is the only way to reach the sequence field.
func frozen(m *EpochMinter, at time.Time) *time.Time {
	now := at
	m.now = func() time.Time { return now }
	return &now
}

func epochParts(e uint64) (ms int64, node, seq uint64) {
	return int64(e >> (epochNodeBits + epochSeqBits)),
		e >> epochSeqBits & epochNodeMax,
		e & epochSeqMax
}

func TestAMintedEpochUnpacksToWhatWentIntoIt(t *testing.T) {
	m := mustEpochs(7)
	at := time.UnixMilli(1_800_000_000_123)
	frozen(m, at)

	e, err := m.Next()
	require.NoError(t, err)

	ms, node, seq := epochParts(e)
	assert.Equal(t, at.UnixMilli(), ms)
	assert.Equal(t, uint64(7), node)
	assert.Equal(t, uint64(0), seq)
	assert.Equal(t, at.UTC(), EpochTime(e))
}

func TestNoMintIsEverZero(t *testing.T) {
	m := mustEpochs(1)
	for range 1000 {
		e, err := m.Next()
		require.NoError(t, err)
		require.NotZero(t, e, "epoch zero is reserved for an unset record")
	}
}

func TestAFrozenClockStillIssuesRisingEpochs(t *testing.T) {
	m := mustEpochs(1)
	frozen(m, time.UnixMilli(1_800_000_000_000))

	seen := make(map[uint64]bool, 4096)
	prev := uint64(0)
	for i := range 4096 {
		e, err := m.Next()
		require.NoError(t, err)
		require.Greater(t, e, prev, "the minter repeated or went backwards at mint %d", i)
		require.False(t, seen[e], "duplicate epoch at mint %d", i)
		seen[e], prev = true, e
	}
}

func TestASpentSequenceBorrowsTheNextMillisecond(t *testing.T) {
	m := mustEpochs(1)
	at := time.UnixMilli(1_800_000_000_000)
	frozen(m, at)

	// One mint per sequence value, so the next one has nowhere left to go.
	for i := 0; i <= epochSeqMax; i++ {
		_, err := m.Next()
		require.NoError(t, err)
	}

	e, err := m.Next()
	require.NoError(t, err)
	ms, _, seq := epochParts(e)
	assert.Equal(t, at.UnixMilli()+1, ms, "the minter should take the next millisecond")
	assert.Equal(t, uint64(0), seq)
}

func TestAClockThatStepsBackwardsCannotReissueAnEpoch(t *testing.T) {
	m := mustEpochs(1)
	at := time.UnixMilli(1_800_000_000_000)
	now := frozen(m, at)

	seen := make(map[uint64]bool)
	var prev uint64
	for i := range 200 {
		// A step back an hour, then a minute, then forward again: NTP is
		// trusted for the reported time, never for the safety property.
		switch i {
		case 50:
			*now = at.Add(-time.Hour)
		case 100:
			*now = at.Add(-time.Minute)
		case 150:
			*now = at.Add(time.Second)
		}

		e, err := m.Next()
		require.NoError(t, err)
		require.Greater(t, e, prev, "the minter went backwards with the clock at step %d", i)
		require.False(t, seen[e], "the minter reissued an epoch at step %d", i)
		seen[e], prev = true, e
	}
}

func TestTwoGatesOnOneClockNeverCollide(t *testing.T) {
	a, b := mustEpochs(1), mustEpochs(2)
	at := time.UnixMilli(1_800_000_000_000)
	frozen(a, at)
	frozen(b, at)

	// Identical clock, identical mint count: without the node field every
	// pair here would be equal.
	for i := 0; i <= epochSeqMax; i++ {
		ea, err := a.Next()
		require.NoError(t, err)
		eb, err := b.Next()
		require.NoError(t, err)
		require.NotEqual(t, ea, eb, "two gates minted the same epoch at mint %d", i)
	}
}

func TestConcurrentMintsAreAllDistinct(t *testing.T) {
	m := mustEpochs(1)
	frozen(m, time.UnixMilli(1_800_000_000_000))

	const workers, each = 8, 200
	out := make(chan uint64, workers*each)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range each {
				e, err := m.Next()
				assert.NoError(t, err)
				out <- e
			}
		})
	}
	wg.Wait()
	close(out)

	seen := make(map[uint64]bool, workers*each)
	for e := range out {
		require.False(t, seen[e], "concurrent mints produced a duplicate epoch")
		seen[e] = true
	}
	assert.Len(t, seen, workers*each)
}

func TestAGateNeedsANodeIdThatFits(t *testing.T) {
	for _, node := range []config.NodeID{0, epochNodeMax + 1, 5000} {
		_, err := NewEpochMinter(node)
		assert.Error(t, err, "node id %d should be refused", node)
	}
	_, err := NewEpochMinter(epochNodeMax)
	assert.NoError(t, err)
}

func TestAWriteWithoutAMinterFailsRatherThanGuessing(t *testing.T) {
	var m *EpochMinter
	_, err := m.Next()
	assert.ErrorIs(t, err, errNoEpochMinter)
}
