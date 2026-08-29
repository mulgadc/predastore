package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The point of taking blocks lazily: a healthy read opens k shards, so the m
// parity buffers are 1/(k+m) of the working set that never gets written to.
func TestABlockIsOnlyTakenWhenItIsAskedFor(t *testing.T) {
	t.Parallel()

	s := newBlockSet(3, streamBlockSize)
	assert.Equal(t, 0, taken(s), "a new set holds nothing")

	s.at(0)
	s.at(1)
	assert.Equal(t, 2, taken(s), "only the blocks asked for are held")

	assert.Nil(t, s.spare(2), "spare must not take a block that was never used")
	assert.Equal(t, 2, taken(s))
}

// spare is what the encoder is handed for a shard that did not arrive, and it
// has to keep the capacity so reconstruction writes back into the working set
// rather than allocating a block per stripe.
func TestSpareKeepsTheCapacityOfABlockAlreadyTaken(t *testing.T) {
	t.Parallel()

	s := newBlockSet(3, streamBlockSize)
	s.at(0)

	spare := s.spare(0)
	require.NotNil(t, spare)
	assert.Empty(t, spare, "the encoder reads length zero as absent")
	assert.GreaterOrEqual(t, cap(spare), streamBlockSize, "reconstruction needs somewhere to land")
}

func TestReleaseEmptiesTheSet(t *testing.T) {
	t.Parallel()

	s := newBlockSet(3, streamBlockSize)
	s.at(0)
	s.at(2)
	s.release()

	assert.Equal(t, 0, taken(s), "a released set holds nothing, so releasing twice is not a double put")
	s.release()
}

// An object recorded before the streaming layout has one block per shard of
// whatever size the shard is. Those must not go into a pool that every 4 MiB
// request draws from, or one 2 GiB read leaves a 2 GiB buffer in it.
func TestAnOversizedBlockIsNeitherPooledNorTruncated(t *testing.T) {
	t.Parallel()

	const big = streamBlockSize * 3
	s := newBlockSet(3, big)

	block := s.at(0)
	assert.Len(t, block, big, "the block must be the size the layout recorded")

	s.release()
	fresh := newBlockSet(1, streamBlockSize)
	assert.Len(t, fresh.at(0), streamBlockSize, "an oversized block must not be handed to a standard request")
}

// Reuse is the entire reason this type exists: at 4 MiB a block, a GET that
// allocates its own working set is the largest allocation in the system. It is
// asserted by counting allocations rather than by comparing pointers, because
// sync.Pool promises reuse in aggregate and not for any one Get.
//
// Not parallel: AllocsPerRun measures the whole process, so another test
// allocating alongside it would be counted here.
func TestASteadyStreamOfRequestsStopsAllocating(t *testing.T) {
	// Warm the pool first, so the measurement is of the steady state rather
	// than of the first request through.
	for range 4 {
		s := newBlockSet(3, streamBlockSize)
		s.at(0)
		s.at(1)
		s.release()
	}

	allocs := testing.AllocsPerRun(200, func() {
		s := newBlockSet(3, streamBlockSize)
		s.at(0)
		s.at(1)
		s.release()
	})

	// The set itself is two small slices; the 8 MiB of blocks it hands out are
	// what must not be allocated again.
	assert.LessOrEqual(t, allocs, float64(4), "a warm block set is allocating its buffers rather than reusing them")
}

func taken(s *blockSet) int {
	n := 0
	for _, b := range s.held {
		if b != nil {
			n++
		}
	}

	return n
}
