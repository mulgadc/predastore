package testport

import (
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlockStaysBelowEphemeralFloor is the invariant the package exists for: an
// allocated port must never be one the kernel could autobind a client socket to.
func TestBlockStaysBelowEphemeralFloor(t *testing.T) {
	for range 50 {
		base := Block(t, 5)
		assert.GreaterOrEqual(t, base, bandStart)
		assert.Less(t, base+5, bandEnd)
	}
}

// TestBlocksAreDisjoint covers the collision mode that the per-suite counters it
// replaces got wrong: two setups in one binary must not share a port.
func TestBlocksAreDisjoint(t *testing.T) {
	seen := make(map[int]bool)
	for range 50 {
		base := Block(t, 5)
		for port := base; port < base+5; port++ {
			require.False(t, seen[port], "port %d handed out twice", port)
			seen[port] = true
		}
	}
}

// TestBlockSkipsOccupiedPorts checks the probe: a block whose ports are already
// taken must not be handed out, whichever protocol holds them.
func TestBlockSkipsOccupiedPorts(t *testing.T) {
	// Take the block that would be returned next, on UDP only, to also prove the
	// probe is not TCP-only.
	base := Block(t, 5)
	pc, err := net.ListenPacket("udp4", fmt.Sprintf("127.0.0.1:%d", base+2))
	require.NoError(t, err)
	defer func() { _ = pc.Close() }()

	// Rewind so the occupied block is the next candidate.
	nextBlock.Add(-1)

	assert.NotEqual(t, base, Block(t, 5), "occupied block should have been skipped")
}

// TestBlockRejectsBadSize guards the two size limits, since exceeding the stride
// would silently overlap the next block.
func TestBlockRejectsBadSize(t *testing.T) {
	for _, size := range []int{0, -1, blockStride + 1} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			// Block calls t.Fatalf, which only aborts the goroutine it runs on, so
			// drive it on its own and assert the test was marked failed.
			fake := &testing.T{}
			done := make(chan struct{})
			go func() {
				defer close(done)
				Block(fake, size)
			}()
			<-done
			assert.True(t, fake.Failed(), "size %d should be rejected", size)
		})
	}
}
