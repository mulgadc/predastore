// The read path had no test below 64 KiB and the write path none between 8 KiB
// and 1 MiB, so an assumption about what a rate means for a small object went
// in as a constant with nothing positioned to disagree with it. These are the
// tests that disagree: a ladder from one byte to 128 KiB through the real write
// and read paths, and the throughput floor stated exactly rather than observed.

package handlers

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// objectSizeLadder is chosen rather than stepped. 1-16 sits below and around
// the shard count, where a data shard can be pure padding; 96, 297 and 489 are
// the sizes the e2e cells were writing when they produced 195 spurious slow
// shard warnings between them; the rest are boundaries and their neighbours,
// doubling to 128 KiB.
//
// Nothing here reaches streamBlockSize. Multi-block reads are covered in
// streamread_test.go, and repeating them would make a millisecond test slow.
var objectSizeLadder = []int{
	1, 2, 3, 4, 5, 7, 8, 15, 16,
	96, 297, 489,
	511, 512, 1024, 2048,
	4095, 4096, 4097, 8192,
	16384, 32768, 65536, 131072,
}

// objectSizeKeys is how many keys each size is written under. Placement is
// derived from the object hash, so the key decides which nodes hold the shards:
// one key exercises one placement and sixteen exercise the ring.
const objectSizeKeys = 16

// slowShardWarning is the message the floor emits. Named once so the tests that
// assert on its absence and the one that asserts on its presence cannot drift.
const slowShardWarning = "Shard delivered below the throughput floor"

// The floor is a rate, and a rate needs a window long enough to be a
// measurement. For a shard that arrives in one read the window is the round
// trip, so this states which reads may be judged at all.
func TestShardBelowFloorIgnoresAShardTooSmallToTime(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		bytes  int64
		active time.Duration
		want   bool
	}{
		// The field case. One shard of a 297-byte object at RS(2,1) is 149
		// bytes, and clearing 8 MiB/s on it needs an 18us round trip.
		{"a shard of a 297 byte object over a LAN round trip", 149, 100 * time.Microsecond, false},
		{"the same shard over a round trip ten times worse", 149, time.Millisecond, false},
		{"a four byte object", 2, 50 * time.Microsecond, false},
		{"a 128 KiB object's shard", 64 << 10, 200 * time.Microsecond, false},

		// The boundary, stated from both sides.
		{"one byte under the minimum, delivered slowly", slowShardMinBytes - 1, time.Second, false},
		{"exactly the minimum, delivered slowly", slowShardMinBytes, time.Second, true},

		// Above the minimum the rate is the whole of the decision, which is
		// what the floor was written to say and still says.
		{"a large shard delivered fast", 64 << 20, time.Second, false},
		{"a large shard delivered slowly", 4 << 20, 2 * time.Second, true},
		{"a large shard exactly at the floor", slowShardFloor, time.Second, false},
		{"a large shard a hair under the floor", slowShardFloor - 1, time.Second, true},

		// A shard whose window did not register cannot be divided by.
		{"no measurable window", 4 << 20, 0, false},
		{"a negative window", 4 << 20, -time.Second, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equalf(t, tc.want, shardBelowFloor(tc.bytes, tc.active),
				"%d bytes in %s", tc.bytes, tc.active)
		})
	}
}

// The ladder itself, through the real write and read paths. Sixteen keys per
// size because placement follows the key: this is a sweep over placements as
// much as over sizes.
func TestObjectSizeLadderRoundTrips(t *testing.T) {
	t.Parallel()

	codes := []struct {
		name         string
		data, parity int
	}{
		// The unencoded fast path, which has to produce what the general path
		// would at every size and was pinned at five of them.
		{"RS(1,0)", 1, 0},
		{"RS(2,1)", 2, 1},
		{"RS(4,2)", 4, 2},
	}

	for _, code := range codes {
		t.Run(code.name, func(t *testing.T) {
			t.Parallel()
			for _, size := range objectSizeLadder {
				t.Run(fmt.Sprintf("%d-bytes", size), func(t *testing.T) {
					t.Parallel()
					ctx := context.Background()
					f := newWriteFixture(code.data, code.parity)

					for i := range objectSizeKeys {
						key := fmt.Sprintf("prefix-%02d/object-%d.bin", i, size)
						want := randomBytes(t, size)
						objectHash := model.ObjectHash("sizes", key)

						place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(size))
						require.NoErrorf(t, err, "writing %d bytes to %s", size, key)
						require.Equalf(t, int64(size), place.Size,
							"the placement of %s records the size written", key)

						got, degraded, err := readObject(ctx, f.bc, f.cfg, "sizes", key, place, place.Size, 0)
						require.NoErrorf(t, err, "reading %d bytes back from %s", size, key)
						assert.Zerof(t, degraded, "%s read cleanly, so nothing was reconstructed", key)
						assert.Equalf(t, want, got, "%d bytes round-tripped through %s", size, key)
					}
				})
			}
		})
	}
}

// The shard arithmetic every size in the ladder rests on, stated separately
// because a round trip that is wrong in both directions still passes.
func TestObjectSizeLadderShardsAreExactlyLargeEnough(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	ctx := context.Background()

	for _, size := range objectSizeLadder {
		t.Run(fmt.Sprintf("%d-bytes", size), func(t *testing.T) {
			t.Parallel()
			key := fmt.Sprintf("layout/object-%d.bin", size)
			objectHash := model.ObjectHash("sizes", key)
			place, _, err := f.write(ctx, objectHash, bytes.NewReader(randomBytes(t, size)), int64(size))
			require.NoError(t, err)

			lay := newLayout(f.cfg.DataShards, place.Size, place.BlockSize)

			// Every size here is smaller than a block, so each shard is one
			// block and the shards together cover the object with less than a
			// shard of padding.
			assert.Equal(t, lay.shardSize, lay.blockSize, "a sub-block object is one block per shard")
			assert.GreaterOrEqual(t, lay.shardSize*int64(f.cfg.DataShards), int64(size),
				"the data shards hold the whole object")
			assert.Less(t, (lay.shardSize-1)*int64(f.cfg.DataShards), int64(size),
				"a shard smaller by one would not, so none of them is padding alone")
		})
	}
}

// The regression. Every object in the ladder is delivered at a plausible LAN
// round trip and none of them may be reported as a slow shard: at these sizes
// the window the rate is computed over is the round trip itself.
//
// Not parallel -- it captures the default logger, which is process-wide.
func TestASmallObjectDoesNotWarnAboutItsShards(t *testing.T) {
	logged := captureWarnings(t)
	ctx := context.Background()

	// Ten times a loopback round trip and still ten times too slow to clear the
	// floor on any size here, which is the point: no plausible latency lets a
	// small object pass a throughput bar.
	const latency = 500 * time.Microsecond

	for _, size := range objectSizeLadder {
		f := newWriteFixture(2, 1)
		paced := newLatencyBlob(f.bc, latency)

		for i := range objectSizeKeys {
			key := fmt.Sprintf("quiet-%02d/object-%d.bin", i, size)
			want := randomBytes(t, size)
			objectHash := model.ObjectHash("sizes", key)

			place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(size))
			require.NoError(t, err)

			got, _, err := readObject(ctx, paced, f.cfg, "sizes", key, place, place.Size, 0,
				withClock(paced.clk))
			require.NoError(t, err)
			require.Equal(t, want, got)
		}

		assert.Zerof(t, strings.Count(logged.String(), slowShardWarning),
			"a %d byte object served in %s named a node for being slow:\n%s",
			size, latency, logged.String())
		logged.Reset()
	}
}

// The other half of the same statement: the floor still fires on a shard large
// enough for the rate to mean something. Without this the test above passes on
// a floor that was deleted rather than guarded.
func TestALargeSlowShardIsStillNamed(t *testing.T) {
	logged := captureWarnings(t)
	ctx := context.Background()

	// A shard well past the minimum, delivered over a window long enough that
	// the rate lands under the floor.
	const size = 2 << 20
	f := newWriteFixture(2, 1)
	paced := newLatencyBlob(f.bc, time.Second)

	key := "slow/object.bin"
	objectHash := model.ObjectHash("sizes", key)
	want := randomBytes(t, size)
	place, _, err := f.write(ctx, objectHash, bytes.NewReader(want), int64(size))
	require.NoError(t, err)

	got, _, err := readObject(ctx, paced, f.cfg, "sizes", key, place, place.Size, 0, withClock(paced.clk))
	require.NoError(t, err)
	require.Equal(t, want, got)

	assert.Containsf(t, logged.String(), slowShardWarning,
		"a %d byte shard delivered in a second is slow and must say so", size/2)
}

// captureWarnings redirects the default logger into a buffer for the length of
// the test. Both callers assert on what the gate did or did not log, which is
// the only place the throughput floor is observable from.
func captureWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	var out bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	return &out
}
