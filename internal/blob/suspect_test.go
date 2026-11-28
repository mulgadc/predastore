package blob_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/blob/engine"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The first fragment of a fresh segment starts after the segment header, and
// its ciphertext after the fragment header. These are the engine's sizes.
const (
	suspectSegHeaderSize  = 14
	suspectFragHeaderSize = 32
)

// startEngineNode runs a blob node over a real engine store in a directory
// the test can reach.
func startEngineNode(t *testing.T, opts ...engine.Option) (*blob.Client, string) {
	t.Helper()
	c, dir, _ := startEngineNodeStore(t, opts...)
	return c, dir
}

func startEngineNodeStore(t *testing.T, opts ...engine.Option) (*blob.Client, string, *engine.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := engine.Open(dir, append([]engine.Option{engine.WithAEAD(storetest.TestAEAD())}, opts...)...)
	require.NoError(t, err)
	return startStoreNode(t, st), dir, st
}

// flipFirstFragment flips one ciphertext byte of the first fragment in the
// only segment that holds data.
func flipFirstFragment(t *testing.T, dir string) {
	t.Helper()
	segs, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	require.NoError(t, err)
	var held []string
	for _, seg := range segs {
		info, err := os.Stat(seg)
		require.NoError(t, err)
		if info.Size() > suspectSegHeaderSize+suspectFragHeaderSize {
			held = append(held, seg)
		}
	}
	require.Len(t, held, 1)
	flipFragmentIn(t, held[0])
}

// flipFragmentIn flips one ciphertext byte of the first fragment in seg.
func flipFragmentIn(t *testing.T, seg string) {
	t.Helper()
	f, err := os.OpenFile(seg, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	b := make([]byte, 1)
	_, err = f.ReadAt(b, suspectSegHeaderSize+suspectFragHeaderSize)
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b, suspectSegHeaderSize+suspectFragHeaderSize)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
}

func statSuspect(t *testing.T, c *blob.Client) bool {
	t.Helper()
	held, err := c.Stat(context.Background(), epochServerNode, blob.StatRequest{Key: epochKey(), Index: epochTestShardIndex})
	require.NoError(t, err)
	return held.Suspect
}

// corruptShard writes the shard at epoch, corrupts it on disk and reads it, so
// the node has seen it fail authentication.
func corruptShard(t *testing.T, c *blob.Client, dir string, epoch uint64) {
	t.Helper()
	put(t, c, epoch, bytes.Repeat([]byte("s"), 4096))
	require.NoError(t, commit(t, c, epoch))
	require.False(t, statSuspect(t, c), "a shard nobody read is not suspect")
	flipFirstFragment(t, dir)
	_, err := get(t, c, epoch)
	require.Error(t, err)
}

// A stat after a read that failed authentication reports the shard suspect,
// and a whole read that authenticates after a replacement clears it.
func TestStatReportsAShardSuspectUntilAWholeReadAuthenticates(t *testing.T) {
	c, dir := startEngineNode(t)
	corruptShard(t, c, dir, 5)
	assert.True(t, statSuspect(t, c))
	assert.True(t, statSuspect(t, c), "a stat does not clear the mark")

	body := bytes.Repeat([]byte("r"), 4096)
	put(t, c, 5, body)
	require.NoError(t, commit(t, c, 5))
	assert.True(t, statSuspect(t, c), "only a read proves the replacement")
	got, err := get(t, c, 5)
	require.NoError(t, err)
	assert.Equal(t, body, got)
	// The node clears the mark once its side of the stream ends, which can
	// be just after the client has read the last byte.
	assert.Eventually(t, func() bool { return !statSuspect(t, c) }, 5*time.Second, 5*time.Millisecond)
}

// A delete, or a newer generation, ends the mark for the position.
func TestASuspectMarkEndsWithItsGeneration(t *testing.T) {
	t.Run("delete", func(t *testing.T) {
		c, dir := startEngineNode(t)
		corruptShard(t, c, dir, 5)
		_, err := c.Delete(context.Background(), epochServerNode, blob.DeleteRequest{Key: epochKey(), Index: epochTestShardIndex})
		require.NoError(t, err)
		put(t, c, 5, []byte("written again"))
		require.NoError(t, commit(t, c, 5))
		assert.False(t, statSuspect(t, c))
	})
	t.Run("newer generation", func(t *testing.T) {
		c, dir := startEngineNode(t)
		corruptShard(t, c, dir, 5)
		put(t, c, 6, []byte("a newer write"))
		require.NoError(t, commit(t, c, 6))
		assert.False(t, statSuspect(t, c))
	})
}

// Compaction copies a suspect shard's ciphertext to a new place unchanged,
// so the mark, which names the generation and not the place, stays.
func TestASuspectMarkSurvivesCompactionMovingTheShard(t *testing.T) {
	c, dir, st := startEngineNodeStore(t, engine.WithMaxSegSize(40<<10), engine.WithCompaction(5*time.Millisecond),
		engine.WithFreeSpaceWatermark(0, 0))
	ctx := context.Background()
	body := bytes.Repeat([]byte("c"), 12<<10)
	for _, index := range []uint32{epochTestShardIndex, 1, 2} {
		_, err := c.Put(ctx, epochServerNode, blob.PutRequest{Key: epochKey(), Index: index, Size: int64(len(body)), Epoch: 5}, bytes.NewReader(body))
		require.NoError(t, err)
		_, err = c.Commit(ctx, epochServerNode, blob.CommitRequest{Key: epochKey(), Index: index, Epoch: 5})
		require.NoError(t, err)
	}
	flipFragmentIn(t, filepath.Join(dir, "0000000000000000.seg"))
	_, err := get(t, c, 5)
	require.Error(t, err)
	require.True(t, statSuspect(t, c))

	before := st.Snapshot().CompactionCycles
	_, err = c.Delete(ctx, epochServerNode, blob.DeleteRequest{Key: epochKey(), Index: 1})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(dir, "0000000000000000.seg"))
		return os.IsNotExist(err) && st.Snapshot().CompactionCycles > before
	}, 10*time.Second, 5*time.Millisecond, "compaction must move the shard out of its first segment")

	assert.True(t, statSuspect(t, c), "the moved shard is still the bad generation")
	_, err = get(t, c, 5)
	assert.Error(t, err, "compaction copies the bad ciphertext as it was")
}

// A store from an earlier release has no key check. Once upgraded, its key
// can verify, but a shard that then fails authentication is never marked:
// such a store could verify a wrong key, and repair must not act on it.
func TestAnUpgradedStoreMarksNoShardSuspect(t *testing.T) {
	dir := t.TempDir()
	aead := storetest.TestAEAD()
	st, err := engine.Open(dir, engine.WithAEAD(aead))
	require.NoError(t, err)
	body := bytes.Repeat([]byte("u"), 4096)
	w, err := st.Append(epochKey(), epochTestShardIndex, int64(len(body)), 5)
	require.NoError(t, err)
	_, err = w.Write(body)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	_, err = st.Commit(epochKey(), epochTestShardIndex, 5)
	require.NoError(t, err)
	require.NoError(t, st.Close())
	require.NoError(t, os.Remove(filepath.Join(dir, "keycheck")))

	st, err = engine.Open(dir, engine.WithAEAD(aead))
	require.NoError(t, err)
	r, err := st.Lookup(epochKey(), epochTestShardIndex)
	require.NoError(t, err)
	_, err = io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Eventually(t, st.KeyVerified, 5*time.Second, time.Millisecond, "a read of the old shard verifies the key")

	c := startStoreNode(t, st)
	flipFirstFragment(t, dir)
	_, err = get(t, c, 5)
	require.Error(t, err)
	assert.False(t, statSuspect(t, c), "an upgraded store must not mark a shard for repair")
}
