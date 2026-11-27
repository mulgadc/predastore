package blob_test

import (
	"bytes"
	"context"
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
func startEngineNode(t *testing.T) (*blob.Client, string) {
	t.Helper()
	dir := t.TempDir()
	st, err := engine.Open(dir, engine.WithAEAD(storetest.TestAEAD()))
	require.NoError(t, err)
	return startStoreNode(t, st), dir
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
	f, err := os.OpenFile(held[0], os.O_RDWR, 0)
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
