package engine

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestShardIndexWriteEIO is driven by a maintainer on a disposable mounted
// device. The control directory lives on a different filesystem. The runner
// observes the prepare marker, changes the mapper, releases the commit, and
// kills its process without calling Store.Close before reopening the disk.
func TestShardIndexWriteEIO(t *testing.T) {
	dir := os.Getenv("PREDASTORE_INDEX_EIO_STORE")
	if dir == "" {
		t.Skip("requires a disposable device-mapper filesystem")
	}
	control := os.Getenv("PREDASTORE_INDEX_EIO_CONTROL")
	require.NotEmpty(t, control)
	phase := os.Getenv("PREDASTORE_INDEX_EIO_PHASE")

	store, err := Open(dir, WithAEAD(testAEAD(t)), WithFreeSpaceWatermark(0, 0))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	overwrite := [32]byte{0x31}
	witness := [32]byte{0x72}
	previous := []byte("previous acknowledged generation")
	current := []byte("new acknowledged generation")
	sentinel := []byte("independent acknowledged shard")

	switch phase {
	case "seed":
		putValue(t, store, overwrite, 0, previous)
		putValue(t, store, witness, 0, sentinel)
	case "commit":
		testEpochs.Store(10)
		epoch := prepareValue(t, store, overwrite, 0, current)
		require.NoError(t, os.WriteFile(filepath.Join(control, "prepared"), nil, 0600))
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(control, "commit"))
			return err == nil
		}, 30*time.Second, 10*time.Millisecond)

		published, err := store.Commit(overwrite, 0, epoch)
		if err != nil {
			t.Logf("commit rejected: %v", err)
			require.NoError(t, os.WriteFile(filepath.Join(control, "rejected"), nil, 0600))
		} else {
			require.True(t, published)
			t.Logf("commit acknowledged: epoch %d checksum %x", epoch, sha256.Sum256(current))
			require.NoError(t, os.WriteFile(filepath.Join(control, "acknowledged"), nil, 0600))
		}
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(control, "close"))
			return err == nil
		}, 60*time.Second, 10*time.Millisecond)
	case "verify":
		want := previous
		if _, err := os.Stat(filepath.Join(control, "acknowledged")); err == nil {
			want = current
		}
		require.Equal(t, sentinel, readValue(t, store, witness, 0))
		require.Equal(t, want, readValue(t, store, overwrite, 0))
	default:
		t.Fatalf("unknown fixture phase %q", phase)
	}
}
