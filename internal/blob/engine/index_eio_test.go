package engine

import (
	"context"
	"crypto/sha256"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
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
	defer func() {
		if store != nil {
			require.NoError(t, store.Close())
		}
	}()
	overwrite := [32]byte{0x31}
	witness := [32]byte{0x72}
	previous := []byte("previous acknowledged generation")
	current := []byte("new acknowledged generation")
	sentinel := []byte("independent acknowledged shard")

	switch phase {
	case "seed":
		putValue(t, store, overwrite, 0, previous)
		putValue(t, store, witness, 0, sentinel)
	case "commit", "shutdown":
		shutdown, stop := signal.NotifyContext(t.Context(), syscall.SIGTERM)
		defer stop()
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
			require.NoError(t, os.WriteFile(filepath.Join(control, "acknowledged"), []byte(strconv.FormatUint(epoch, 10)), 0600))
		}
		if phase == "shutdown" {
			<-shutdown.Done()
			t.Log("shutdown requested")
			return
		}
		require.Eventually(t, func() bool {
			_, err := os.Stat(filepath.Join(control, "close"))
			return err == nil
		}, 60*time.Second, 10*time.Millisecond)
	case "verify":
		require.Equal(t, sentinel, readValue(t, store, witness, 0))
		want := previous
		if acknowledged, err := os.ReadFile(filepath.Join(control, "acknowledged")); err == nil {
			want = current
			if os.Getenv("PREDASTORE_INDEX_EIO_VERIFY_SIDECAR") == "1" {
				epoch, err := strconv.ParseUint(string(acknowledged), 10, 64)
				require.NoError(t, err)
				require.Equal(t, previous, readValue(t, store, overwrite, 0), "the live row must be stale for the replay probe")
				require.NoError(t, store.Close())
				store = nil
				staged, err := Open(dir, WithAEAD(testAEAD(t)), WithFreeSpaceWatermark(0, 0), WithStartupPrepareRecovery(t.Context(),
					func(_ context.Context, key [32]byte, index uint32, candidate uint64) (bool, error) {
						return key == overwrite && index == 0 && candidate == epoch, nil
					}))
				require.NoError(t, err)
				store = staged
			}
		}
		require.Equal(t, want, readValue(t, store, overwrite, 0))
	default:
		t.Fatalf("unknown fixture phase %q", phase)
	}
}
