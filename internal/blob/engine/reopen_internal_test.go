package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSecondOpenCannotChangeStateOfAnOpenStore(t *testing.T) {
	first, dir := openTestStore(t)
	key := [32]byte{0xce}
	body := []byte("acknowledged payload")
	putValue(t, first, key, 0, body)

	statePath := filepath.Join(dir, stateFilename)
	before, err := os.ReadFile(statePath)
	require.NoError(t, err)

	second, err := Open(dir, WithAEAD(testAEAD(t)), WithFreeSpaceWatermark(0, 0))
	if second != nil {
		t.Cleanup(func() { _ = second.Close() })
	}
	require.Error(t, err)
	after, err := os.ReadFile(statePath)
	require.NoError(t, err)
	require.Equal(t, before, after)

	require.NoError(t, first.Close())
	reopened, err := Open(dir, WithAEAD(testAEAD(t)), WithFreeSpaceWatermark(0, 0))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	require.Equal(t, body, readValue(t, reopened, key, 0))
}
