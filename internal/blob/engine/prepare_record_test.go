package engine

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// replayPrepareRecord models recovery when the caller has an authoritative epoch.
// The test scan runs off the normal request path.
func replayPrepareRecord(t *testing.T, store *Store, key [32]byte, index uint32, epoch uint64) []byte {
	t.Helper()
	wantKey := MakeKey(key, index)
	entries, err := filepath.Glob(filepath.Join(store.dir, "*.idx"))
	require.NoError(t, err)
	for _, name := range entries {
		var segment uint64
		_, err := fmt.Sscanf(filepath.Base(name), "%d.idx", &segment)
		require.NoError(t, err)
		data, err := os.ReadFile(name)
		require.NoError(t, err)
		allocations := make(map[int64]idxEntry)
		for offset := 0; offset+idxEntrySize <= len(data); offset += idxEntrySize {
			row := data[offset : offset+idxEntrySize]
			if row[0]&0x80 == 0 {
				allocation, err := decodeIdxEntry(row)
				require.NoError(t, err)
				allocations[allocation.Off] = allocation
				continue
			}
			record, ok := decodePrepareRecord(row)
			allocation, found := allocations[record.off]
			if !ok || !found || record.epoch != epoch || !bytes.Equal(allocation.Key[:], wantKey) {
				continue
			}
			require.GreaterOrEqual(t, record.size, int64(0))
			require.Equal(t, (record.size+fragBodySize-1)/fragBodySize*totalFragSize, allocation.PSize)
			extent := extent{SegNum: segment, Off: record.off, PSize: allocation.PSize, LSize: record.size}
			require.NoError(t, store.prepareExtent(key, index, extent, epoch, true))
			_, err = store.Commit(key, index, epoch)
			require.NoError(t, err)
			reader, err := store.LookupAt(key, index, epoch)
			require.NoError(t, err)
			defer func() { require.NoError(t, reader.Close()) }()
			body, err := io.ReadAll(io.NewSectionReader(reader, 0, reader.Size()))
			require.NoError(t, err)
			return body
		}
	}
	t.Fatalf("sidecar has no prepare record for epoch %d", epoch)
	return nil
}

func TestReopenCanReplayExactPreparedEpoch(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, WithAEAD(testAEAD(t)), WithFreeSpaceWatermark(0, 0))
	require.NoError(t, err)

	key := [32]byte{1}
	seed := prepareValue(t, store, key, 0, []byte("previous durable generation"))
	_, err = store.Commit(key, 0, seed)
	require.NoError(t, err)
	keyBytes := MakeKey(key, 0)
	oldRow, err := store.indexGet(keyBytes)
	require.NoError(t, err)

	expected := bytes.Repeat([]byte("new acknowledged generation"), 500)
	acknowledged := prepareValue(t, store, key, 0, expected)
	_, err = store.Commit(key, 0, acknowledged)
	require.NoError(t, err)
	abandoned := prepareValue(t, store, key, 0, []byte("never acknowledged"))
	require.NoError(t, store.index.Update(func(txn *badger.Txn) error {
		return errors.Join(txn.Set(keyBytes, oldRow), txn.Delete(preparedKey(keyBytes, abandoned)))
	}))
	require.NoError(t, store.Close())

	reopened, err := Open(dir, WithAEAD(testAEAD(t)), WithFreeSpaceWatermark(0, 0))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	require.Equal(t, []byte("previous durable generation"), readValue(t, reopened, key, 0))
	_, err = reopened.LookupAt(key, 0, acknowledged)
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.Equal(t, expected, replayPrepareRecord(t, reopened, key, 0, acknowledged))
	_, err = reopened.LookupAt(key, 0, abandoned)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestPrepareRecordSurvivesCompactionSourceDrop(t *testing.T) {
	store, dir := openTestStore(t, WithMaxSegSize(20*KiB))
	key := [32]byte{1}
	value := []byte("acknowledged, relocated payload")
	epoch := prepareValue(t, store, key, 0, value)
	_, err := store.Commit(key, 0, epoch)
	require.NoError(t, err)
	source := extentOf(t, store, key, 0).SegNum

	putValue(t, store, [32]byte{2}, 0, []byte("fill source"))
	putValue(t, store, [32]byte{3}, 0, []byte("roll to next segment"))
	_, err = store.compactSegment(source, nil)
	require.NoError(t, err)
	require.NoFileExists(t, filepath.Join(dir, fmt.Sprintf(idxSegFilename, source)))
	require.NoError(t, store.index.Update(func(txn *badger.Txn) error {
		return txn.Delete(MakeKey(key, 0))
	}))

	require.Equal(t, value, replayPrepareRecord(t, store, key, 0, epoch))
}
