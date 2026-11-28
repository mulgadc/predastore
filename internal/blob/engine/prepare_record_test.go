package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// replayPrepareRecord models recovery when the caller has an authoritative epoch.
// The test scan runs off the normal request path.
func replayPrepareRecord(t *testing.T, store *Store, key [32]byte, index uint32, epoch uint64) []byte {
	t.Helper()
	require.NoError(t, store.replayRecoveredPrepares(t.Context(), func(_ context.Context, candidate [32]byte, shard uint32, candidateEpoch uint64) (bool, error) {
		return candidate == key && shard == index && candidateEpoch == epoch, nil
	}))
	reader, err := store.LookupAt(key, index, epoch)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	body, err := io.ReadAll(io.NewSectionReader(reader, 0, reader.Size()))
	require.NoError(t, err)
	return body
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
	require.Equal(t, []byte("previous durable generation"), readValue(t, reopened, key, 0))
	_, err = reopened.LookupAt(key, 0, acknowledged)
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, reopened.Close())

	authorityUnavailable := errors.New("authority unavailable")
	failed, err := Open(dir, WithAEAD(testAEAD(t)), WithStartupPrepareRecovery(t.Context(), func(context.Context, [32]byte, uint32, uint64) (bool, error) {
		return false, authorityUnavailable
	}))
	require.Nil(t, failed)
	require.ErrorIs(t, err, authorityUnavailable)

	recovered, err := Open(dir, WithAEAD(testAEAD(t)), WithStartupPrepareRecovery(t.Context(), func(_ context.Context, candidate [32]byte, shard uint32, candidateEpoch uint64) (bool, error) {
		return candidate == key && shard == 0 && candidateEpoch == acknowledged, nil
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })
	require.Equal(t, expected, readValue(t, recovered, key, 0))
	_, err = recovered.LookupAt(key, 0, abandoned)
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

func TestPrepareExtentRejectsIncompleteAndMismatchedPayloads(t *testing.T) {
	store, _ := openTestStore(t)
	key := [32]byte{1}
	size := int64(fragBodySize * 2)
	epoch := nextTestEpoch()
	w, err := store.Append(key, 0, size, epoch)
	require.NoError(t, err)
	pending, ok := w.(*writer)
	require.True(t, ok)
	// The sidecar allocation exists. Only the first fragment reached the file.
	written, err := w.Write(bytes.Repeat([]byte{0x35}, fragBodySize))
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, fragBodySize, written)
	require.NoError(t, pending.flush(false))
	require.NoError(t, w.Discard())
	require.ErrorIs(t, store.verifyPrepareExtent(key, 0, pending.ext), ErrIntegrity)
	_, err = store.LookupAt(key, 0, epoch)
	require.ErrorIs(t, err, ErrKeyNotFound)

	putValue(t, store, key, 0, bytes.Repeat([]byte{0x35}, int(size)))
	valid := extentOf(t, store, key, 0)
	require.NoError(t, store.verifyPrepareExtent(key, 0, valid))
	require.ErrorIs(t, store.verifyPrepareExtent([32]byte{2}, 0, valid), ErrIntegrity)

	// A short logical size can have the right physical fragment count.
	valid.LSize--
	require.ErrorIs(t, store.verifyPrepareExtent(key, 0, valid), ErrIntegrity)
}

func TestStageRecoveredPrepareRefusesTombstonedExtent(t *testing.T) {
	store, _ := openTestStore(t)
	key := [32]byte{1}
	epoch := prepareValue(t, store, key, 0, []byte("reclaimed payload"))
	_, err := store.Commit(key, 0, epoch)
	require.NoError(t, err)
	location := extentOf(t, store, key, 0)

	deleted, err := store.DeleteEpoch(key, 0, epoch)
	require.NoError(t, err)
	require.True(t, deleted)
	require.NoError(t, store.verifyPrepareExtent(key, 0, location))
	require.ErrorIs(t, store.stageRecoveredPrepare(key, 0, epoch, location), ErrNotPrepared)
	_, err = store.Commit(key, 0, epoch)
	require.ErrorIs(t, err, ErrNotPrepared)
}
