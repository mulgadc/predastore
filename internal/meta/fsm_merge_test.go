package meta

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unsortedFrames writes the retired frame format without a format marker.
func unsortedFrames(t *testing.T, keys []string, data map[string][]byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	var lenBuf [4]byte
	write := func(b []byte) {
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
		_, err := buf.Write(lenBuf[:])
		require.NoError(t, err)
		_, err = buf.Write(b)
		require.NoError(t, err)
	}
	for _, k := range keys {
		write([]byte(k))
		write(data[k])
	}

	return buf.Bytes()
}

// contents reads the whole store back, which is what every assertion here is
// really about: the state after a restore must be the snapshot's state exactly.
func contents(t *testing.T, db *badger.DB) map[string]string {
	t.Helper()

	got := map[string]string{}
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			got[string(it.Item().Key())] = string(v)
		}
		return nil
	}))

	return got
}

// The four classifications a merge has to get right, in one store: a key it
// must leave alone, one it must overwrite, one it must add and one it must
// delete. Getting the last wrong is the dangerous one -- a restore that only
// writes what the snapshot names leaves an object visible that the cluster
// deleted while this node was away.
func TestTheMergeWritesOnlyWhatDiffers(t *testing.T) {
	logs := captureLogs(t)

	db := newTestDB(t)
	dbSet(t, db, []byte("a/same"), []byte("v"))
	dbSet(t, db, []byte("b/changed"), []byte("old"))
	dbSet(t, db, []byte("c/orphan"), []byte("gone in the snapshot"))

	stream := snapshotOf(t, map[string][]byte{
		"a/same":    []byte("v"),
		"b/changed": []byte("new"),
		"d/added":   []byte("fresh"),
	})
	require.NoError(t, NewFSM(db).Restore(io.NopCloser(bytes.NewReader(stream))))

	assert.Equal(t, map[string]string{
		"a/same":    "v",
		"b/changed": "new",
		"d/added":   "fresh",
	}, contents(t, db))

	// The counts are not decoration: after the repair set is derived from this
	// merge, added plus changed is the queue depth, so a wrong count is a wrong
	// repair set rather than a cosmetic log line.
	assert.Contains(t, logs.String(), "added=1")
	assert.Contains(t, logs.String(), "changed=1")
	assert.Contains(t, logs.String(), "deleted=1")
	assert.Contains(t, logs.String(), "unchanged=1")
}

// The merge reaches the same state whether it runs once or twice, which is what
// makes it safe to re-run after an interrupted restore. The old path could not
// claim this: it passed through an empty store between the drop and the flush,
// so a crash in the middle left the node with no metadata at all.
func TestARestoreThatRunsTwiceConverges(t *testing.T) {
	db := newTestDB(t)
	dbSet(t, db, []byte("stale"), []byte("v"))

	stream := snapshotOf(t, map[string][]byte{"a": []byte("1"), "b": []byte("2")})
	for range 2 {
		require.NoError(t, NewFSM(db).Restore(io.NopCloser(bytes.NewReader(stream))))
		assert.Equal(t, map[string]string{"a": "1", "b": "2"}, contents(t, db))
	}
}

// A truncated stream must fail rather than commit the prefix it managed to
// read. Half a snapshot restored as though it were whole is silent metadata
// loss, and on the merge path it would also delete every key past the cut.
func TestATruncatedSnapshotIsRefused(t *testing.T) {
	stream := snapshotOf(t, map[string][]byte{
		"a": bytes.Repeat([]byte("x"), 64),
		"b": bytes.Repeat([]byte("y"), 64),
	})

	t.Run("value body", func(t *testing.T) {
		db := newTestDB(t)
		err := NewFSM(db).Restore(io.NopCloser(bytes.NewReader(stream[:len(stream)-8])))
		require.ErrorContains(t, err, "read value")
	})
	t.Run("absent key body", func(t *testing.T) {
		db := newTestDB(t)
		dbSet(t, db, []byte("b"), []byte("acknowledged"))
		keyBodyOffset := len(snapshotMagic) + 4 + len("a") + 4 + 64 + 4
		err := NewFSM(db).Restore(io.NopCloser(bytes.NewReader(stream[:keyBodyOffset])))
		require.ErrorContains(t, err, "read key")
		assert.Equal(t, "acknowledged", contents(t, db)["b"])
	})
}

// The claim C4 makes, stated as a property rather than as a duration: capturing
// a snapshot allocates the same amount whether the store holds sixteen keys or
// four thousand. It used to allocate the whole keyspace into a map, on the
// goroutine raft also runs Apply on, every snapshot interval forever.
func TestTheCaptureDoesNotScaleWithTheStore(t *testing.T) {
	measure := func(keys int) float64 {
		db := newTestDB(t)
		wb := db.NewWriteBatch()
		for i := range keys {
			require.NoError(t, wb.Set(
				[]byte(fmt.Sprintf("objects/%08d", i)), bytes.Repeat([]byte("v"), 128)))
		}
		require.NoError(t, wb.Flush())

		f := NewFSM(db)

		return testing.AllocsPerRun(10, func() {
			snap, err := f.Snapshot()
			require.NoError(t, err)
			snap.Release()
		})
	}

	small, large := measure(16), measure(4000)

	// Bounded rather than equal: badger's own bookkeeping moves by an
	// allocation between runs, while the behaviour being ruled out is the old
	// capture copying every key and value, which would put large in the
	// thousands.
	assert.Less(t, large, 32.0,
		"the capture allocates in proportion to the store, so it is walking it")
	assert.LessOrEqual(t, large, small+2,
		"the capture grew with the store, so it is not a point-in-time view")
}
