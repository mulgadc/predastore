package meta

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unsortedFrames writes the frame format the way this file used to: no marker,
// and in whatever order the keys are given, which for a Go map was arbitrary.
// A merging restore cannot be correct against one of these, so producing one is
// how the fallback gets tested.
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

// A merge join is only correct against a stream in key order, and two unsorted
// formats are already written down on disk somewhere: the legacy JSON map, and
// the frame stream this file produced while it iterated a Go map. Both must
// still restore, by clearing and rewriting rather than by merging -- so a
// follower on this version can take a snapshot from a leader on the last one.
func TestAnUnsortedSnapshotIsStillRestored(t *testing.T) {
	data := map[string][]byte{
		"a/first":  []byte("1"),
		"b/second": []byte("2"),
		"c/third":  []byte("3"),
	}
	want := map[string]string{"a/first": "1", "b/second": "2", "c/third": "3"}

	t.Run("frames in the wrong order", func(t *testing.T) {
		keys := make([]string, 0, len(data))
		for k := range data {
			keys = append(keys, k)
		}
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))

		db := newTestDB(t)
		dbSet(t, db, []byte("stale"), []byte("must not survive"))
		require.NoError(t, NewFSM(db).Restore(
			io.NopCloser(bytes.NewReader(unsortedFrames(t, keys, data)))))

		assert.Equal(t, want, contents(t, db))
	})

	t.Run("legacy json", func(t *testing.T) {
		body, err := json.Marshal(data)
		require.NoError(t, err)

		db := newTestDB(t)
		dbSet(t, db, []byte("stale"), []byte("must not survive"))
		require.NoError(t, NewFSM(db).Restore(io.NopCloser(bytes.NewReader(body))))

		assert.Equal(t, want, contents(t, db))
	})
}

// A truncated stream must fail rather than commit the prefix it managed to
// read. Half a snapshot restored as though it were whole is silent metadata
// loss, and on the merge path it would also delete every key past the cut.
func TestATruncatedSnapshotIsRefused(t *testing.T) {
	stream := snapshotOf(t, map[string][]byte{
		"a": bytes.Repeat([]byte("x"), 64),
		"b": bytes.Repeat([]byte("y"), 64),
	})

	db := newTestDB(t)
	err := NewFSM(db).Restore(io.NopCloser(bytes.NewReader(stream[:len(stream)-8])))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read value")
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

	assert.Equal(t, measure(16), measure(4000),
		"the capture allocates in proportion to the store, so it is walking it")
}
