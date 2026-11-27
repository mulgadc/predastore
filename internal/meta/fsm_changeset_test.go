package meta

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// changeSet collects what a restore reported, copying as it goes because the
// callback lends its buffers for the duration of the call only.
func changeSet(t *testing.T) (map[string]string, func(key, value []byte)) {
	t.Helper()

	got := map[string]string{}

	return got, func(key, value []byte) {
		got[string(bytes.Clone(key))] = string(bytes.Clone(value))
	}
}

// The claim C6 makes: the merge already knows which objects moved, so the
// repair sweep does not have to rediscover them by scanning the whole
// placement table. What it reports must be the added and changed keys exactly
// -- a key it misses is a shard nobody goes looking for.
func TestTheRestoreReportsWhatItChanged(t *testing.T) {
	db := newTestDB(t)
	dbSet(t, db, []byte("a/same"), []byte("v"))
	dbSet(t, db, []byte("b/changed"), []byte("old"))
	dbSet(t, db, []byte("c/orphan"), []byte("gone in the snapshot"))

	stream := snapshotOf(t, map[string][]byte{
		"a/same":    []byte("v"),
		"b/changed": []byte("new"),
		"d/added":   []byte("fresh"),
	})

	got, onChange := changeSet(t)
	f := NewFSM(db, OnKeyChanged(onChange))
	require.NoError(t, f.Restore(io.NopCloser(bytes.NewReader(stream))))

	// a/same is identical and must not be reported: repairing every key a
	// snapshot mentions is the full scan this replaces.
	assert.Equal(t, map[string]string{
		"b/changed": "new",
		"d/added":   "fresh",
	}, got)
}

// A restore that changes nothing must report nothing, which is what makes an
// empty change set meaningful rather than ambiguous.
func TestARestoreThatChangesNothingReportsNothing(t *testing.T) {
	data := map[string][]byte{"a": []byte("1"), "b": []byte("2")}

	db := newTestDB(t)
	for k, v := range data {
		dbSet(t, db, []byte(k), v)
	}

	got, onChange := changeSet(t)
	require.NoError(t, NewFSM(db, OnKeyChanged(onChange)).Restore(
		io.NopCloser(bytes.NewReader(snapshotOf(t, data)))))

	assert.Empty(t, got)
}

// The callback is optional, and an FSM without one restores exactly as before.
// Every other test in this package constructs one that way.
func TestARestoreWithoutACallbackIsUnaffected(t *testing.T) {
	db := newTestDB(t)
	stream := snapshotOf(t, map[string][]byte{"a": []byte("1")})
	require.NoError(t, NewFSM(db).Restore(io.NopCloser(bytes.NewReader(stream))))

	assert.Equal(t, map[string]string{"a": "1"}, contents(t, db))
}
