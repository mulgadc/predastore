// The upgrade scenario passes when this probe stops listing a tombstone, so a
// probe that misses a row would report a reclaim that never happened. These
// tests hold it to the prefixes and paging the reaper uses.

package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeMeta answers ScanFrom over a sorted key space, the way a replica does.
type fakeMeta struct {
	rows  map[string][]byte
	calls int
	err   error
}

func (f *fakeMeta) ScanFrom(_ context.Context, prefix, after string, limit int) ([]meta.Item, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	keys := make([]string, 0, len(f.rows))
	for k := range f.rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []meta.Item
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) || k <= after {
			continue
		}
		out = append(out, meta.Item{Key: k, Value: f.rows[k]})
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func encodeInfo(t *testing.T, info handlers.DeletedObjectInfo) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(info))
	return buf.Bytes()
}

// A legacy row and a scoped row are both owed shards, and the reaper reads
// both prefixes, so the probe must list both. A placement row under the same
// table is not a tombstone and must not be listed.
func TestListTombstonesReadsBothPrefixes(t *testing.T) {
	hash := model.ObjectHash("b", "legacy")
	f := &fakeMeta{rows: map[string][]byte{
		handlers.TableKey(model.TableObjects, handlers.DeletedObjectPrefix+"#aa/b/legacy"): encodeInfo(t,
			handlers.DeletedObjectInfo{Bucket: "b", Key: "legacy", ObjectHash: hash, WriteEpoch: 3}),
		handlers.TableKey(model.TableObjects, handlers.DeletedObjectKey("b", "scoped", hash, 9)): encodeInfo(t,
			handlers.DeletedObjectInfo{Bucket: "b", Key: "scoped", ObjectHash: hash, WriteEpoch: 9}),
		handlers.TableKey(model.TableObjects, string(hash[:])): []byte("placement"),
	}}

	var bad bytes.Buffer
	rows, err := listTombstones(context.Background(), f, 16, &bad)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, "prefix=deleted: bucket=b key=legacy epoch=3", rows[0].String())
	assert.Equal(t, "prefix=deleted-epoch: bucket=b key=scoped epoch=9", rows[1].String())
	assert.Empty(t, bad.String())
}

// A page that comes back full means there may be more, so the listing must
// continue from the last key rather than stop at the first page.
func TestListTombstonesPagesPastAFullPage(t *testing.T) {
	f := &fakeMeta{rows: map[string][]byte{}}
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		f.rows[handlers.TableKey(model.TableObjects, handlers.DeletedObjectPrefix+k)] = encodeInfo(t,
			handlers.DeletedObjectInfo{Bucket: "b", Key: k, WriteEpoch: 1})
	}

	rows, err := listTombstones(context.Background(), f, 2, &bytes.Buffer{})
	require.NoError(t, err)
	got := make([]string, 0, len(rows))
	for _, r := range rows {
		got = append(got, r.Info.Key)
	}
	assert.Equal(t, []string{"a", "b", "c", "d", "e"}, got)
}

// A row the reaper cannot decode is still held by the cluster, so it is named
// on the side channel instead of disappearing from the output.
func TestListTombstonesReportsUndecodableRows(t *testing.T) {
	key := handlers.TableKey(model.TableObjects, handlers.DeletedEpochPrefix+"junk")
	f := &fakeMeta{rows: map[string][]byte{key: []byte("not gob")}}

	var bad bytes.Buffer
	rows, err := listTombstones(context.Background(), f, 16, &bad)
	require.NoError(t, err)
	assert.Empty(t, rows)
	assert.Contains(t, bad.String(), "junk")
}

// A scan that fails must fail the listing, or an unreachable cluster would
// read as one that holds no tombstone.
func TestListTombstonesFailsWhenTheScanFails(t *testing.T) {
	f := &fakeMeta{err: errors.New("no replica answered")}

	_, err := listTombstones(context.Background(), f, 16, &bytes.Buffer{})
	require.Error(t, err)
	assert.Equal(t, 1, f.calls)
}
