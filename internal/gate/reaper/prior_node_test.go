package reaper

import (
	"bytes"
	"context"
	"encoding/gob"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/mulgadc/predastore/internal/blob/blobtest"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gateMeta is one meta store both a gate's handlers and its reaper run on, so
// the reaper reads the tombstone the handler wrote.
type gateMeta struct {
	mu   sync.Mutex
	rows map[string][]byte
}

func newGateMeta() *gateMeta { return &gateMeta{rows: make(map[string][]byte)} }

func (m *gateMeta) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.rows[key]
	if !ok {
		return nil, meta.ErrNotFound
	}
	return append([]byte(nil), v...), nil
}

func (m *gateMeta) LeaderGet(ctx context.Context, key string) ([]byte, error) {
	return m.Get(ctx, key)
}

func (m *gateMeta) Put(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows[key] = append([]byte(nil), value...)
	return nil
}

func (m *gateMeta) Swap(_ context.Context, key string, value []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	previous := m.rows[key]
	m.rows[key] = append([]byte(nil), value...)
	return previous, nil
}

func (m *gateMeta) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.rows, key)
	return nil
}

func (m *gateMeta) Scan(ctx context.Context, prefix string, limit int) ([]meta.Item, error) {
	return m.ScanFrom(ctx, prefix, "", limit)
}

func (m *gateMeta) ScanFrom(_ context.Context, prefix, after string, limit int) ([]meta.Item, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := slices.Sorted(maps.Keys(m.rows))
	var items []meta.Item
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) || k <= after {
			continue
		}
		items = append(items, meta.Item{Key: k, Value: append([]byte(nil), m.rows[k]...)})
		if limit > 0 && len(items) == limit {
			break
		}
	}
	return items, nil
}

var (
	_ MetaClient          = (*gateMeta)(nil)
	_ handlers.MetaClient = (*gateMeta)(nil)
)

// tombstones decodes every delete tombstone in the objects table, whatever
// prefix names it.
func (m *gateMeta) tombstones(t *testing.T) []handlers.DeletedObjectInfo {
	t.Helper()
	items, err := m.Scan(context.Background(), handlers.TableKey(model.TableObjects, "deleted"), 0)
	require.NoError(t, err)
	infos := make([]handlers.DeletedObjectInfo, 0, len(items))
	for _, item := range items {
		var info handlers.DeletedObjectInfo
		require.NoError(t, gob.NewDecoder(bytes.NewReader(item.Value)).Decode(&info))
		infos = append(infos, info)
	}
	return infos
}

// A delete to a node that predates scoped deletes leaves its shards owed. The
// reaper must keep owing them, without an unscoped delete, until the node is
// upgraded, and then reclaim them and drop the tombstone.
func TestTheReaperReclaimsFromAPriorNodeOnlyOnceItIsUpgraded(t *testing.T) {
	t.Parallel()

	bc, node, prior := blobtest.StartPriorNode(t)
	mc := newGateMeta()
	ctx := context.Background()
	const bucket, key = "prior-bucket", "prior.txt"
	const epoch uint64 = 5151
	objectHash := model.ObjectHash(bucket, key)
	body := []byte("a shard owed to the reaper")

	prior.Seed(t, objectHash, 0, epoch, body)
	prior.Seed(t, objectHash, 1, epoch, body)
	record, err := handlers.EncodePlacement(handlers.ObjectToShardNodes{
		Size:             int64(len(body)),
		WriteEpoch:       epoch,
		DataShardNodes:   []config.NodeID{node},
		ParityShardNodes: []config.NodeID{node},
	})
	require.NoError(t, err)
	require.NoError(t, mc.Put(ctx, handlers.TableKey(model.TableObjects, string(objectHash[:])), record))

	cache := handlers.NewBucketCache([]handlers.BucketConfig{{Name: bucket, Region: "ap-southeast-2"}})
	reqCtx := handlers.WithObject(handlers.WithBucket(ctx, model.Bucket{Name: bucket}),
		model.Object{Bucket: model.Bucket{Name: bucket}, Key: key})
	req := httptest.NewRequest(http.MethodDelete, "/"+bucket+"/"+key, nil).WithContext(reqCtx)
	rr := httptest.NewRecorder()
	handlers.DeleteObject(mc, bc, cache, handlers.Config{Region: "ap-southeast-2"}).ServeHTTP(rr, req)
	require.Equal(t, http.StatusNoContent, rr.Code, rr.Body.String())
	require.Len(t, mc.tombstones(t), 1, "setup: the rejected inline delete must leave a tombstone")
	rejectedInline := prior.RejectedDeleteEpochs()

	svc, err := New(Config{Meta: mc, Blob: bc, Workers: 2})
	require.NoError(t, err)

	require.NoError(t, svc.Pass(ctx))
	assert.Greater(t, prior.RejectedDeleteEpochs(), rejectedInline, "setup: the pass must reach the node")
	assert.Zero(t, prior.UnscopedDeletes(), "a rejected scoped delete must not fall back to an unscoped one")
	assert.Len(t, mc.tombstones(t), 1, "a tombstone whose shards were not reclaimed must stay")
	for index := range uint32(2) {
		held, ok := prior.Holds(t, objectHash, index)
		require.True(t, ok, "shard %d was removed by a delete the node rejected", index)
		assert.Equal(t, epoch, held)
	}
	assert.Equal(t, int64(1), svc.Stats().Pending)

	prior.Upgrade()
	require.NoError(t, svc.Pass(ctx))
	for index := range uint32(2) {
		_, ok := prior.Holds(t, objectHash, index)
		assert.False(t, ok, "shard %d must be reclaimed once the node is upgraded", index)
	}
	assert.Empty(t, mc.tombstones(t), "a reclaimed tombstone must be dropped")
	assert.Zero(t, prior.UnscopedDeletes())
	assert.Zero(t, svc.Stats().Pending)
}
