package handlers

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/stretchr/testify/require"
)

type mapHeadMeta struct {
	*fakeMeta

	key string
	err error
}

func (m mapHeadMeta) LeaderLastKey(_ context.Context, prefix string) (string, error) {
	if prefix != TableKey(model.TableObjects, objectARN(listTestBucket, "vol/map/head.")) {
		return "", meta.ErrNoLeaderRead
	}
	return m.key, m.err
}

func TestMapHeadListRequiresLeaderResult(t *testing.T) {
	const header = "X-Predastore-Read-Mode"
	const mode = "latest-map-head"
	old := "vol/map/head.00000000000000000010.00000000000000000000"
	latest := "vol/map/head.00000000000000000020.00000000000000000000"
	fixture := newListFixture(t, old)
	query := url.Values{"list-type": {"2"}, "prefix": {"vol/map/head."}, "max-keys": {"1"}}
	require.Equal(t, old, contentsOf(fixture.list(t, query))[0].Key)
	cache := NewBucketCache([]BucketConfig{{Name: listTestBucket}})

	t.Run("leader ahead of ordinary listing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/"+listTestBucket+"?"+query.Encode(), nil)
		req = req.WithContext(WithBucket(req.Context(), model.Bucket{Name: listTestBucket}))
		req.Header.Set(header, mode)
		recorder := httptest.NewRecorder()
		client := mapHeadMeta{fakeMeta: fixture.mc, key: TableKey(model.TableObjects, objectARN(listTestBucket, latest))}
		ListObjects(client, cache).ServeHTTP(recorder, req)
		require.Equal(t, http.StatusOK, recorder.Code)
		require.Equal(t, mode, recorder.Header().Get(header))
		require.NotContains(t, recorder.Body.String(), "<Size>")
		require.NotContains(t, recorder.Body.String(), "<LastModified>")
		require.NotContains(t, recorder.Body.String(), "<StorageClass>")
		var result ListObjectsV2
		require.NoError(t, xml.NewDecoder(recorder.Body).Decode(&result))
		require.Len(t, contentsOf(result), 1)
		require.Equal(t, latest, contentsOf(result)[0].Key)
		require.False(t, result.IsTruncated)
	})

	t.Run("unavailable leader", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/"+listTestBucket+"?"+query.Encode(), nil)
		req = req.WithContext(WithBucket(req.Context(), model.Bucket{Name: listTestBucket}))
		req.Header.Set(header, mode)
		recorder := httptest.NewRecorder()
		ListObjects(mapHeadMeta{fakeMeta: fixture.mc, err: meta.ErrNoLeaderRead}, cache).ServeHTTP(recorder, req)
		require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
		require.Empty(t, recorder.Header().Get(header))
	})
}
