package handlers

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
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

type wireMapHeadMeta struct {
	*fakeMeta

	client *meta.Client
}

func (m wireMapHeadMeta) LeaderLastKey(ctx context.Context, prefix string) (string, error) {
	return m.client.LeaderLastKey(ctx, prefix)
}

func TestMapHeadListRejectsPreExtensionMetadataReplies(t *testing.T) {
	const host = "map-head-legacy-meta"
	cfg := &config.Config{Hosts: []config.Host{{ID: 1, Addr: host, Nodes: []config.Node{
		{ID: 1, Role: config.RoleMeta, Port: 6321},
		{ID: 2, Role: config.RoleGate, Port: 6322},
	}}}}
	serverTransport := transport.NewPipeTransport(host, 6321)
	t.Cleanup(func() { serverTransport.Close() })
	listener, err := serverTransport.Listen()
	require.NoError(t, err)

	var response atomic.Pointer[meta.MetaResponse]
	mux := rpc.NewMux()
	rpc.RegisterHandler(mux, meta.OpMetaLastKey, func(_ context.Context, _ meta.MetaRequest, stream transport.Stream) error {
		return json.NewEncoder(stream).Encode(response.Load())
	})
	svc, err := rpc.NewServer(mux, []transport.Listener{listener}, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})

	clientTransport := transport.NewPipeTransport("map-head-legacy-client", 0)
	t.Cleanup(func() { clientTransport.Close() })
	resolver, err := rpc.NewResolver(cfg, 2, clientTransport)
	require.NoError(t, err)
	pool := rpc.NewConnPool(2, resolver)
	t.Cleanup(func() { pool.Close() })
	client, err := meta.NewClient(meta.ClientConfig{Client: rpc.NewClient(pool), Replicas: []config.NodeID{1}})
	require.NoError(t, err)
	fixture := newListFixture(t)
	gate := ListObjects(wireMapHeadMeta{fakeMeta: fixture.mc, client: client}, NewBucketCache([]BucketConfig{{Name: listTestBucket}}))
	const prefix = "vol/map/head."
	query := url.Values{"list-type": {"2"}, "prefix": {prefix}, "max-keys": {"1"}}
	oldKey := TableKey(model.TableObjects, objectARN(listTestBucket, prefix+"00000000000000000001.00000000000000000000"))
	for _, tc := range []struct {
		name  string
		reply meta.MetaResponse
		code  int
	}{
		{"old empty scan", meta.MetaResponse{}, http.StatusServiceUnavailable},
		{"old nonempty scan", meta.MetaResponse{Items: []meta.ScanItem{{Key: []byte(oldKey)}}}, http.StatusServiceUnavailable},
		{"confirmed empty", meta.MetaResponse{Last: true}, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response.Store(&tc.reply)
			req := httptest.NewRequest(http.MethodGet, "/"+listTestBucket+"?"+query.Encode(), nil)
			req = req.WithContext(WithBucket(req.Context(), model.Bucket{Name: listTestBucket}))
			req.Header.Set(MapHeadReadModeHeader, latestMapHeadMode)
			recorder := httptest.NewRecorder()
			gate.ServeHTTP(recorder, req)
			require.Equal(t, tc.code, recorder.Code)
			if tc.code == http.StatusOK {
				require.Equal(t, latestMapHeadMode, recorder.Header().Get(MapHeadReadModeHeader))
			} else {
				require.Empty(t, recorder.Header().Get(MapHeadReadModeHeader))
			}
		})
	}
}
