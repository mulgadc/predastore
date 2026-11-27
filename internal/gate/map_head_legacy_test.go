package gate

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/require"
)

type opcodeListener struct {
	transport.Listener

	lastKey *atomic.Int64
}

func (l opcodeListener) Accept(ctx context.Context) (transport.Conn, error) {
	conn, err := l.Listener.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return opcodeConn{Conn: conn, lastKey: l.lastKey}, nil
}

type opcodeConn struct {
	transport.Conn

	lastKey *atomic.Int64
}

func (c opcodeConn) AcceptStream(ctx context.Context) (transport.Stream, error) {
	stream, err := c.Conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return &opcodeStream{Stream: stream, lastKey: c.lastKey}, nil
}

type opcodeStream struct {
	transport.Stream

	lastKey *atomic.Int64
	header  [4]byte
	read    int
}

func (s *opcodeStream) Read(p []byte) (int, error) {
	n, err := s.Stream.Read(p)
	if s.read < len(s.header) {
		s.read += copy(s.header[s.read:], p[:n])
		if s.read == len(s.header) && rpc.Opcode(binary.BigEndian.Uint32(s.header[:])) == meta.OpMetaLastKey {
			s.lastKey.Add(1)
		}
	}
	return n, err
}

type multiplexedLastKeyClient struct {
	*meta.Client

	rpc *rpc.Client
}

func (c multiplexedLastKeyClient) LeaderLastKey(ctx context.Context, prefix string) (string, error) {
	request := &meta.MetaRequest{Key: []byte(prefix), Leader: true, Last: true, Limit: 1}
	stream, err := rpc.OpenStream(ctx, c.rpc, 1, meta.OpMetaScan, request)
	if err != nil {
		return "", err
	}
	if err := stream.Close(); err != nil {
		return "", err
	}
	var result meta.MetaResponse
	if err := json.NewDecoder(stream).Decode(&result); err != nil {
		return "", err
	}
	if !result.Last {
		return "", meta.ErrNoLeaderRead
	}
	return "", nil
}

func TestSignedMapHeadLookupDoesNotScanOnLegacyReplica(t *testing.T) {
	const host = "map-head-old-dispatch"
	cfg := &config.Config{Hosts: []config.Host{{ID: 1, Addr: host, Nodes: []config.Node{
		{ID: 1, Role: config.RoleMeta, Port: 6331},
		{ID: 2, Role: config.RoleGate, Port: 6332},
	}}}}
	serverTransport := transport.NewPipeTransport(host, 6331)
	t.Cleanup(func() { serverTransport.Close() })
	listener, err := serverTransport.Listen()
	require.NoError(t, err)

	var scans, dedicatedRequests atomic.Int64
	mux := rpc.NewMux()
	rpc.RegisterHandler(mux, meta.OpMetaScan, func(_ context.Context, _ meta.MetaRequest, stream transport.Stream) error {
		scans.Add(1)
		return json.NewEncoder(stream).Encode(meta.MetaResponse{})
	})
	rpc.RegisterHandler(mux, meta.OpMetaGet, func(_ context.Context, _ meta.MetaRequest, stream transport.Stream) error {
		return json.NewEncoder(stream).Encode(meta.MetaResponse{Err: meta.ErrCodeNotFound})
	})
	server, err := rpc.NewServer(mux, []transport.Listener{opcodeListener{Listener: listener, lastKey: &dedicatedRequests}}, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, <-done)
	})

	clientTransport := transport.NewPipeTransport("old-dispatch-test-client", 0)
	t.Cleanup(func() { clientTransport.Close() })
	resolver, err := rpc.NewResolver(cfg, 2, clientTransport)
	require.NoError(t, err)
	pool := rpc.NewConnPool(2, resolver)
	t.Cleanup(func() { pool.Close() })
	rpcClient := rpc.NewClient(pool)
	client, err := meta.NewClient(meta.ClientConfig{Client: rpcClient, Replicas: []config.NodeID{1}})
	require.NoError(t, err)

	gateConfig := newAuthTestConfig()
	gateConfig.Meta = client
	g := keyGate{t: t, server: newTestGate(t, gateConfig), region: gateConfig.Region}
	query := url.Values{"list-type": {"2"}, "prefix": {"vol/map/head."}, "max-keys": {"1"}}
	path := "/local?" + query.Encode()
	header := http.Header{handlers.MapHeadReadModeHeader: {"latest-map-head"}}

	require.Equal(t, http.StatusOK, g.do(http.MethodGet, path, nil, nil).Code)
	require.Positive(t, scans.Load())
	scans.Store(0)
	gateConfig.Meta = multiplexedLastKeyClient{Client: client, rpc: rpcClient}
	oldGate := keyGate{t: t, server: newTestGate(t, gateConfig), region: gateConfig.Region}
	oldResult := oldGate.do(http.MethodGet, path, nil, header)
	require.Equal(t, http.StatusServiceUnavailable, oldResult.Code)
	require.Empty(t, oldResult.Header().Get(handlers.MapHeadReadModeHeader))
	require.EqualValues(t, 1, scans.Load())

	scans.Store(0)
	newResult := g.do(http.MethodGet, path, nil, header)
	require.Equal(t, http.StatusServiceUnavailable, newResult.Code)
	require.Empty(t, newResult.Header().Get(handlers.MapHeadReadModeHeader))
	require.EqualValues(t, 1, dedicatedRequests.Load(), "the old dispatcher must receive the dedicated opcode")
	require.Zero(t, scans.Load())
}
