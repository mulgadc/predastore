// Package blobtest runs real blob nodes for tests in other packages. Production
// code never imports it.
package blobtest

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/require"
)

const (
	priorServerPort = 6581
	priorClientPort = 6582

	// PriorNodeID is the node a prior-version fixture serves.
	PriorNodeID config.NodeID = 1

	// headerFrame is the opcode and header length that start every rpc stream.
	headerFrame = 8
	// maxPeekedHeader bounds how much header the front reads itself. A longer
	// one is left for the real server, which rejects it on its own terms.
	maxPeekedHeader = 1 << 16
)

// priorHosts keeps each fixture in its own pipe namespace, so tests in one
// package can each run a node at the same ports in parallel.
var priorHosts atomic.Uint64

// errUnknownOpcode fails the server's header read, which makes the rpc server
// reset the stream with no envelope, as it does for an opcode it has no
// handler for.
var errUnknownOpcode = errors.New("prior-version node: no handler found")

// PriorNode controls a real blob node that answers as a node from before
// OpDeleteEpoch until Upgrade is called. Every other opcode reaches the real
// server and its real store.
type PriorNode struct {
	client   *blob.Client
	upgraded atomic.Bool
	unscoped atomic.Int64
	rejected atomic.Int64
}

// StartPriorNode runs a prior-version node over a fresh engine store and
// returns a real client that reaches it as PriorNodeID.
func StartPriorNode(t testing.TB) (*blob.Client, config.NodeID, *PriorNode) {
	t.Helper()

	host := "prior-node-" + strconv.FormatUint(priorHosts.Add(1), 10)
	clusterCfg := &config.Config{
		Hosts: []config.Host{{
			ID:   1,
			Addr: host,
			Nodes: []config.Node{
				{ID: PriorNodeID, Role: config.RoleBlob, Port: priorServerPort},
				{ID: 2, Role: config.RoleBlob, Port: priorClientPort},
			},
		}},
	}

	node := &PriorNode{}

	serverTr := transport.NewPipeTransport(host, priorServerPort)
	t.Cleanup(func() { serverTr.Close() })
	ln, err := serverTr.Listen()
	require.NoError(t, err)
	srv, err := blob.New(blob.Config{
		NodeID:    PriorNodeID,
		DataDir:   t.TempDir(),
		AEAD:      storetest.TestAEAD(),
		Listeners: []transport.Listener{&priorListener{Listener: ln, node: node}},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	clientTr := transport.NewPipeTransport(host, priorClientPort)
	t.Cleanup(func() { clientTr.Close() })
	clientRes, err := rpc.NewResolver(clusterCfg, 2, clientTr)
	require.NoError(t, err)
	client, err := blob.NewClient(blob.ClientConfig{
		Client:          rpc.NewClient(rpc.NewConnPool(2, clientRes)),
		EnvelopeTimeout: 10 * time.Second,
		IdleTimeout:     10 * time.Second,
	})
	require.NoError(t, err)
	node.client = client

	return client, PriorNodeID, node
}

// Upgrade makes the node answer OpDeleteEpoch, as a node restarted on the
// current version does. The store is kept.
func (p *PriorNode) Upgrade() { p.upgraded.Store(true) }

// UnscopedDeletes counts the OpDelete streams the node has received.
func (p *PriorNode) UnscopedDeletes() int64 { return p.unscoped.Load() }

// RejectedDeleteEpochs counts the OpDeleteEpoch streams the node reset because
// it was not yet upgraded.
func (p *PriorNode) RejectedDeleteEpochs() int64 { return p.rejected.Load() }

// Seed writes and commits one shard at epoch, as a completed write leaves it.
func (p *PriorNode) Seed(t testing.TB, key [32]byte, index uint32, epoch uint64, body []byte) {
	t.Helper()
	ctx := context.Background()
	_, err := p.client.Put(ctx, PriorNodeID, blob.PutRequest{
		Key: key, Index: index, Size: int64(len(body)), Epoch: epoch,
	}, bytes.NewReader(body))
	require.NoError(t, err)
	_, err = p.client.Commit(ctx, PriorNodeID, blob.CommitRequest{Key: key, Index: index, Epoch: epoch})
	require.NoError(t, err)
}

// Holds reports the epoch the node serves at a position, or false when it
// holds nothing there.
func (p *PriorNode) Holds(t testing.TB, key [32]byte, index uint32) (uint64, bool) {
	t.Helper()
	resp, err := p.client.Stat(context.Background(), PriorNodeID, blob.StatRequest{Key: key, Index: index})
	if errors.Is(err, blob.ErrNotFound) {
		return 0, false
	}
	require.NoError(t, err)
	return resp.Epoch, true
}

// priorListener hands the server connections whose streams are screened by
// opcode before the server reads them.
type priorListener struct {
	transport.Listener
	node *PriorNode
}

func (l *priorListener) Accept(ctx context.Context) (transport.Conn, error) {
	conn, err := l.Listener.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return &priorConn{Conn: conn, node: l.node}, nil
}

var _ transport.Listener = (*priorListener)(nil)

type priorConn struct {
	transport.Conn
	node *PriorNode
}

func (c *priorConn) AcceptStream(ctx context.Context) (transport.Stream, error) {
	stream, err := c.Conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return &priorStream{Stream: stream, node: c.node}, nil
}

var _ transport.Conn = (*priorConn)(nil)

// priorStream reads a stream's whole header before the server does. A header
// it lets through is replayed to the server unchanged.
type priorStream struct {
	transport.Stream
	node    *PriorNode
	peeked  bool
	pending []byte
}

func (s *priorStream) Read(p []byte) (int, error) {
	if !s.peeked {
		s.peeked = true
		if err := s.peek(); err != nil {
			return 0, err
		}
	}
	if len(s.pending) > 0 {
		n := copy(p, s.pending)
		s.pending = s.pending[n:]
		return n, nil
	}
	return s.Stream.Read(p)
}

// WriteTo goes through Read, so a server that copies out of the stream still
// sees the replayed header.
func (s *priorStream) WriteTo(w io.Writer) (int64, error) {
	return io.Copy(w, struct{ io.Reader }{s})
}

func (s *priorStream) peek() error {
	frame := make([]byte, headerFrame)
	if _, err := io.ReadFull(s.Stream, frame); err != nil {
		return fmt.Errorf("read metadata: %w", err)
	}
	s.pending = frame
	n := binary.BigEndian.Uint32(frame[4:])
	if n > maxPeekedHeader {
		return nil
	}
	header := make([]byte, n)
	if _, err := io.ReadFull(s.Stream, header); err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	s.pending = append(s.pending, header...)

	switch rpc.Opcode(binary.BigEndian.Uint32(frame)) {
	case blob.OpDelete:
		s.node.unscoped.Add(1)
	case blob.OpDeleteEpoch:
		if !s.node.upgraded.Load() {
			s.node.rejected.Add(1)
			return errUnknownOpcode
		}
	}
	return nil
}

var _ transport.Stream = (*priorStream)(nil)
