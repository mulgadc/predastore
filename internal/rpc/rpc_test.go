package rpc_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/mulgadc/predastore/internal/testport"
	"github.com/mulgadc/predastore/internal/transport"
)

const (
	opEcho rpc.Opcode = 1
)

// nodeTopo stands in for the real topology: a fixed map from node id to the
// one address that node answers on. Tests pick the addresses so they can mix
// pipe and QUIC endpoints without a cluster config.
type nodeTopo map[int]net.Addr

func (t nodeTopo) NodeAddr(nodeID int) (net.Addr, error) {
	addr, ok := t[nodeID]
	if !ok {
		return nil, fmt.Errorf("unknown node %d", nodeID)
	}
	return addr, nil
}

func (t nodeTopo) ListenAddrs(nodeID int) ([]net.Addr, error) {
	addr, err := t.NodeAddr(nodeID)
	if err != nil {
		return nil, err
	}
	return []net.Addr{addr}, nil
}

// echoHeader is a minimal Header implementation: the payload is the prefix
// string itself.
type echoHeader struct {
	Prefix string
}

func (h *echoHeader) Append(buf []byte) ([]byte, error) {
	return append(buf, h.Prefix...), nil
}

func (h *echoHeader) Unmarshal(b []byte) error {
	h.Prefix = string(b)
	return nil
}

// echoMux replies with the header prefix concatenated with the request body.
func echoMux() *rpc.Mux {
	mux := rpc.NewMux()
	rpc.RegisterHandler(mux, opEcho, func(ctx context.Context, h echoHeader, stream transport.Stream) error {
		body, err := io.ReadAll(stream)
		if err != nil {
			return fmt.Errorf("read body: %w", err)
		}
		if _, err := stream.Write(append([]byte(h.Prefix), body...)); err != nil {
			return fmt.Errorf("write response: %w", err)
		}
		return nil
	})
	return mux
}

// pipeTopo maps one node id to a named pipe endpoint.
func pipeTopo(t *testing.T, nodeID int, name string) nodeTopo {
	t.Helper()
	addr, err := transport.ResolveAddr(string(transport.NetworkPipe), name)
	if err != nil {
		t.Fatalf("ResolveAddr: %v", err)
	}
	return nodeTopo{nodeID: addr}
}

// runServer starts an rpc server for one node over the given transports and
// registers cleanup that stops it and verifies a clean drain.
func runServer(t *testing.T, mux *rpc.Mux, nodeID int, topo rpc.Topology, trs ...transport.Transport) {
	t.Helper()
	srv, err := rpc.NewServer(rpc.ServerConfig{
		Mux:        mux,
		NodeID:     nodeID,
		Topology:   topo,
		Transports: trs,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("server Run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("server did not stop")
		}
	})
}

// echo performs one full request round trip on a fresh stream.
func echo(ctx context.Context, c *rpc.Client, nodeID int, prefix, body string) (string, error) {
	stream, err := rpc.OpenStream(ctx, c, nodeID, opEcho, &echoHeader{Prefix: prefix})
	if err != nil {
		return "", err
	}
	if _, err := stream.Write([]byte(body)); err != nil {
		return "", fmt.Errorf("write body: %w", err)
	}
	// Half-close: the server's body read completes on EOF while the
	// response direction stays open.
	if err := stream.Close(); err != nil {
		return "", fmt.Errorf("close write side: %w", err)
	}
	resp, err := io.ReadAll(stream)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	return string(resp), nil
}

func TestRPCEchoOverPipe(t *testing.T) {
	topo := pipeTopo(t, 1, "rpc-echo-server")
	runServer(t, echoMux(), 1, topo, transport.NewPipeTransport())

	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{transport.NewPipeTransport()},
		Topology:   topo,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got, err := echo(ctx, client, 1, "pre:", "body")
	if err != nil {
		t.Fatalf("echo: %v", err)
	}
	if got != "pre:body" {
		t.Fatalf("got %q, want %q", got, "pre:body")
	}

	// A second request reuses the cached connection.
	got, err = echo(ctx, client, 1, "again:", "more")
	if err != nil {
		t.Fatalf("echo reuse: %v", err)
	}
	if got != "again:more" {
		t.Fatalf("got %q, want %q", got, "again:more")
	}
}

func TestRPCEchoOverQUIC(t *testing.T) {
	certPath, keyPath, pool := testcerts.Generate(t)
	bind := fmt.Sprintf("127.0.0.1:%d", testport.Block(t, 1))
	server := transport.NewQUICTransport(transport.QUICTransportConfig{
		TLSCert: certPath,
		TLSKey:  keyPath,
	})
	topo := nodeTopo{1: transport.NewQUICAddr(bind, "node-1")}
	runServer(t, echoMux(), 1, topo, server)

	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{
			transport.NewQUICTransport(transport.QUICTransportConfig{RootCAs: pool}),
		},
		Topology: topo,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got, err := echo(ctx, client, 1, "q:", "uic")
	if err != nil {
		t.Fatalf("echo: %v", err)
	}
	if got != "q:uic" {
		t.Fatalf("got %q, want %q", got, "q:uic")
	}
}

func TestRPCUnknownOpcode(t *testing.T) {
	topo := pipeTopo(t, 1, "rpc-unknown-op")
	runServer(t, echoMux(), 1, topo, transport.NewPipeTransport())

	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{transport.NewPipeTransport()},
		Topology:   topo,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := rpc.OpenStream(ctx, client, 1, rpc.Opcode(999), &echoHeader{})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	stream.Close()
	// The server has no handler for the opcode and aborts the stream; the
	// response read must fail rather than hang or return data.
	if _, err := io.ReadAll(stream); err == nil {
		t.Fatal("read from aborted stream succeeded")
	}
}

func TestRPCNoTransportForNetwork(t *testing.T) {
	client := rpc.NewClient(rpc.ClientConfig{Topology: pipeTopo(t, 1, "nowhere")})
	if _, err := rpc.OpenStream(context.Background(), client, 1, opEcho, &echoHeader{}); err == nil {
		t.Fatal("OpenStream without a matching transport succeeded")
	}

	// The client must not deadlock after the failed attempt.
	if _, err := rpc.OpenStream(context.Background(), client, 1, opEcho, &echoHeader{}); err == nil {
		t.Fatal("second OpenStream succeeded")
	}
}

// TestRPCUnaddressableNode covers the two ways a node id fails to become an
// address: no topology at all, and a topology that does not know the node.
func TestRPCUnaddressableNode(t *testing.T) {
	trs := []transport.Transport{transport.NewPipeTransport()}

	noTopo := rpc.NewClient(rpc.ClientConfig{Transports: trs})
	if _, err := rpc.OpenStream(context.Background(), noTopo, 1, opEcho, &echoHeader{}); err == nil {
		t.Fatal("OpenStream on a client with no topology succeeded")
	}

	client := rpc.NewClient(rpc.ClientConfig{
		Transports: trs,
		Topology:   pipeTopo(t, 1, "rpc-unaddressable"),
	})
	if _, err := rpc.OpenStream(context.Background(), client, 2, opEcho, &echoHeader{}); err == nil {
		t.Fatal("OpenStream to a node outside the topology succeeded")
	}

	// A server cannot bind a node the topology does not place either.
	if _, err := rpc.NewServer(rpc.ServerConfig{Mux: echoMux(), NodeID: 2, Transports: trs}); err == nil {
		t.Fatal("NewServer with no topology succeeded")
	}
}

func TestRPCConcurrentStreams(t *testing.T) {
	topo := pipeTopo(t, 1, "rpc-concurrent")
	runServer(t, echoMux(), 1, topo, transport.NewPipeTransport())

	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{transport.NewPipeTransport()},
		Topology:   topo,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const n = 16
	errCh := make(chan error, n)
	for i := range n {
		go func() {
			want := fmt.Sprintf("p%d:b%d", i, i)
			got, err := echo(ctx, client, 1, fmt.Sprintf("p%d:", i), fmt.Sprintf("b%d", i))
			if err == nil && got != want {
				err = fmt.Errorf("got %q, want %q", got, want)
			}
			errCh <- err
		}()
	}
	for range n {
		if err := <-errCh; err != nil {
			t.Errorf("concurrent echo: %v", err)
		}
	}
}

func TestRPCHeaderTooLarge(t *testing.T) {
	topo := pipeTopo(t, 1, "rpc-big-header")
	runServer(t, echoMux(), 1, topo, transport.NewPipeTransport())

	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{transport.NewPipeTransport()},
		Topology:   topo,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	big := &echoHeader{Prefix: string(make([]byte, 2*1024*1024))}
	if _, err := rpc.OpenStream(ctx, client, 1, opEcho, big); !errors.Is(err, rpc.ErrHeaderTooLarge) {
		t.Fatalf("got %v, want ErrHeaderTooLarge", err)
	}
}

// TestRPCNodesShareOneSocket is the property the per-node server design rests
// on: several servers listen on one process socket and each only ever sees the
// requests addressed to its own node.
func TestRPCNodesShareOneSocket(t *testing.T) {
	certPath, keyPath, pool := testcerts.Generate(t)
	bind := fmt.Sprintf("127.0.0.1:%d", testport.Block(t, 1))
	server := transport.NewQUICTransport(transport.QUICTransportConfig{
		TLSCert: certPath,
		TLSKey:  keyPath,
	})
	t.Cleanup(func() { server.Close() })

	// Both nodes share the socket and differ only by their address's node key.
	topo := nodeTopo{
		1: transport.NewQUICAddr(bind, "node-1"),
		2: transport.NewQUICAddr(bind, "node-2"),
	}

	// Each node answers with its own id, so a misrouted request is visible in
	// the response rather than merely absent.
	for _, id := range []int{1, 2} {
		mux := rpc.NewMux()
		rpc.RegisterHandler(mux, opEcho, func(_ context.Context, h echoHeader, stream transport.Stream) error {
			body, err := io.ReadAll(stream)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(stream, "node-%d/%s%s", id, h.Prefix, body)
			return err
		})
		runServer(t, mux, id, topo, server)
	}

	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{
			transport.NewQUICTransport(transport.QUICTransportConfig{RootCAs: pool}),
		},
		Topology: topo,
	})
	t.Cleanup(func() { client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for _, id := range []int{1, 2} {
		got, err := echo(ctx, client, id, "p:", "body")
		if err != nil {
			t.Fatalf("echo node-%d: %v", id, err)
		}
		if want := fmt.Sprintf("node-%d/p:body", id); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}
