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

// mustResolve builds an address for the given network, failing the test if
// the network is unknown.
func mustResolve(t *testing.T, network, addr string) net.Addr {
	t.Helper()
	a, err := transport.ResolveAddr(network, addr)
	if err != nil {
		t.Fatalf("ResolveAddr: %v", err)
	}
	return a
}

// runServer starts an rpc server over the given transports and registers
// cleanup that stops it and verifies a clean drain.
func runServer(t *testing.T, mux *rpc.Mux, addr net.Addr, trs ...transport.Transport) {
	t.Helper()
	srv, err := rpc.NewServer(rpc.ServerConfig{Mux: mux, Addrs: []net.Addr{addr}, Transports: trs})
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
func echo(ctx context.Context, c *rpc.Client, addr net.Addr, prefix, body string) (string, error) {
	stream, err := rpc.OpenStream(ctx, c, addr, opEcho, &echoHeader{Prefix: prefix})
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
	pipeTr := transport.NewPipeTransport()
	srvAddr := mustResolve(t, string(transport.NetworkPipe), "rpc-echo-server")
	runServer(t, echoMux(), srvAddr, pipeTr)

	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{transport.NewPipeTransport()},
	})
	addr, err := transport.ResolveAddr(string(transport.NetworkPipe), "rpc-echo-server")
	if err != nil {
		t.Fatalf("ResolveAddr: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got, err := echo(ctx, client, addr, "pre:", "body")
	if err != nil {
		t.Fatalf("echo: %v", err)
	}
	if got != "pre:body" {
		t.Fatalf("got %q, want %q", got, "pre:body")
	}

	// A second request reuses the cached connection.
	got, err = echo(ctx, client, addr, "again:", "more")
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
	addr := transport.NewQUICAddr(bind, "node-1")
	runServer(t, echoMux(), addr, server)

	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{
			transport.NewQUICTransport(transport.QUICTransportConfig{RootCAs: pool}),
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got, err := echo(ctx, client, addr, "q:", "uic")
	if err != nil {
		t.Fatalf("echo: %v", err)
	}
	if got != "q:uic" {
		t.Fatalf("got %q, want %q", got, "q:uic")
	}
}

func TestRPCUnknownOpcode(t *testing.T) {
	pipeTr := transport.NewPipeTransport()
	srvAddr := mustResolve(t, string(transport.NetworkPipe), "rpc-unknown-op")
	runServer(t, echoMux(), srvAddr, pipeTr)

	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{transport.NewPipeTransport()},
	})
	addr, err := transport.ResolveAddr(string(transport.NetworkPipe), "rpc-unknown-op")
	if err != nil {
		t.Fatalf("ResolveAddr: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := rpc.OpenStream(ctx, client, addr, rpc.Opcode(999), &echoHeader{})
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
	client := rpc.NewClient(rpc.ClientConfig{})
	addr, err := transport.ResolveAddr(string(transport.NetworkPipe), "nowhere")
	if err != nil {
		t.Fatalf("ResolveAddr: %v", err)
	}
	if _, err := rpc.OpenStream(context.Background(), client, addr, opEcho, &echoHeader{}); err == nil {
		t.Fatal("OpenStream without a matching transport succeeded")
	}

	// The client must not deadlock after the failed attempt.
	if _, err := rpc.OpenStream(context.Background(), client, addr, opEcho, &echoHeader{}); err == nil {
		t.Fatal("second OpenStream succeeded")
	}
}

func TestRPCConcurrentStreams(t *testing.T) {
	pipeTr := transport.NewPipeTransport()
	srvAddr := mustResolve(t, string(transport.NetworkPipe), "rpc-concurrent")
	runServer(t, echoMux(), srvAddr, pipeTr)

	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{transport.NewPipeTransport()},
	})
	addr, err := transport.ResolveAddr(string(transport.NetworkPipe), "rpc-concurrent")
	if err != nil {
		t.Fatalf("ResolveAddr: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const n = 16
	errCh := make(chan error, n)
	for i := range n {
		go func() {
			want := fmt.Sprintf("p%d:b%d", i, i)
			got, err := echo(ctx, client, addr, fmt.Sprintf("p%d:", i), fmt.Sprintf("b%d", i))
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
	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{transport.NewPipeTransport()},
	})
	pipeTr := transport.NewPipeTransport()
	srvAddr := mustResolve(t, string(transport.NetworkPipe), "rpc-big-header")
	runServer(t, echoMux(), srvAddr, pipeTr)
	addr, err := transport.ResolveAddr(string(transport.NetworkPipe), "rpc-big-header")
	if err != nil {
		t.Fatalf("ResolveAddr: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	big := &echoHeader{Prefix: string(make([]byte, 2*1024*1024))}
	if _, err := rpc.OpenStream(ctx, client, addr, opEcho, big); !errors.Is(err, rpc.ErrHeaderTooLarge) {
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

	// Each node answers with its own prefix, so a misrouted request is
	// visible in the response rather than merely absent.
	for _, node := range []string{"node-1", "node-2"} {
		mux := rpc.NewMux()
		rpc.RegisterHandler(mux, opEcho, func(_ context.Context, h echoHeader, stream transport.Stream) error {
			body, err := io.ReadAll(stream)
			if err != nil {
				return err
			}
			_, err = stream.Write([]byte(node + "/" + h.Prefix + string(body)))
			return err
		})
		runServer(t, mux, transport.NewQUICAddr(bind, node), server)
	}

	client := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{
			transport.NewQUICTransport(transport.QUICTransportConfig{RootCAs: pool}),
		},
	})
	t.Cleanup(func() { client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for _, node := range []string{"node-1", "node-2"} {
		got, err := echo(ctx, client, transport.NewQUICAddr(bind, node), "p:", "body")
		if err != nil {
			t.Fatalf("echo %s: %v", node, err)
		}
		if want := node + "/p:body"; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}
