package rpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/mulgadc/predastore/internal/transport"
)

// quicClient stands up a server on an ephemeral port and returns a client that
// dials it as node id 2. The pipe transport the other tests use has no stream
// accounting, so only quic can show a peer running out of credit.
func quicClient(t *testing.T, mux *Mux) *Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	certPath, keyPath, roots := testcerts.Generate(t)

	serverTr, err := transport.NewQUICTransport("127.0.0.1", 0, certPath, keyPath)
	if err != nil {
		t.Fatalf("server transport: %v", err)
	}
	t.Cleanup(func() { serverTr.Close() })

	ln, err := serverTr.Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv, err := NewServer(mux, []transport.Listener{ln}, nil, WithDrainTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("server did not stop")
		}
	})

	clientTr, err := transport.NewQUICTransport("127.0.0.1", 0, certPath, keyPath, transport.WithRootCAs(roots))
	if err != nil {
		t.Fatalf("client transport: %v", err)
	}
	t.Cleanup(func() { clientTr.Close() })

	pool := NewConnPool(1, &Resolver{
		routes: map[config.NodeID]Route{2: {Transport: clientTr, Addr: ln.Addr()}},
		nodes:  map[addrKey]config.NodeID{addrKeyOf(ln.Addr()): 2},
	})
	t.Cleanup(func() { pool.Close() })

	return NewClient(pool)
}

// pipeClient is the same wiring over the in-process transport, where streams
// are not metered but a destructive teardown is still visible to the peer.
func pipeClient(t *testing.T, mux *Mux) *Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	serverTr := transport.NewPipeTransport(t.Name()+"-server", 2)
	t.Cleanup(func() { serverTr.Close() })
	ln, err := serverTr.Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv, err := NewServer(mux, []transport.Listener{ln}, nil, WithDrainTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("server did not stop")
		}
	})

	clientTr := transport.NewPipeTransport(t.Name()+"-client", 1)
	t.Cleanup(func() { clientTr.Close() })

	pool := NewConnPool(1, &Resolver{
		routes: map[config.NodeID]Route{2: {Transport: clientTr, Addr: ln.Addr()}},
		nodes:  map[addrKey]config.NodeID{addrKeyOf(ln.Addr()): 2},
	})
	t.Cleanup(func() { pool.Close() })

	return NewClient(pool)
}

// callPing runs one header-only round trip, half-closing only once the answer
// is in. A client that half-closes straight after the header usually gets both
// into one packet, which hides the race this exercises by arriving together.
func callPing(ctx context.Context, c *Client, name string) (string, error) {
	stream, err := OpenStream(ctx, c, 2, opPing, &pingHeader{Name: name})
	if err != nil {
		return "", fmt.Errorf("open: %w", err)
	}
	stop := context.AfterFunc(ctx, func() { stream.CancelRead(0) })
	defer stop()

	b, err := io.ReadAll(stream)
	if err != nil {
		return "", fmt.Errorf("read: %w", err)
	}
	if err := stream.Close(); err != nil {
		return "", fmt.Errorf("half-close: %w", err)
	}
	return string(b), nil
}

// TestServedResponseSurvivesTheDrain is the regression for retiring a stream
// destructively. A blob get half-closes before reading and then reads a body
// far larger than one packet, so anything the server does to its own read side
// after answering lands on a client that is still consuming the answer. The
// first attempt cancelled the read when the drain errored, which the pipe
// transport surfaces to the peer as `stream error: code 0` and which took every
// shard read on dev-prod with it.
func TestServedResponseSurvivesTheDrain(t *testing.T) {
	const opBody Opcode = 0x7f02
	body := make([]byte, 4<<20)
	for i := range body {
		body[i] = byte(i)
	}

	mux := NewMux()
	RegisterHandler(mux, opBody, func(_ context.Context, h pingHeader, stream transport.Stream) error {
		_, err := stream.Write(body)
		return err
	})

	// Both transports: the credit accounting is quic's, but the damage was the
	// pipe's, and only running each proves the drain is safe on both.
	for _, tc := range []struct {
		name   string
		client *Client
	}{
		{"quic", quicClient(t, mux)},
		{"pipe", pipeClient(t, mux)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i := range 20 {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				stream, err := OpenStream(ctx, tc.client, 2, opBody, &pingHeader{Name: "x"})
				if err != nil {
					cancel()
					t.Fatalf("round %d: open: %v", i, err)
				}
				if err := stream.Close(); err != nil {
					cancel()
					t.Fatalf("round %d: half-close: %v", i, err)
				}
				got, err := io.ReadAll(stream)
				cancel()
				if err != nil {
					t.Fatalf("round %d: read after %d of %d bytes: %v", i, len(got), len(body), err)
				}
				if !bytes.Equal(got, body) {
					t.Fatalf("round %d: got %d bytes, want %d", i, len(got), len(body))
				}
			}
		})
	}
}

// TestLongLivedStreamIsNotTornDownByTheDrain covers the shape the raft dial
// takes: the handler owns the stream for the connection's lifetime and neither
// end half-closes. Draining that stream blocks until the bound expires and then
// cancels a stream that is still in use, which on dev-prod surfaced as blob
// nodes evicting their own in-process connections as unresponsive.
func TestLongLivedStreamIsNotTornDownByTheDrain(t *testing.T) {
	const opHold Opcode = 0x7f03
	served := make(chan transport.Stream, 1)

	// The bound is what is under test, not its production length: a drain that
	// cancels does so the moment it expires, so a short one exercises the same
	// path.
	restore := requestDrainTimeout
	requestDrainTimeout = 250 * time.Millisecond
	t.Cleanup(func() { requestDrainTimeout = restore })

	mux := NewMux()
	RegisterHandler(mux, opHold, func(_ context.Context, h pingHeader, stream transport.Stream) error {
		// Answer, then hand the stream on and return, exactly as handleRaftDial
		// does once raft has taken the connection.
		if _, err := stream.Write([]byte("held")); err != nil {
			return err
		}
		served <- stream
		return nil
	})

	client := pipeClient(t, mux)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := OpenStream(ctx, client, 2, opHold, &pingHeader{Name: "x"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(stream, buf); err != nil {
		t.Fatalf("read greeting: %v", err)
	}

	var held transport.Stream
	select {
	case held = <-served:
	case <-ctx.Done():
		t.Fatal("handler never ran")
	}
	t.Cleanup(func() {
		held.CancelRead(0)
		held.CancelWrite(0)
		stream.CancelRead(0)
		stream.CancelWrite(0)
	})

	// Well past requestDrainTimeout: neither end has half-closed, so a drain
	// that cancels on expiry destroys this stream.
	time.Sleep(requestDrainTimeout + 750*time.Millisecond)

	// A pipe is synchronous and nothing is reading the held stream, so the write
	// blocking is the healthy outcome. What must not happen is a stream error,
	// which is a torn-down stream answering instantly.
	wrote := make(chan error, 1)
	go func() {
		_, err := stream.Write([]byte("still here"))
		wrote <- err
	}()
	select {
	case err := <-wrote:
		t.Fatalf("writing to a stream the handler still owns: %v", err)
	case <-time.After(750 * time.Millisecond):
	}
}

// TestServerReturnsStreamCreditToPeer holds the server to answering more
// requests on one connection than quic will let the peer have open at once.
//
// A handler is given the stream after its header has been read and most read
// nothing further. Quic reports the FIN alongside the last bytes, so that read
// completes the receive side when the half-close has already arrived and
// silently does not when it has not. An incomplete stream is never retired and
// never raises the peer's stream limit, so the budget drains one request at a
// time until OpenStream blocks and stays blocked: a collapse no retry recovers
// from, because nothing frees a slot.
func TestServerReturnsStreamCreditToPeer(t *testing.T) {
	client := quicClient(t, pingMux())

	// Comfortably past the 1000-stream limit, so a connection that never gets
	// credit back stalls well before the end.
	const calls = 2500
	for i := range calls {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		got, err := callPing(ctx, client, "hello")
		cancel()
		if err != nil {
			t.Fatalf("call %d of %d: %v", i+1, calls, err)
		}
		if got != "pong:hello" {
			t.Fatalf("call %d: got %q, want %q", i+1, got, "pong:hello")
		}
	}
}
