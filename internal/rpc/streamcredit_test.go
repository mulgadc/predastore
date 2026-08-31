package rpc

import (
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
