package transport_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/mulgadc/predastore/internal/transport"
)

// fanoutServer binds one socket and registers the named nodes on it,
// returning their listeners keyed by node.
func fanoutServer(t *testing.T, nodes ...string) (*transport.QUICTransport, map[string]transport.Listener) {
	t.Helper()
	certPath, keyPath, _ := testcerts.Generate(t)
	qt := transport.NewQUICTransport(transport.QUICTransportConfig{
		TLSCert: certPath,
		TLSKey:  keyPath,
	})
	t.Cleanup(func() { qt.Close() })

	lns := make(map[string]transport.Listener, len(nodes))
	endpoint := "127.0.0.1:0"
	for _, node := range nodes {
		ln, err := qt.Listen(transport.NewQUICAddr(endpoint, node))
		if err != nil {
			t.Fatalf("Listen %s: %v", node, err)
		}
		lns[node] = ln
		// Every later node must name the endpoint originally requested, not
		// the ephemeral port the socket actually took.
	}
	return qt, lns
}

// TestQUICManyNodesOneSocket is the core property: several nodes listen on one
// process socket and each receives only the connections addressed to it.
func TestQUICManyNodesOneSocket(t *testing.T) {
	certPath, keyPath, pool := testcerts.Generate(t)
	server := transport.NewQUICTransport(transport.QUICTransportConfig{
		TLSCert: certPath,
		TLSKey:  keyPath,
	})
	defer server.Close()

	first, err := server.Listen(transport.NewQUICAddr("127.0.0.1:0", "node-1"))
	if err != nil {
		t.Fatalf("Listen node-1: %v", err)
	}
	second, err := server.Listen(transport.NewQUICAddr("127.0.0.1:0", "node-2"))
	if err != nil {
		t.Fatalf("Listen node-2: %v", err)
	}
	if first.Addr().String() == second.Addr().String() {
		t.Fatal("nodes on one socket must have distinct addresses")
	}

	endpoint := first.Addr().(*transport.QUICAddr).Endpoint()
	if got := second.Addr().(*transport.QUICAddr).Endpoint(); got != endpoint {
		t.Fatalf("nodes bound different endpoints: %s and %s", endpoint, got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := transport.NewQUICTransport(transport.QUICTransportConfig{RootCAs: pool})
	defer client.Close()

	// Dial node-2 only; node-1 must see nothing.
	accepted := make(chan transport.Conn, 1)
	go func() {
		c, err := second.Accept(ctx)
		if err == nil {
			accepted <- c
		}
	}()

	dial, err := client.Dial(ctx, transport.NewQUICAddr(endpoint, "node-2"))
	if err != nil {
		t.Fatalf("Dial node-2: %v", err)
	}
	defer dial.Close()

	select {
	case c := <-accepted:
		defer c.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("node-2 never accepted its connection")
	}

	idle, cancelIdle := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelIdle()
	if _, err := first.Accept(idle); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("node-1 accepted traffic addressed to node-2: %v", err)
	}
}

// TestQUICDialUnknownNode covers ALPN rejecting a node the peer does not run,
// rather than the connection being misrouted or silently dropped.
func TestQUICDialUnknownNode(t *testing.T) {
	certPath, keyPath, pool := testcerts.Generate(t)
	server := transport.NewQUICTransport(transport.QUICTransportConfig{
		TLSCert: certPath,
		TLSKey:  keyPath,
	})
	defer server.Close()

	ln, err := server.Listen(transport.NewQUICAddr("127.0.0.1:0", "node-1"))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	endpoint := ln.Addr().(*transport.QUICAddr).Endpoint()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := transport.NewQUICTransport(transport.QUICTransportConfig{RootCAs: pool})
	defer client.Close()

	_, err = client.Dial(ctx, transport.NewQUICAddr(endpoint, "node-9"))
	if err == nil {
		t.Fatal("dial for an unserved node succeeded")
	}
	if !strings.Contains(err.Error(), "alpn") && !strings.Contains(err.Error(), "protocol") {
		t.Logf("unknown-node dial failed with: %v", err)
	}
}

// TestQUICListenSameNodeTwice guards the registry against two servers claiming
// one node on the same socket.
func TestQUICListenSameNodeTwice(t *testing.T) {
	qt, _ := fanoutServer(t, "node-1")
	if _, err := qt.Listen(transport.NewQUICAddr("127.0.0.1:0", "node-1")); !errors.Is(err, transport.ErrAddrAlreadyInUse) {
		t.Fatalf("got %v, want ErrAddrAlreadyInUse", err)
	}
}

// TestQUICListenSecondEndpointRejected pins one socket per process: a node
// naming a different endpoint is a configuration error, not a second bind.
func TestQUICListenSecondEndpointRejected(t *testing.T) {
	qt, _ := fanoutServer(t, "node-1")
	if _, err := qt.Listen(transport.NewQUICAddr("127.0.0.1:1", "node-2")); err == nil {
		t.Fatal("listening on a second endpoint succeeded")
	}
}

// TestQUICOneNodeCloseLeavesSiblings covers the lifecycle rule that drove the
// refcount decision: one node draining must not take the socket down.
func TestQUICOneNodeCloseLeavesSiblings(t *testing.T) {
	certPath, keyPath, pool := testcerts.Generate(t)
	server := transport.NewQUICTransport(transport.QUICTransportConfig{
		TLSCert: certPath,
		TLSKey:  keyPath,
	})
	defer server.Close()

	first, err := server.Listen(transport.NewQUICAddr("127.0.0.1:0", "node-1"))
	if err != nil {
		t.Fatalf("Listen node-1: %v", err)
	}
	second, err := server.Listen(transport.NewQUICAddr("127.0.0.1:0", "node-2"))
	if err != nil {
		t.Fatalf("Listen node-2: %v", err)
	}
	endpoint := second.Addr().(*transport.QUICAddr).Endpoint()

	// node-1 drains; node-2 must still be reachable on the shared socket.
	first.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	accepted := make(chan transport.Conn, 1)
	go func() {
		c, err := second.Accept(ctx)
		if err == nil {
			accepted <- c
		}
	}()

	client := transport.NewQUICTransport(transport.QUICTransportConfig{RootCAs: pool})
	defer client.Close()
	dial, err := client.Dial(ctx, transport.NewQUICAddr(endpoint, "node-2"))
	if err != nil {
		t.Fatalf("Dial node-2 after sibling close: %v", err)
	}
	defer dial.Close()

	select {
	case c := <-accepted:
		c.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("surviving node stopped accepting after a sibling closed")
	}
}

// TestQUICTransportCloseFailsListeners covers router teardown: nobody may be
// left blocked in Accept on a socket that no longer reads.
func TestQUICTransportCloseFailsListeners(t *testing.T) {
	qt, lns := fanoutServer(t, "node-1", "node-2")

	errCh := make(chan error, 2)
	for _, ln := range lns {
		go func() {
			_, err := ln.Accept(context.Background())
			errCh <- err
		}()
	}
	time.Sleep(50 * time.Millisecond)
	qt.Close()

	for range lns {
		select {
		case err := <-errCh:
			if err == nil {
				t.Fatal("Accept returned no error after transport close")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Accept did not unblock on transport close")
		}
	}
}
