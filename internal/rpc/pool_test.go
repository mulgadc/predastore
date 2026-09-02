package rpc

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/transport"
)

const opPing Opcode = 0x7f01

// pingHeader is the smallest header the mux can carry: a name the peer echoes
// back, encoded as its own bytes.
type pingHeader struct{ Name string }

var _ Header = (*pingHeader)(nil)

func (h *pingHeader) Append(buf []byte) ([]byte, error) { return append(buf, h.Name...), nil }
func (h *pingHeader) Unmarshal(b []byte) error          { h.Name = string(b); return nil }

// testNode is one node of a test cluster: its transport, the pool it dials
// peers from and the server answering the streams it is sent.
type testNode struct {
	id     config.NodeID
	tr     *transport.PipeTransport
	pool   *ConnPool
	client *Client
	done   chan error
}

// testResolver builds a route table by hand, so a test names its peers without
// a configuration behind them.
func testResolver(own *transport.PipeTransport, peers map[config.NodeID]*transport.PipeTransport) *Resolver {
	r := &Resolver{
		routes: make(map[config.NodeID]Route, len(peers)),
		nodes:  make(map[addrKey]config.NodeID, len(peers)),
	}
	for id, peer := range peers {
		r.routes[id] = Route{Transport: own, Addr: peer.Addr()}
		r.nodes[addrKeyOf(peer.Addr())] = id
	}
	return r
}

// newTestCluster wires one pipe-connected node per id, each running a server
// that answers opPing. Every node is torn down when the test ends.
func newTestCluster(t *testing.T, ids ...config.NodeID) map[config.NodeID]*testNode {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	trs := make(map[config.NodeID]*transport.PipeTransport, len(ids))
	for _, id := range ids {
		trs[id] = transport.NewPipeTransport(t.Name(), int(id))
	}

	nodes := make(map[config.NodeID]*testNode, len(ids))
	for _, id := range ids {
		peers := make(map[config.NodeID]*transport.PipeTransport, len(ids)-1)
		for _, peer := range ids {
			if peer != id {
				peers[peer] = trs[peer]
			}
		}

		ln, err := trs[id].Listen()
		if err != nil {
			t.Fatalf("listen for node %d: %v", id, err)
		}

		pool := NewConnPool(id, testResolver(trs[id], peers))
		srv, err := NewServer(pingMux(), []transport.Listener{ln}, pool, WithDrainTimeout(2*time.Second))
		if err != nil {
			t.Fatalf("server for node %d: %v", id, err)
		}

		n := &testNode{id: id, tr: trs[id], pool: pool, client: NewClient(pool), done: make(chan error, 1)}
		go func() { n.done <- srv.Run(ctx) }()
		nodes[id] = n
	}

	t.Cleanup(func() {
		cancel()
		for _, n := range nodes {
			select {
			case <-n.done:
			case <-time.After(10 * time.Second):
				t.Errorf("node %d did not stop", n.id)
			}
			_ = n.pool.Close()
			_ = n.tr.Close()
		}
	})
	return nodes
}

func pingMux() *Mux {
	mux := NewMux()
	RegisterHandler(mux, opPing, func(_ context.Context, h pingHeader, stream transport.Stream) error {
		_, err := stream.Write([]byte("pong:" + h.Name))
		return err
	})
	return mux
}

// ping runs one request round trip, aborting the read side when ctx expires
// the way a client with a deadline does.
func ping(ctx context.Context, from *testNode, to config.NodeID) (string, error) {
	stream, err := OpenStream(ctx, from.client, to, opPing, &pingHeader{Name: "hello"})
	if err != nil {
		return "", err
	}
	if err := stream.Close(); err != nil {
		return "", err
	}
	stop := context.AfterFunc(ctx, func() { stream.CancelRead(0) })
	defer stop()

	b, err := io.ReadAll(stream)
	return string(b), err
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestDonatedConnectionAnswersOutboundCalls is the direction regression: a node
// that adopts a connection its peer dialed must still get answers on it.
func TestDonatedConnectionAnswersOutboundCalls(t *testing.T) {
	nodes := newTestCluster(t, 1, 2)
	// The lower id is the preferred dialer, so node 2 keeps what node 1 dials.
	dialer, adopter := nodes[1], nodes[2]

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := dialer.pool.Dial(ctx, adopter.id); err != nil {
		t.Fatalf("dial node %d: %v", adopter.id, err)
	}
	waitFor(t, "the donated connection to be pooled", func() bool {
		c, _ := adopter.pool.held(dialer.id)
		return c != nil
	})

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()

	got, err := ping(callCtx, adopter, dialer.id)
	if err != nil {
		t.Fatalf("ping over the donated connection: %v", err)
	}
	if got != "pong:hello" {
		t.Fatalf("ping returned %q, want %q", got, "pong:hello")
	}
}

// TestServerAdoptsConnectionsDialedBeforeStart covers the startup window: a
// node dials a peer before its own server is running, so the dial's hook finds
// nothing to serve with. Starting the server must still pick that connection up.
func TestServerAdoptsConnectionsDialedBeforeStart(t *testing.T) {
	const dialer, adopter = config.NodeID(1), config.NodeID(2)
	trDialer := transport.NewPipeTransport(t.Name(), int(dialer))
	trAdopter := transport.NewPipeTransport(t.Name(), int(adopter))
	t.Cleanup(func() { _ = trDialer.Close(); _ = trAdopter.Close() })

	lnDialer, err := trDialer.Listen()
	if err != nil {
		t.Fatalf("listen for node %d: %v", dialer, err)
	}
	lnAdopter, err := trAdopter.Listen()
	if err != nil {
		t.Fatalf("listen for node %d: %v", adopter, err)
	}

	poolDialer := NewConnPool(dialer, testResolver(trDialer, map[config.NodeID]*transport.PipeTransport{adopter: trAdopter}))
	poolAdopter := NewConnPool(adopter, testResolver(trAdopter, map[config.NodeID]*transport.PipeTransport{dialer: trDialer}))
	t.Cleanup(func() { _ = poolDialer.Close(); _ = poolAdopter.Close() })

	srvDialer, err := NewServer(pingMux(), []transport.Listener{lnDialer}, poolDialer, WithDrainTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("server for node %d: %v", dialer, err)
	}
	srvAdopter, err := NewServer(pingMux(), []transport.Listener{lnAdopter}, poolAdopter, WithDrainTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("server for node %d: %v", adopter, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	adopterDone, dialerDone := make(chan error, 1), make(chan error, 1)
	t.Cleanup(func() {
		cancel()
		for _, done := range []chan error{adopterDone, dialerDone} {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("server did not stop")
			}
		}
	})

	// Only the adopter is running, so the dial below is accepted and donated
	// while the dialer's own server has no session to serve it with.
	go func() { adopterDone <- srvAdopter.Run(ctx) }()

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dialCancel()
	if _, err := poolDialer.Dial(dialCtx, adopter); err != nil {
		t.Fatalf("dial node %d: %v", adopter, err)
	}
	waitFor(t, "the donated connection to be pooled", func() bool {
		c, _ := poolAdopter.held(dialer)
		return c != nil
	})

	go func() { dialerDone <- srvDialer.Run(ctx) }()

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()

	caller := &testNode{id: adopter, tr: trAdopter, pool: poolAdopter, client: NewClient(poolAdopter)}
	got, err := ping(callCtx, caller, dialer)
	if err != nil {
		t.Fatalf("ping over the connection dialed before start: %v", err)
	}
	if got != "pong:hello" {
		t.Fatalf("ping returned %q, want %q", got, "pong:hello")
	}
}

// TestEmptySlotFollowsTiebreak covers the slot the tiebreak used to skip: an
// empty one adopted whatever arrived, in either direction.
func TestEmptySlotFollowsTiebreak(t *testing.T) {
	const low, high = config.NodeID(1), config.NodeID(2)
	trLow := transport.NewPipeTransport(t.Name(), int(low))
	trHigh := transport.NewPipeTransport(t.Name(), int(high))
	t.Cleanup(func() { _ = trLow.Close(); _ = trHigh.Close() })

	lnLow, err := trLow.Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	accepted := make(chan transport.Conn, 4)
	go func() {
		for {
			c, err := lnLow.Accept(ctx)
			if err != nil {
				return
			}
			accepted <- c
		}
	}()

	poolLow := NewConnPool(low, testResolver(trLow, map[config.NodeID]*transport.PipeTransport{high: trHigh}))
	poolHigh := NewConnPool(high, testResolver(trHigh, map[config.NodeID]*transport.PipeTransport{low: trLow}))
	t.Cleanup(func() { _ = poolLow.Close(); _ = poolHigh.Close() })

	// The lower id prefers to dial, so it must not adopt what the higher id
	// dialed even with nothing in the slot.
	if _, err := trHigh.Dial(ctx, trLow.Addr()); err != nil {
		t.Fatalf("dial: %v", err)
	}
	if poolLow.Donate(<-accepted) {
		t.Fatal("node 1 adopted a connection it prefers to dial itself")
	}
	if c, _ := poolLow.held(high); c != nil {
		t.Fatal("node 1 pooled a connection it prefers to dial itself")
	}

	// Its own dial is kept whichever way the tiebreak points: refusing that
	// would leave the higher id with no way to open a connection at all.
	conn, err := poolHigh.Dial(ctx, low)
	if err != nil {
		t.Fatalf("dial node %d: %v", low, err)
	}
	<-accepted
	if c, _ := poolHigh.held(low); c != conn {
		t.Fatal("node 2 dropped the connection it dialed")
	}
}

// TestUnopenableStreamsEvictConnection covers the case the pool could not see:
// a connection alive at the transport that cannot open a stream at all. No
// response is ever read over it, so no stall was recorded and it was reused
// forever, which is how a raft peer stayed unreachable for hours.
func TestUnopenableStreamsEvictConnection(t *testing.T) {
	const peerID config.NodeID = 2
	callerTr := transport.NewPipeTransport(t.Name(), 1)
	peerTr := transport.NewPipeTransport(t.Name(), int(peerID))
	t.Cleanup(func() { _ = callerTr.Close(); _ = peerTr.Close() })

	ln, err := peerTr.Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx := t.Context()

	// The peer completes the dial and then does nothing with the connection. It
	// never accepts a stream, which is what a wedged replica presents to peers:
	// a healthy socket that answers no request opened on it.
	accepted := make(chan transport.Conn, 1)
	go func() {
		if conn, err := ln.Accept(ctx); err == nil {
			accepted <- conn
		}
	}()

	pool := NewConnPool(1, testResolver(callerTr, map[config.NodeID]*transport.PipeTransport{peerID: peerTr}))
	t.Cleanup(func() { _ = pool.Close() })
	client := NewClient(pool)

	conn, err := pool.Dial(ctx, peerID)
	if err != nil {
		t.Fatalf("dial node %d: %v", peerID, err)
	}
	<-accepted

	for i := range maxStreamStalls {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, 50*time.Millisecond)
		_, err := OpenStream(attemptCtx, client, peerID, opPing, &pingHeader{Name: "hello"})
		attemptCancel()
		if err == nil {
			t.Fatal("opened a stream on a peer that never accepts one")
		}
		held, _ := pool.held(peerID)
		if i < maxStreamStalls-1 && held != conn {
			t.Fatalf("connection evicted after %d failed opens, want %d", i+1, maxStreamStalls)
		}
	}
	if held, _ := pool.held(peerID); held == conn {
		t.Fatalf("connection still pooled after %d failed stream opens", maxStreamStalls)
	}
}

// TestStalledStreamsEvictConnection covers the escape hatch: a connection alive
// at the transport but answering nothing is dropped so a dial can replace it.
func TestStalledStreamsEvictConnection(t *testing.T) {
	nodes := newTestCluster(t, 1, 2)
	caller, peer := nodes[1], nodes[2]

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := caller.pool.Dial(ctx, peer.id)
	if err != nil {
		t.Fatalf("dial node %d: %v", peer.id, err)
	}

	stall := func() {
		t.Helper()
		stream, err := OpenStream(ctx, caller.client, peer.id, opPing, &pingHeader{Name: "hello"})
		if err != nil {
			t.Fatalf("open stream: %v", err)
		}
		// The idle guard aborting the read is what a peer that has stopped
		// making progress looks like, so the stream ends without a byte of
		// response and the connection is charged for it.
		stream.CancelRead(transport.StreamCodeIdle)
		if _, err := io.ReadAll(stream); err == nil {
			t.Fatal("read from an aborted stream succeeded")
		}
	}

	// A round trip that gets an answer clears the run of stalls before it.
	for range maxStreamStalls - 1 {
		stall()
	}
	if got, err := ping(ctx, caller, peer.id); err != nil || got != "pong:hello" {
		t.Fatalf("ping returned %q, %v", got, err)
	}

	for i := range maxStreamStalls {
		stall()
		held, _ := caller.pool.held(peer.id)
		if i < maxStreamStalls-1 && held != conn {
			t.Fatalf("connection evicted after %d stalls, want %d", i+1, maxStreamStalls)
		}
	}
	if held, _ := caller.pool.held(peer.id); held == conn {
		t.Fatalf("connection still pooled after %d stalls", maxStreamStalls)
	}

	// The pool must be usable again: the next call dials a replacement.
	if got, err := ping(ctx, caller, peer.id); err != nil || got != "pong:hello" {
		t.Fatalf("ping after eviction returned %q, %v", got, err)
	}
}

// TestAbandonedStreamsDoNotEvictConnection is the other half of the stall rule.
// A caller abandons streams routinely — an open deadline expires, a hedged
// shard is dropped once a faster copy lands — and each one ends in a failed
// read. Charging those to the peer evicts connections that are serving
// perfectly, and the colder replacement is abandoned the same way, so the
// eviction repeats for as long as the caller keeps hedging.
func TestAbandonedStreamsDoNotEvictConnection(t *testing.T) {
	nodes := newTestCluster(t, 1, 2)
	caller, peer := nodes[1], nodes[2]

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := caller.pool.Dial(ctx, peer.id)
	if err != nil {
		t.Fatalf("dial node %d: %v", peer.id, err)
	}

	abandon := func() {
		t.Helper()
		stream, err := OpenStream(ctx, caller.client, peer.id, opPing, &pingHeader{Name: "hello"})
		if err != nil {
			t.Fatalf("open stream: %v", err)
		}
		stream.CancelRead(transport.StreamCodeCallerGone)
		if _, err := io.ReadAll(stream); err == nil {
			t.Fatal("read from an abandoned stream succeeded")
		}
	}

	// Well past maxStreamStalls, so a connection charged for these would be
	// long gone by the time the loop ends.
	for range maxStreamStalls * 3 {
		abandon()
	}
	if held, _ := caller.pool.held(peer.id); held != conn {
		t.Fatalf("connection evicted after %d abandoned streams", maxStreamStalls*3)
	}

	// And the connection is still the one being used, not merely still listed.
	if got, err := ping(ctx, caller, peer.id); err != nil || got != "pong:hello" {
		t.Fatalf("ping over the retained connection returned %q, %v", got, err)
	}
}
