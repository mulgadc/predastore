package quicclient

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestClientActiveStreams(t *testing.T) {
	c := &Client{}
	if got := c.ActiveStreams(); got != 0 {
		t.Fatalf("initial ActiveStreams: got %d, want 0", got)
	}
	c.incActive()
	c.incActive()
	if got := c.ActiveStreams(); got != 2 {
		t.Fatalf("after 2 incs: got %d, want 2", got)
	}
	c.decActive()
	if got := c.ActiveStreams(); got != 1 {
		t.Fatalf("after dec: got %d, want 1", got)
	}
	c.decActive()
	if got := c.ActiveStreams(); got != 0 {
		t.Fatalf("after balanced inc/dec: got %d, want 0", got)
	}
}

func TestStreamReadCloserCloseFiresOnCloseOnce(t *testing.T) {
	var calls atomic.Int32
	rc := &streamReadCloser{
		onClose: func() { calls.Add(1) },
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("onClose calls: got %d, want 1", got)
	}
}

func TestStreamReadCloserCloseWithoutOnCloseIsSafe(t *testing.T) {
	rc := &streamReadCloser{}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close without onClose: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("second Close without onClose: %v", err)
	}
}

// Pool.cleanup removes connections whose underlying quic.Conn is nil/closed.
// The "busy guard skips stale connections with activeStreams > 0" path
// requires a live quic.Conn and is exercised by backend/distributed tests.
func TestPoolCleanupReapsClosed(t *testing.T) {
	p := &Pool{connections: map[string]*pooledConn{}}
	p.connections["stale"] = &pooledConn{
		client:   &Client{}, // conn == nil ⇒ isClosed true
		lastUsed: time.Now().Add(-5 * time.Minute),
	}
	p.cleanup()
	if _, stillThere := p.connections["stale"]; stillThere {
		t.Fatalf("cleanup did not evict closed connection")
	}
}
