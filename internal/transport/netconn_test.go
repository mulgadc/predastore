package transport

import (
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

// connPair returns two StreamConns wired back to back over a pipe stream.
func connPair(t *testing.T) (a, b net.Conn) {
	t.Helper()
	as, bs := newPipeStreamPair(NewAddr(NetworkPipe, "nc-a"), NewAddr(NetworkPipe, "nc-b"))
	ac, bc := NewStreamConn(as), NewStreamConn(bs)
	t.Cleanup(func() {
		ac.Close()
		bc.Close()
	})
	return ac, bc
}

func TestStreamConnRoundTrip(t *testing.T) {
	a, b := connPair(t)

	got := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 16)
		n, _ := b.Read(buf)
		got <- buf[:n]
	}()
	if _, err := a.Write([]byte("ping")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if string(<-got) != "ping" {
		t.Fatal("payload mismatch")
	}
}

func TestStreamConnReadDeadline(t *testing.T) {
	a, _ := connPair(t)

	if err := a.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	start := time.Now()
	_, err := a.Read(make([]byte, 1))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("got %v, want os.ErrDeadlineExceeded", err)
	}
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("error %v does not report Timeout", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("read blocked past deadline")
	}
}

func TestStreamConnWriteDeadline(t *testing.T) {
	a, _ := connPair(t)

	// Nobody reads the peer side, so the write blocks until the deadline
	// aborts it.
	if err := a.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	_, err := a.Write(make([]byte, 1))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("got %v, want os.ErrDeadlineExceeded", err)
	}
}

func TestStreamConnClearDeadline(t *testing.T) {
	a, b := connPair(t)

	// A deadline set and cleared before expiry must not fire.
	if err := a.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if err := a.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear deadline: %v", err)
	}
	time.Sleep(40 * time.Millisecond)

	got := make(chan error, 1)
	go func() {
		_, err := a.Read(make([]byte, 4))
		got <- err
	}()
	if _, err := b.Write([]byte("ok")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := <-got; err != nil {
		t.Fatalf("Read after cleared deadline: %v", err)
	}
}

func TestStreamConnPastDeadlineFailsImmediately(t *testing.T) {
	a, _ := connPair(t)

	if err := a.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := a.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("got %v, want os.ErrDeadlineExceeded", err)
	}
}

func TestStreamConnCloseSignalsDoneAndPeer(t *testing.T) {
	as, bs := newPipeStreamPair(NewAddr(NetworkPipe, "nc-close-a"), NewAddr(NetworkPipe, "nc-close-b"))
	a, b := NewStreamConn(as), NewStreamConn(bs)
	defer b.Close()

	readErr := make(chan error, 1)
	go func() {
		_, err := b.Read(make([]byte, 1))
		readErr <- err
	}()

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-a.Done():
	default:
		t.Fatal("Done not closed after Close")
	}
	// The peer's blocked read terminates: the write side closed cleanly.
	select {
	case err := <-readErr:
		if !errors.Is(err, io.EOF) {
			// Abort-before-close can surface as a StreamError instead of
			// EOF; either way the read must not hang.
			if _, ok := errors.AsType[*StreamError](err); !ok {
				t.Fatalf("peer read: %v", err)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("peer read did not unblock on Close")
	}
	// Close is idempotent.
	if err := a.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
