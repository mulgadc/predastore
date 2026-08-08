package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// testPort hands out a distinct port per transport: the pipe registry is
// process-wide, so two tests binding the same name would collide.
var testPort atomic.Int64

func nextPort() int { return int(testPort.Add(1)) }

// bindPipe binds a pipe transport on the given port, with cleanup registered.
func bindPipe(t *testing.T, port int) *PipeTransport {
	t.Helper()
	pt := NewPipeTransport("127.0.0.1", port)
	t.Cleanup(func() { pt.Close() })
	return pt
}

func testTransport(t *testing.T) *PipeTransport {
	t.Helper()
	return bindPipe(t, nextPort())
}

// dialedPair returns a connected conn pair plus the transports that own the
// two ends, with cleanup registered.
func dialedPair(t *testing.T) (lt, dt *PipeTransport, dial, accepted Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	lt = testTransport(t)
	ln, err := lt.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	acceptedCh := make(chan Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		c, err := ln.Accept(ctx)
		if err != nil {
			errCh <- err
			return
		}
		acceptedCh <- c
	}()

	dt = testTransport(t)
	dial, err = dt.Dial(ctx, lt.Addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	select {
	case accepted = <-acceptedCh:
	case err := <-errCh:
		t.Fatalf("Accept: %v", err)
	}
	t.Cleanup(func() { dial.Close() })
	return lt, dt, dial, accepted
}

// streamPair opens a stream from dc and accepts it on ac.
func streamPair(t *testing.T, dc, ac Conn) (opened, accepted Stream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	acceptedCh := make(chan Stream, 1)
	errCh := make(chan error, 1)
	go func() {
		s, err := ac.AcceptStream(ctx)
		if err != nil {
			errCh <- err
			return
		}
		acceptedCh <- s
	}()

	opened, err := dc.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	select {
	case accepted = <-acceptedCh:
	case err := <-errCh:
		t.Fatalf("AcceptStream: %v", err)
	}
	return opened, accepted
}

func TestPipeDialNoListener(t *testing.T) {
	pt := testTransport(t)
	_, err := pt.Dial(context.Background(), NewAddr(NetworkPipe, "127.0.0.1:65000"))
	if !errors.Is(err, ErrNoListener) {
		t.Fatalf("got %v, want ErrNoListener", err)
	}
}

func TestPipeDialNilAddr(t *testing.T) {
	pt := testTransport(t)
	if _, err := pt.Dial(context.Background(), nil); !errors.Is(err, ErrMissingAddr) {
		t.Fatalf("got %v, want ErrMissingAddr", err)
	}
}

func TestPipeListenTwiceSameAddr(t *testing.T) {
	port := nextPort()
	a := bindPipe(t, port)
	ln, err := a.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	// A second transport bound to the same name cannot take the registry entry.
	if _, err := bindPipe(t, port).Listen(); !errors.Is(err, ErrAddrAlreadyInUse) {
		t.Fatalf("other transport: got %v, want ErrAddrAlreadyInUse", err)
	}
	// Nor may one transport listen twice.
	if _, err := a.Listen(); !errors.Is(err, ErrAddrAlreadyInUse) {
		t.Fatalf("same transport: got %v, want ErrAddrAlreadyInUse", err)
	}
}

func TestPipeListenerCloseUnblocksAccept(t *testing.T) {
	port := nextPort()
	pt := bindPipe(t, port)
	ln, err := pt.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := ln.Accept(context.Background())
		errCh <- err
	}()

	// Give Accept a moment to block before closing under it.
	time.Sleep(10 * time.Millisecond)
	ln.Close()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrListenerClosed) {
			t.Fatalf("got %v, want ErrListenerClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Accept did not unblock on Close")
	}

	// The name is free again after close.
	if _, err := bindPipe(t, port).Listen(); err != nil {
		t.Fatalf("relisten after close: %v", err)
	}
}

func TestPipeTransportCloseClosesListener(t *testing.T) {
	pt := testTransport(t)
	ln, err := pt.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	pt.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := ln.Accept(ctx); !errors.Is(err, ErrListenerClosed) {
		t.Fatalf("Accept: got %v, want ErrListenerClosed", err)
	}
	if _, err := pt.Listen(); !errors.Is(err, ErrTransportClosed) {
		t.Fatalf("Listen: got %v, want ErrTransportClosed", err)
	}
	if _, err := pt.Dial(context.Background(), NewAddr(NetworkPipe, "127.0.0.1:65000")); !errors.Is(err, ErrTransportClosed) {
		t.Fatalf("Dial: got %v, want ErrTransportClosed", err)
	}
}

func TestPipeConnAddrs(t *testing.T) {
	lt, dt, dial, accepted := dialedPair(t)
	if got := dial.LocalAddr().String(); got != dt.Addr().String() {
		t.Fatalf("dial local = %s, want %s", got, dt.Addr())
	}
	if got := dial.RemoteAddr().String(); got != lt.Addr().String() {
		t.Fatalf("dial remote = %s, want %s", got, lt.Addr())
	}
	if got := accepted.LocalAddr().String(); got != lt.Addr().String() {
		t.Fatalf("accepted local = %s, want %s", got, lt.Addr())
	}
	// The accepted end must name the dialer's own bound source, so the peer
	// is identifiable from the connection alone.
	if got := accepted.RemoteAddr().String(); got != dt.Addr().String() {
		t.Fatalf("accepted remote = %s, want dialer source %s", got, dt.Addr())
	}
}

func TestPipeStreamRoundTrip(t *testing.T) {
	_, _, dc, ac := dialedPair(t)
	opened, accepted := streamPair(t, dc, ac)

	// Reader must run concurrently: pipe writes rendezvous with reads.
	got := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(accepted)
		got <- b
	}()

	msg := []byte("hello over pipe")
	if _, err := opened.Write(msg); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := opened.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if b := <-got; !bytes.Equal(b, msg) {
		t.Fatalf("got %q, want %q", b, msg)
	}

	// The reverse direction still works after the opener closed its write side.
	reply := []byte("reply")
	go func() {
		accepted.Write(reply)
		accepted.Close()
	}()
	b, err := io.ReadAll(opened)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if !bytes.Equal(b, reply) {
		t.Fatalf("got %q, want %q", b, reply)
	}
}

func TestPipeStreamCancelWrite(t *testing.T) {
	_, _, dc, ac := dialedPair(t)
	opened, accepted := streamPair(t, dc, ac)

	opened.CancelWrite(7)

	_, err := io.ReadAll(accepted)
	var se *StreamError
	if !errors.As(err, &se) || se.Code != 7 {
		t.Fatalf("peer read: got %v, want StreamError code 7", err)
	}
	// Our own writes fail after the abort.
	if _, err := opened.Write([]byte("x")); err == nil {
		t.Fatal("Write after CancelWrite succeeded")
	}
}

func TestPipeStreamCancelRead(t *testing.T) {
	_, _, dc, ac := dialedPair(t)
	opened, accepted := streamPair(t, dc, ac)

	opened.CancelRead(9)

	// The peer's writes fail with the code once the abort lands.
	var err error
	for range 10 {
		if _, err = accepted.Write([]byte("x")); err != nil {
			break
		}
	}
	var se *StreamError
	if !errors.As(err, &se) || se.Code != 9 {
		t.Fatalf("peer write: got %v, want StreamError code 9", err)
	}
	// Our own reads fail too.
	if _, err := opened.Read(make([]byte, 1)); err == nil {
		t.Fatal("Read after CancelRead succeeded")
	}
}

func TestPipeStreamReadFromWriteTo(t *testing.T) {
	_, _, dc, ac := dialedPair(t)
	opened, accepted := streamPair(t, dc, ac)

	src := strings.Repeat("payload!", 4096)
	var dst bytes.Buffer

	done := make(chan error, 1)
	go func() {
		_, err := accepted.WriteTo(&dst)
		done <- err
	}()

	n, err := opened.ReadFrom(strings.NewReader(src))
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if n != int64(len(src)) {
		t.Fatalf("ReadFrom copied %d, want %d", n, len(src))
	}
	opened.Close()
	if err := <-done; err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if dst.String() != src {
		t.Fatalf("payload mismatch: got %d bytes, want %d", dst.Len(), len(src))
	}
}

func TestPipeConnCloseUnblocksStreams(t *testing.T) {
	_, _, dc, ac := dialedPair(t)

	errCh := make(chan error, 1)
	go func() {
		_, err := ac.AcceptStream(context.Background())
		errCh <- err
	}()

	time.Sleep(10 * time.Millisecond)
	// Closing one endpoint closes the connection for both.
	dc.Close()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrConnClosed) {
			t.Fatalf("AcceptStream: got %v, want ErrConnClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("AcceptStream did not unblock on conn close")
	}

	if _, err := dc.OpenStream(context.Background()); !errors.Is(err, ErrConnClosed) {
		t.Fatalf("OpenStream: got %v, want ErrConnClosed", err)
	}
}

func TestPipeStreamsSurviveConnClose(t *testing.T) {
	_, _, dc, ac := dialedPair(t)
	opened, accepted := streamPair(t, dc, ac)

	dc.Close()

	got := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(accepted)
		got <- b
	}()
	if _, err := opened.Write([]byte("still alive")); err != nil {
		t.Fatalf("Write after conn close: %v", err)
	}
	opened.Close()
	if b := <-got; string(b) != "still alive" {
		t.Fatalf("got %q", b)
	}
}

func TestPipeDialContextCanceled(t *testing.T) {
	pt := testTransport(t)
	ln, err := pt.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	// Nobody accepts, so the dial rendezvous blocks until the context fires.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	dt := testTransport(t)
	if _, err := dt.Dial(ctx, ln.Addr()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
}

func TestPipeOpErrorsCarryAddrs(t *testing.T) {
	pt := testTransport(t)
	_, err := pt.Dial(context.Background(), NewAddr(NetworkPipe, "127.0.0.1:65000"))
	var oe *net.OpError
	if !errors.As(err, &oe) {
		t.Fatalf("got %T, want *net.OpError", err)
	}
	if oe.Net != "pipe" || oe.Addr.String() != "127.0.0.1:65000" || oe.Source.String() != pt.Addr().String() {
		t.Fatalf("OpError = %+v", oe)
	}
}
