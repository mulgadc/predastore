package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// dialedPair returns a connected conn pair plus the listener, with cleanup
// registered. Each test uses a unique name: the pipe registry is global.
func dialedPair(t *testing.T, name string) (dial, accepted Conn, ln Listener) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	lt := NewPipeTransport(name)
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

	dt := NewPipeTransport(name + "-client")
	dial, err = dt.Dial(ctx, ln.Addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	select {
	case accepted = <-acceptedCh:
	case err := <-errCh:
		t.Fatalf("Accept: %v", err)
	}
	t.Cleanup(func() { dial.Close() })
	return dial, accepted, ln
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

func TestPipeResolveAddr(t *testing.T) {
	addr, err := ResolveAddr(string(NetworkPipe), "node-1")
	if err != nil {
		t.Fatalf("ResolveAddr: %v", err)
	}
	if addr.Network() != "pipe" || addr.String() != "node-1" {
		t.Fatalf("got %s/%s, want pipe/node-1", addr.Network(), addr.String())
	}

	if _, err := ResolveAddr("bogus", "x"); !errors.As(err, new(UnknownNetworkError)) {
		t.Fatalf("unknown network: got %v, want UnknownNetworkError", err)
	}
}

func TestPipeDialNoListener(t *testing.T) {
	pt := NewPipeTransport("dial-no-listener-client")
	_, err := pt.Dial(context.Background(), newPipeAddr("dial-no-listener"))
	if !errors.Is(err, ErrNoListener) {
		t.Fatalf("got %v, want ErrNoListener", err)
	}
}

func TestPipeDialNilAddr(t *testing.T) {
	pt := NewPipeTransport("dial-nil-addr")
	if _, err := pt.Dial(context.Background(), nil); !errors.Is(err, ErrMissingAddr) {
		t.Fatalf("got %v, want ErrMissingAddr", err)
	}
}

func TestPipeListenTwiceSameName(t *testing.T) {
	a := NewPipeTransport("listen-twice")
	ln, err := a.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	b := NewPipeTransport("listen-twice")
	if _, err := b.Listen(); !errors.Is(err, ErrAddrAlreadyInUse) {
		t.Fatalf("got %v, want ErrAddrAlreadyInUse", err)
	}
}

func TestPipeListenerCloseUnblocksAccept(t *testing.T) {
	pt := NewPipeTransport("close-unblocks-accept")
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
	if _, err := NewPipeTransport("close-unblocks-accept").Listen(); err != nil {
		t.Fatalf("relisten after close: %v", err)
	}
}

func TestPipeTransportCloseClosesListener(t *testing.T) {
	pt := NewPipeTransport("transport-close")
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
	if _, err := pt.Dial(context.Background(), newPipeAddr("x")); !errors.Is(err, ErrTransportClosed) {
		t.Fatalf("Dial: got %v, want ErrTransportClosed", err)
	}
}

func TestPipeConnAddrs(t *testing.T) {
	dial, accepted, ln := dialedPair(t, "conn-addrs")
	if got := dial.RemoteAddr().String(); got != ln.Addr().String() {
		t.Fatalf("dial remote = %s, want %s", got, ln.Addr())
	}
	if got := accepted.LocalAddr().String(); got != ln.Addr().String() {
		t.Fatalf("accepted local = %s, want %s", got, ln.Addr())
	}
	if got := accepted.RemoteAddr().String(); got != dial.LocalAddr().String() {
		t.Fatalf("accepted remote = %s, want %s", got, dial.LocalAddr())
	}
}

func TestPipeStreamRoundTrip(t *testing.T) {
	dc, ac, _ := dialedPair(t, "stream-roundtrip")
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
	dc, ac, _ := dialedPair(t, "cancel-write")
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
	dc, ac, _ := dialedPair(t, "cancel-read")
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
	dc, ac, _ := dialedPair(t, "readfrom-writeto")
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
	dc, ac, _ := dialedPair(t, "conn-close-streams")

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
	dc, ac, _ := dialedPair(t, "streams-survive-close")
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
	pt := NewPipeTransport("dial-ctx")
	ln, err := pt.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	// Nobody accepts, so the dial rendezvous blocks until the context fires.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	dt := NewPipeTransport("dial-ctx-client")
	if _, err := dt.Dial(ctx, ln.Addr()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
}

func TestPipeOpErrorsCarryAddrs(t *testing.T) {
	pt := NewPipeTransport("operr-client")
	_, err := pt.Dial(context.Background(), newPipeAddr("operr-missing"))
	var oe *net.OpError
	if !errors.As(err, &oe) {
		t.Fatalf("got %T, want *net.OpError", err)
	}
	if oe.Net != "pipe" || oe.Addr.String() != "operr-missing" {
		t.Fatalf("OpError = %+v", oe)
	}
}
