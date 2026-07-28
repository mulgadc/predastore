package transport_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/mulgadc/predastore/internal/transport"
)

// quicPair spins up a listener on an ephemeral port and returns a connected
// conn pair. The client trusts only the test CA.
func quicPair(t *testing.T) (dial, accepted transport.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	certPath, keyPath, pool := testcerts.Generate(t)

	server := transport.NewQUICTransport(transport.QUICTransportConfig{
		TLSCert: certPath,
		TLSKey:  keyPath,
	})
	ln, err := server.Listen(transport.NewQUICAddr("127.0.0.1:0", "node-1"))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	acceptedCh := make(chan transport.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		c, err := ln.Accept(ctx)
		if err != nil {
			errCh <- err
			return
		}
		acceptedCh <- c
	}()

	client := transport.NewQUICTransport(transport.QUICTransportConfig{RootCAs: pool})
	dial, err = client.Dial(ctx, ln.Addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { dial.Close() })

	select {
	case accepted = <-acceptedCh:
	case err := <-errCh:
		t.Fatalf("Accept: %v", err)
	}
	t.Cleanup(func() { accepted.Close() })
	return dial, accepted
}

func TestQUICResolveAddr(t *testing.T) {
	addr, err := transport.ResolveAddr(string(transport.NetworkQUIC), "10.0.0.1:6660")
	if err != nil {
		t.Fatalf("ResolveAddr: %v", err)
	}
	if addr.Network() != "quic" || addr.String() != "10.0.0.1:6660" {
		t.Fatalf("got %s/%s, want quic/10.0.0.1:6660", addr.Network(), addr.String())
	}
}

func TestQUICListenRequiresCerts(t *testing.T) {
	qt := transport.NewQUICTransport(transport.QUICTransportConfig{})
	if _, err := qt.Listen(transport.NewQUICAddr("127.0.0.1:0", "node-1")); err == nil {
		t.Fatal("Listen without certs succeeded")
	}
	if _, err := qt.Listen(nil); !errors.Is(err, transport.ErrMissingAddr) {
		t.Fatalf("got %v, want ErrMissingAddr", err)
	}
	// An address without a node key names no listener.
	if _, err := qt.Listen(transport.NewQUICAddr("127.0.0.1:0", "")); !errors.Is(err, transport.ErrMissingAddr) {
		t.Fatalf("got %v, want ErrMissingAddr", err)
	}
}

func TestQUICStreamRoundTrip(t *testing.T) {
	dc, ac := quicPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opened, err := dc.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	msg := []byte("hello over quic")
	if _, err := opened.Write(msg); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// The stream only materialises on the peer once data flows.
	accepted, err := ac.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	opened.Close()
	b, err := io.ReadAll(accepted)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(b, msg) {
		t.Fatalf("got %q, want %q", b, msg)
	}

	// Reverse direction still open after the opener closed its write side.
	reply := []byte("reply")
	if _, err := accepted.Write(reply); err != nil {
		t.Fatalf("write reply: %v", err)
	}
	accepted.Close()
	b, err = io.ReadAll(opened)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if !bytes.Equal(b, reply) {
		t.Fatalf("got %q, want %q", b, reply)
	}
}

func TestQUICStreamCancelWrite(t *testing.T) {
	dc, ac := quicPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opened, err := dc.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := opened.Write([]byte("partial")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	accepted, err := ac.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}

	opened.CancelWrite(7)

	// The abort races ahead of buffered data in QUIC; all we are guaranteed
	// is that the read eventually fails with the code.
	_, err = io.ReadAll(accepted)
	var se *transport.StreamError
	if !errors.As(err, &se) || se.Code != 7 {
		t.Fatalf("peer read: got %v, want StreamError code 7", err)
	}
}

func TestQUICStreamReadFromWriteTo(t *testing.T) {
	dc, ac := quicPair(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opened, err := dc.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	src := strings.Repeat("payload!", 64*1024)
	done := make(chan error, 1)
	go func() {
		defer opened.Close()
		if _, err := opened.ReadFrom(strings.NewReader(src)); err != nil {
			done <- err
			return
		}
		done <- nil
	}()

	accepted, err := ac.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	var dst bytes.Buffer
	if _, err := accepted.WriteTo(&dst); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if dst.String() != src {
		t.Fatalf("payload mismatch: got %d bytes, want %d", dst.Len(), len(src))
	}
}

func TestQUICListenerCloseUnblocksAccept(t *testing.T) {
	certPath, keyPath, _ := testcerts.Generate(t)
	qt := transport.NewQUICTransport(transport.QUICTransportConfig{
		TLSCert: certPath,
		TLSKey:  keyPath,
	})
	ln, err := qt.Listen(transport.NewQUICAddr("127.0.0.1:0", "node-1"))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := ln.Accept(context.Background())
		errCh <- err
	}()
	time.Sleep(10 * time.Millisecond)
	ln.Close()

	select {
	case err := <-errCh:
		if !errors.Is(err, transport.ErrListenerClosed) {
			t.Fatalf("got %v, want ErrListenerClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Accept did not unblock on Close")
	}
}

func TestQUICDialUntrustedServer(t *testing.T) {
	certPath, keyPath, _ := testcerts.Generate(t)
	server := transport.NewQUICTransport(transport.QUICTransportConfig{
		TLSCert: certPath,
		TLSKey:  keyPath,
	})
	ln, err := server.Listen(transport.NewQUICAddr("127.0.0.1:0", "node-1"))
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	// A client trusting a different CA must refuse the server certificate.
	_, _, otherPool := testcerts.Generate(t)
	client := transport.NewQUICTransport(transport.QUICTransportConfig{RootCAs: otherPool})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := client.Dial(ctx, ln.Addr()); err == nil {
		t.Fatal("Dial with untrusted CA succeeded")
	}
}
