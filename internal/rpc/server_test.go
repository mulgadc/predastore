package rpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/mulgadc/predastore/internal/transport"
)

// quicConnPair returns a dialed QUIC connection and the one its listener
// accepted, over loopback.
func quicConnPair(t *testing.T) (dialed, accepted transport.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	certPath, keyPath, pool := testcerts.Generate(t)
	server, err := transport.NewQUICTransport("127.0.0.1", 0, certPath, keyPath)
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	t.Cleanup(func() { server.Close() })
	ln, err := server.Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	client, err := transport.NewQUICTransport("127.0.0.1", 0, certPath, keyPath, transport.WithRootCAs(pool))
	if err != nil {
		t.Fatalf("NewQUICTransport: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	dialed, err = client.Dial(ctx, ln.Addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { dialed.Close() })

	accepted, err = ln.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	t.Cleanup(func() { accepted.Close() })
	return dialed, accepted
}

// captureLogs routes the default logger into a JSON buffer for one test.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

func TestServeConnQuietWhenQUICPeerCloses(t *testing.T) {
	dialed, accepted := quicConnPair(t)
	logs := captureLogs(t)

	srv, err := NewServer(pingMux(), nil, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.serveConn(context.Background(), context.Background(), accepted, func() { accepted.Close() })
	}()

	dialed.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("serveConn did not return after the peer closed")
	}

	if bytes.Contains(logs.Bytes(), []byte(`"level":"ERROR"`)) {
		t.Fatalf("graceful peer close logged at ERROR:\n%s", logs)
	}
}

// A handler parked in a stream read when the peer closes the connection gets a
// connection-level error, which reaches the stream log site rather than the
// accept one. Prod logged 12 of these as ERROR on one node.
func TestStreamErrorQuietWhenQUICPeerClosesMidStream(t *testing.T) {
	dialed, accepted := quicConnPair(t)
	logs := captureLogs(t)

	reading := make(chan struct{})
	mux := NewMux()
	RegisterHandler(mux, opPing, func(_ context.Context, _ pingHeader, stream transport.Stream) error {
		close(reading)
		_, err := io.ReadAll(stream)
		return err
	})

	srv, err := NewServer(mux, nil, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.serveConn(context.Background(), context.Background(), accepted, func() { accepted.Close() })
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := dialed.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	header := []byte("hello")
	prefix := make([]byte, 8)
	binary.BigEndian.PutUint32(prefix, uint32(opPing))
	binary.BigEndian.PutUint32(prefix[4:8], uint32(len(header)))
	if _, err := stream.Write(append(prefix, header...)); err != nil {
		t.Fatalf("write stream metadata: %v", err)
	}

	select {
	case <-reading:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never reached its read")
	}

	dialed.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("serveConn did not return after the peer closed")
	}

	if bytes.Contains(logs.Bytes(), []byte(`"level":"ERROR"`)) {
		t.Fatalf("graceful peer close mid-stream logged at ERROR:\n%s", logs)
	}
}
