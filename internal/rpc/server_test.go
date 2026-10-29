package rpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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
// pingFrame is the opPing request wire frame: the opcode, the header length,
// then the header itself.
func pingFrame(header string) []byte {
	frame := make([]byte, 0, 8+len(header))
	frame = binary.BigEndian.AppendUint32(frame, uint32(opPing))
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(header)))
	return append(frame, header...)
}

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
	if _, err := stream.Write(pingFrame("hello")); err != nil {
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

// A stream aborted with StreamCodeCallerGone is the client withdrawing its
// request, which the pool already declines to charge to the peer. An idle
// abort is the peer failing to answer and stays a fault.
func TestStreamErrorLevelFollowsTheAbortCode(t *testing.T) {
	cases := []struct {
		name  string
		code  transport.StreamErrorCode
		quiet bool
	}{
		{"the caller withdrew its request", transport.StreamCodeCallerGone, true},
		{"the stream went idle", transport.StreamCodeIdle, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dialed, accepted := quicConnPair(t)
			logs := captureLogs(t)

			// The handler is what observes the abort, so the test can wait for
			// that rather than for the log it is asserting about.
			reading := make(chan struct{})
			observed := make(chan error, 1)
			mux := NewMux()
			RegisterHandler(mux, opPing, func(_ context.Context, _ pingHeader, stream transport.Stream) error {
				close(reading)
				_, err := io.ReadAll(stream)
				observed <- err
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
			if _, err := stream.Write(pingFrame("hello")); err != nil {
				t.Fatalf("write stream metadata: %v", err)
			}

			select {
			case <-reading:
			case <-time.After(5 * time.Second):
				t.Fatal("handler never reached its read")
			}
			stream.CancelWrite(tc.code)

			var seen error
			select {
			case seen = <-observed:
			case <-time.After(5 * time.Second):
				t.Fatal("handler never observed the abort")
			}
			var se *transport.StreamError
			if !errors.As(seen, &se) || se.Code != tc.code {
				t.Fatalf("handler saw %v, want a stream error with code %d", seen, tc.code)
			}

			// serveConn waits for its stream goroutines, so once it returns
			// the log call for this abort has happened or never will.
			dialed.Close()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("serveConn did not return after the peer closed")
			}

			logged := bytes.Contains(logs.Bytes(), []byte(`"msg":"stream error"`))
			if tc.quiet && logged {
				t.Fatalf("code %d logged at ERROR:\n%s", tc.code, logs)
			}
			if !tc.quiet && !logged {
				t.Fatalf("code %d was not logged:\n%s", tc.code, logs)
			}
		})
	}
}
