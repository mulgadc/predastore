package blob

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob/engine"
	"github.com/mulgadc/predastore/internal/transport"
)

// stallReader is a value the store hands out. Close records that it ran, which
// is what releases the segment reference in the real engine.
type stallReader struct {
	*bytes.Reader

	closed atomic.Bool
}

func (r *stallReader) Close() error  { r.closed.Store(true); return nil }
func (r *stallReader) Size() int64   { return r.Reader.Size() }
func (r *stallReader) Epoch() uint64 { return 1 }
func (r *stallReader) WriteTo(w io.Writer) (int64, error) {
	return io.Copy(w, r.Reader)
}

type stallStore struct {
	Store

	reader *stallReader
}

func (s *stallStore) Lookup(_ [32]byte, _ uint32) (engine.Reader, error) { return s.reader, nil }

// stallStream models a peer that stops draining the body without aborting the
// stream: the envelope is accepted, then the copy never completes.
type stallStream struct {
	transport.Stream

	envelope  bytes.Buffer
	cancelled chan struct{}
	once      atomic.Bool
}

func (s *stallStream) Write(p []byte) (int, error) { return s.envelope.Write(p) }

func (s *stallStream) ReadFrom(r io.Reader) (int64, error) {
	// One read to arm the guard, then block as a stalled flow-control window
	// does, until the guard aborts the write.
	if _, err := r.Read(make([]byte, 1)); err != nil {
		return 0, err
	}
	<-s.cancelled
	return 0, errors.New("stream reset")
}

func (s *stallStream) CancelWrite(transport.StreamErrorCode) {
	if s.once.CompareAndSwap(false, true) {
		close(s.cancelled)
	}
}

func (s *stallStream) CancelRead(transport.StreamErrorCode) {}
func (s *stallStream) Close() error                         { return nil }
func (s *stallStream) LocalAddr() net.Addr                  { return nil }
func (s *stallStream) RemoteAddr() net.Addr                 { return nil }
func (s *stallStream) Read([]byte) (int, error)             { return 0, io.EOF }
func (s *stallStream) WriteTo(io.Writer) (int64, error)     { return 0, io.EOF }

// A get whose peer stops reading must be abandoned rather than held open. The
// reader pins its segment until it closes, and compaction defers every segment
// that is pinned, so an unbounded body write is an unbounded compaction stall.
func TestHandleGetAbortsAStalledBody(t *testing.T) {
	reader := &stallReader{Reader: bytes.NewReader(bytes.Repeat([]byte{0x7}, 64*1024))}
	srv := &Server{
		cfg:   Config{NodeID: 1, BodyIdleTimeout: 100 * time.Millisecond},
		store: &stallStore{reader: reader},
	}
	stream := &stallStream{cancelled: make(chan struct{})}

	done := make(chan error, 1)
	go func() {
		done <- srv.handleGet(context.Background(), Request{RangeStart: -1, RangeEnd: -1}, stream)
	}()

	select {
	case err := <-done:
		if !errors.Is(err, transport.ErrIdleTimeout) {
			t.Fatalf("want idle timeout, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handleGet never returned on a stalled body write")
	}

	if !reader.closed.Load() {
		t.Error("the reader must be closed, or its segment stays pinned")
	}
}
