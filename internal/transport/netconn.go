package transport

import (
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// streamConnAbortCode marks a stream aborted by the net.Conn adapter, on
// deadline expiry or close.
const streamConnAbortCode StreamErrorCode = 1

var _ net.Conn = (*StreamConn)(nil)

// StreamConn adapts a Stream to net.Conn for consumers that require that
// contract, notably the hashicorp/raft network transport.
//
// Deadlines are destructive: a stream cannot interrupt a blocked read or
// write and resume later, so an expired deadline aborts the affected
// direction and the connection is unusable afterwards. Consumers that
// discard connections on deadline errors — raft does — observe standard
// net.Conn behavior.
type StreamConn struct {
	s Stream

	mu     sync.Mutex
	rTimer *time.Timer
	wTimer *time.Timer

	rExpired atomic.Bool
	wExpired atomic.Bool

	done      chan struct{}
	closeOnce sync.Once
}

func NewStreamConn(s Stream) *StreamConn {
	return &StreamConn{s: s, done: make(chan struct{})}
}

// Done is closed when the connection is closed; it lets the goroutine that
// owns the underlying rpc stream hold it open for the connection's lifetime.
func (c *StreamConn) Done() <-chan struct{} { return c.done }

func (c *StreamConn) Read(p []byte) (int, error) {
	n, err := c.s.Read(p)
	if err != nil && c.rExpired.Load() {
		return n, os.ErrDeadlineExceeded
	}
	return n, err
}

func (c *StreamConn) Write(p []byte) (int, error) {
	n, err := c.s.Write(p)
	if err != nil && c.wExpired.Load() {
		return n, os.ErrDeadlineExceeded
	}
	return n, err
}

// Close aborts the read side and closes the write side so both directions
// terminate promptly, then signals Done.
func (c *StreamConn) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.stopTimersLocked()
		c.mu.Unlock()
		c.s.CancelRead(streamConnAbortCode)
		c.s.Close()
		close(c.done)
	})
	return nil
}

func (c *StreamConn) LocalAddr() net.Addr  { return c.s.LocalAddr() }
func (c *StreamConn) RemoteAddr() net.Addr { return c.s.RemoteAddr() }

func (c *StreamConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

func (c *StreamConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rTimer != nil {
		c.rTimer.Stop()
		c.rTimer = nil
	}
	if t.IsZero() {
		return nil
	}
	if d := time.Until(t); d > 0 {
		c.rTimer = time.AfterFunc(d, c.expireRead)
	} else {
		c.expireRead()
	}
	return nil
}

func (c *StreamConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.wTimer != nil {
		c.wTimer.Stop()
		c.wTimer = nil
	}
	if t.IsZero() {
		return nil
	}
	if d := time.Until(t); d > 0 {
		c.wTimer = time.AfterFunc(d, c.expireWrite)
	} else {
		c.expireWrite()
	}
	return nil
}

// expireRead aborts the read side; a pending or future Read observes the
// abort and reports os.ErrDeadlineExceeded.
func (c *StreamConn) expireRead() {
	c.rExpired.Store(true)
	c.s.CancelRead(streamConnAbortCode)
}

func (c *StreamConn) expireWrite() {
	c.wExpired.Store(true)
	c.s.CancelWrite(streamConnAbortCode)
}

func (c *StreamConn) stopTimersLocked() {
	if c.rTimer != nil {
		c.rTimer.Stop()
		c.rTimer = nil
	}
	if c.wTimer != nil {
		c.wTimer.Stop()
		c.wTimer = nil
	}
}
