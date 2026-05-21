package quicclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/testcerts"
	quic "github.com/quic-go/quic-go"
	"github.com/stretchr/testify/require"
)

// poolTestPortCounter hands each test invocation a unique UDP port.
var poolTestPortCounter atomic.Int32

// startMinimalQUICListener spins up a trivial quic-go listener that accepts
// connections and streams but never replies. Enough to exercise the pool's
// dial + cleanup behaviour without pulling in quicserver (which would create
// an import cycle since quicclient is imported by quicserver transitively).
func startMinimalQUICListener(t *testing.T) (string, func()) {
	t.Helper()

	certPath, keyPath, pool := testcerts.Generate(t)
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	require.NoError(t, err)
	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{alpn},
	}

	SetDefaultRootCAs(pool)
	t.Cleanup(func() { SetDefaultRootCAs(nil) })

	port := 47000 + int(poolTestPortCounter.Add(1))
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	ln, err := quic.ListenAddr(addr, tlsConf, &quic.Config{
		KeepAlivePeriod: 15 * time.Second,
		MaxIdleTimeout:  60 * time.Second,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			go func(c *quic.Conn) {
				for {
					s, err := c.AcceptStream(ctx)
					if err != nil {
						return
					}
					_ = s
				}
			}(conn)
		}
	}()

	return addr, func() {
		cancel()
		_ = ln.Close()
	}
}

// TestQuicClientPool_ReapSkipsActiveRPCStreams validates the fix for Bug A
// in docs/development/bugs/multipart-upload-deadlock.md. Pool.Get bumps
// lastUsed on entry, but subsequent doPut/doDelete/do calls on the same
// pooled client do not, so a handler that holds a stream longer than maxIdle
// (2 minutes) became eligible for reaping mid-transfer. The fix tracks
// active RPC streams on quicclient.Client (incActive/decActive around
// doPut/doDelete/do) and makes cleanup() treat a stale connection with
// activeStreams > 0 as busy rather than idle.
//
// This test drives the fix's exact code path: bump the client's
// activeStreams counter (as doPut would do on entry), backdate lastUsed
// past the threshold, call cleanup(), and assert the connection is NOT
// evicted. This would have failed pre-fix (no counter existed) and must
// pass post-fix.
func TestQuicClientPool_ReapSkipsActiveRPCStreams(t *testing.T) {
	addr, stop := startMinimalQUICListener(t)
	defer stop()

	pool := NewPool()
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := pool.Get(ctx, addr)
	require.NoError(t, err)
	require.NotNil(t, client)

	// Simulate an in-flight RPC by incrementing activeStreams the way
	// doPut does after OpenStreamSync. Real workloads hit this via
	// client.Put; we take the shortcut here because the minimal listener
	// above does not speak the RPC protocol.
	client.incActive()
	defer client.decActive()

	// Backdate lastUsed past the 2-minute idle threshold.
	pool.mu.RLock()
	pc := pool.connections[addr]
	pool.mu.RUnlock()
	require.NotNil(t, pc)

	pc.mu.Lock()
	pc.lastUsed = time.Now().Add(-3 * time.Minute)
	pc.mu.Unlock()

	// Trigger the reaper synchronously (background ticker runs every 30s).
	pool.cleanup()

	// Post-fix expectation: connection survives reap because
	// activeStreams > 0.
	pool.mu.RLock()
	_, stillPooled := pool.connections[addr]
	pool.mu.RUnlock()
	require.True(t, stillPooled,
		"cleanup() evicted a connection with activeStreams > 0 — Bug A regression")

	// Sanity: once the RPC completes (decActive), the connection becomes
	// eligible for reaping again on the next cleanup() pass.
	client.decActive()
	client.incActive() // restore the deferred decActive balance below
	pc.mu.Lock()
	pc.lastUsed = time.Now().Add(-3 * time.Minute)
	pc.mu.Unlock()

	// Drop the "active" marker and re-trigger reap. Now the connection
	// must be evicted.
	client.decActive()
	pool.cleanup()
	pool.mu.RLock()
	_, stillPooled = pool.connections[addr]
	pool.mu.RUnlock()
	require.False(t, stillPooled,
		"cleanup() failed to evict a genuinely idle connection — reaper is now over-conservative")

	// Keep the deferred decActive balanced (we did one extra inc above).
	client.incActive()
}
