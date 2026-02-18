package quicclient

import (
	"context"
	"crypto/tls"
	"log/slog"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// Pool manages a pool of QUIC connections to multiple nodes.
// Connections are reused to avoid TLS handshake overhead.
type Pool struct {
	mu          sync.RWMutex
	connections map[string]*pooledConn
	tlsConf     *tls.Config
	quicConf    *quic.Config
}

type pooledConn struct {
	client   *Client
	lastUsed time.Time
	useCount int64
	mu       sync.Mutex
}

// NewPool creates a new connection pool.
func NewPool() *Pool {
	p := &Pool{
		connections: make(map[string]*pooledConn),
		tlsConf: &tls.Config{
			InsecureSkipVerify: true, // demo only. Use mTLS with your CA in prod.
			NextProtos:         []string{alpn},
		},
		quicConf: &quic.Config{
			HandshakeIdleTimeout: 5 * time.Second,
			KeepAlivePeriod:      15 * time.Second,
			MaxIdleTimeout:       120 * time.Second, // Longer timeout for pooled connections
		},
	}

	// Start background cleanup goroutine
	go p.cleanupLoop()

	return p
}

// Get returns a Client for the given address, reusing an existing connection if available.
func (p *Pool) Get(ctx context.Context, addr string) (*Client, error) {
	p.mu.RLock()
	pc, exists := p.connections[addr]
	p.mu.RUnlock()

	if exists {
		pc.mu.Lock()
		// Check if connection is still alive - a closed QUIC connection's Context() is canceled
		if pc.client != nil && pc.client.conn != nil && pc.client.conn.Context().Err() == nil {
			// Connection exists and is still open
			pc.lastUsed = time.Now()
			pc.useCount++
			useCount := pc.useCount
			pc.mu.Unlock()
			slog.Debug("Pool.Get: reusing connection", "addr", addr, "useCount", useCount)
			return pc.client, nil
		}
		pc.mu.Unlock()
		slog.Debug("Pool.Get: existing connection dead", "addr", addr)
	}

	// Need to create a new connection
	slog.Debug("Pool.Get: creating new connection", "addr", addr)
	return p.createConnection(ctx, addr)
}

// createConnection creates a new pooled connection.
func (p *Pool) createConnection(ctx context.Context, addr string) (*Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if pc, exists := p.connections[addr]; exists {
		pc.mu.Lock()
		// Check connection is still alive
		if pc.client != nil && pc.client.conn != nil && pc.client.conn.Context().Err() == nil {
			pc.lastUsed = time.Now()
			pc.useCount++
			pc.mu.Unlock()
			return pc.client, nil
		}
		// Connection is dead, clean it up
		if pc.client != nil {
			pc.client.Close()
		}
		delete(p.connections, addr)
		pc.mu.Unlock()
	}

	// Create new QUIC connection
	conn, err := quic.DialAddr(ctx, addr, p.tlsConf, p.quicConf)
	if err != nil {
		return nil, err
	}

	client := &Client{conn: conn}
	p.connections[addr] = &pooledConn{
		client:   client,
		lastUsed: time.Now(),
		useCount: 1,
	}

	return client, nil
}

// Release marks a connection as available for reuse.
// For pooled connections, this is a no-op since we reuse connections.
func (p *Pool) Release(addr string, client *Client) {
	// No-op for now - connections stay in pool
	// Could implement reference counting if needed
}

// Invalidate removes a connection from the pool, typically after an error.
// This should be called when a connection error occurs to force reconnection.
func (p *Pool) Invalidate(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if pc, exists := p.connections[addr]; exists {
		pc.mu.Lock()
		if pc.client != nil {
			pc.client.Close()
		}
		pc.mu.Unlock()
		delete(p.connections, addr)
	}
}

// InvalidatePooled removes a connection from the default pool.
func InvalidatePooled(addr string) {
	DefaultPool.Invalidate(addr)
}

// Close closes all pooled connections.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for addr, pc := range p.connections {
		pc.mu.Lock()
		if pc.client != nil {
			pc.client.Close()
		}
		pc.mu.Unlock()
		delete(p.connections, addr)
	}
}

// cleanupLoop periodically removes idle connections.
func (p *Pool) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		p.cleanup()
	}
}

// cleanup removes connections that have been idle for too long or are closed.
func (p *Pool) cleanup() {
	p.mu.Lock()
	defer p.mu.Unlock()

	maxIdle := 2 * time.Minute
	now := time.Now()

	for addr, pc := range p.connections {
		pc.mu.Lock()
		// Remove if idle too long or if connection is already closed
		isIdle := now.Sub(pc.lastUsed) > maxIdle
		isClosed := pc.client == nil || pc.client.conn == nil || pc.client.conn.Context().Err() != nil
		if isIdle || isClosed {
			if pc.client != nil {
				pc.client.Close()
			}
			delete(p.connections, addr)
		}
		pc.mu.Unlock()
	}
}

// Stats returns pool statistics.
func (p *Pool) Stats() map[string]int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := make(map[string]int64)
	stats["total_connections"] = int64(len(p.connections))

	var totalUseCount int64
	for _, pc := range p.connections {
		pc.mu.Lock()
		totalUseCount += pc.useCount
		pc.mu.Unlock()
	}
	stats["total_use_count"] = totalUseCount

	return stats
}

// DefaultPool is the global connection pool.
var DefaultPool = NewPool()

// DialPooled returns a Client from the default pool.
func DialPooled(ctx context.Context, addr string) (*Client, error) {
	return DefaultPool.Get(ctx, addr)
}
