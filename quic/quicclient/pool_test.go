package quicclient

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"sync/atomic"
	"testing"
	"time"

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

	tlsConf := generateTestTLSConfig(t)
	tlsConf.NextProtos = []string{alpn}

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

// TestQuicClientPool_ReapEvictsLiveConnection is the targeted reproducer for
// Bug A in docs/development/bugs/multipart-upload-deadlock.md. The pool's
// cleanup() evicts connections purely on `lastUsed` staleness and has no
// awareness of in-flight streams. A long-running multipart upload that opens
// one stream per part but doesn't re-Get the pooled connection will cross
// the 2-minute idle threshold while the upload is still in flight; cleanup()
// then closes the connection out from under the live streams.
//
// This test drives that exact shape deterministically by mutating the
// `pc.lastUsed` timestamp directly (backdating it past the 2-minute
// threshold) and then calling cleanup() once. Under the current (buggy)
// implementation the connection is evicted even though we have an open,
// in-flight stream. A proper fix — tracking in-flight streams, or bumping
// lastUsed on stream open — will flip this test from failure to success.
//
// Acceptance:
//   - On `main` today: assertions about the live connection surviving reap
//     FAIL (connection is evicted and/or the stream is closed under us).
//   - After fix: assertions PASS (reaper skips the connection with in-flight
//     streams, and the stream remains usable).
func TestQuicClientPool_ReapEvictsLiveConnection(t *testing.T) {
	if os.Getenv("PREDASTORE_BUG_REPRO") != "1" {
		t.Skip("bug reproducer — set PREDASTORE_BUG_REPRO=1 to run")
	}
	addr, stop := startMinimalQUICListener(t)
	defer stop()

	pool := NewPool()
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := pool.Get(ctx, addr)
	require.NoError(t, err)
	require.NotNil(t, client)

	// Open a stream on the pooled connection and hold it. This models an
	// in-flight multipart part upload that has not yet completed.
	stream, err := client.conn.OpenStreamSync(ctx)
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()

	// Backdate lastUsed past the 2-minute idle threshold used by cleanup().
	// This is the condition a real upload hits whenever a single part takes
	// longer than 2 minutes to drain (large part, slow network, contended
	// server-side WAL lock, etc.).
	pool.mu.RLock()
	pc := pool.connections[addr]
	pool.mu.RUnlock()
	require.NotNil(t, pc, "connection should be in pool after Get")

	pc.mu.Lock()
	pc.lastUsed = time.Now().Add(-3 * time.Minute)
	pc.mu.Unlock()

	// Trigger the reaper synchronously (the 30s background ticker is too
	// slow for a unit test).
	pool.cleanup()

	// Bug A signature: the pooled connection entry has been evicted even
	// though a stream is in flight.
	pool.mu.RLock()
	_, stillPooled := pool.connections[addr]
	pool.mu.RUnlock()
	require.True(t, stillPooled,
		"Bug A: pool.cleanup() evicted a connection with an in-flight stream. "+
			"Reaper must either track active streams or consult the connection's "+
			"stream count before closing.")

	// Stronger signal: the stream itself should still be usable after reap.
	// If cleanup() closed the underlying connection, this write will error.
	_, err = stream.Write([]byte{0})
	require.NoError(t, err,
		"Bug A (secondary): connection was closed by reaper while stream was in flight")
}

// generateTestTLSConfig mirrors quicserver's makeServerTLSConfig but lives
// in this package to avoid importing quicserver (which would create a cycle).
func generateTestTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}
}
