package quicserver_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/quic/quicclient"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/stretchr/testify/require"
)

// quicServerTestPortCounter hands each test invocation a unique port to avoid
// UDP bind conflicts when the OS has not fully released prior test sockets.
var quicServerTestPortCounter atomic.Int32

func newTestQuicServer(t *testing.T) (*quicserver.QuicServer, string) {
	t.Helper()
	dir := t.TempDir()
	port := 46000 + int(quicServerTestPortCounter.Add(1))
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	qs, err := quicserver.NewWithRetry(filepath.Join(dir, "wal"), addr, 5, quicserver.WithMasterKey(storetest.TestKey()))
	require.NoError(t, err)
	return qs, addr
}

// TestQuicServer_ConcurrentShardWrites_NoWedge validates that N concurrent
// PUT shard requests on a single pooled QUIC connection complete within a
// generous deadline. This is the narrow-scope reproducer for Bug C in
// docs/development/bugs/multipart-upload-deadlock.md — the WAL+QUIC head-of-
// line wedge. Required conditions for the wedge:
//   - One pooled QUIC connection shared by multiple streams.
//   - Per-stream bodies large enough in aggregate to saturate the connection-
//     level flow-control window (quic-go default 15 MiB).
//   - Server-side handlers that hold a global lock (wal.mu) while reading
//     from the stream.
//
// On a wedged server the test hangs until the 60s budget elapses. Post-fix
// (9dcd3dd drains the shard body into a memory buffer before taking wal.mu
// and 43e20f6 widens QUIC flow-control windows), the writes serialize
// through wal.mu in well under a second each. This is now a regression
// gate for those two fixes together.
func TestQuicServer_ConcurrentShardWrites_NoWedge(t *testing.T) {
	qs, addr := newTestQuicServer(t)
	defer qs.Close()

	// Explicitly use a single direct Dial (not DefaultPool) so we control the
	// connection-sharing topology — all streams fan out from this one
	// connection, which is the necessary condition for the wedge.
	dialCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := quicclient.Dial(dialCtx, addr)
	require.NoError(t, err)
	defer client.Close()

	// 16 MiB per shard exceeds quic-go's 15 MiB default connection window, so
	// 10 concurrent streams cannot all be in flight simultaneously — the
	// server's handling of the contended wal.mu determines whether progress
	// is made at all.
	const streams = 10
	const shardSize = 16 * 1024 * 1024

	errCh := make(chan error, streams)
	var wg sync.WaitGroup
	start := time.Now()

	for i := range streams {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Unique ObjectHash/ShardIndex per goroutine to avoid local
			// metadata key collisions on the server's badger DB.
			hash := sha256.Sum256(fmt.Appendf(nil, "wedge-test-%d", idx))
			body := bytes.Repeat([]byte{byte(idx)}, shardSize)

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			_, perr := client.Put(ctx, quicserver.PutRequest{
				Bucket:     "wedge",
				Object:     fmt.Sprintf("obj-%d", idx),
				ObjectHash: hash,
				ShardSize:  shardSize,
				ShardIndex: idx,
			}, bytes.NewReader(body))
			errCh <- perr
		}(i)
	}

	wg.Wait()
	close(errCh)
	elapsed := time.Since(start)

	for e := range errCh {
		require.NoError(t, e, "concurrent PUT shard failed")
	}

	// Deadlock detector, not a perf bound. 10 × 16 MiB serialized through a
	// local WAL on loopback should finish comfortably under 90 s; a wedged
	// path blows through this via the 60 s per-stream context deadline.
	require.Less(t, elapsed, 90*time.Second, "suspected WAL+QUIC wedge")
	t.Logf("10 concurrent shards (16 MiB each) completed in %v", elapsed)
}

// Fail fast if the accept loop dies without logging; tests above rely on the
// server actually serving connections.
func TestQuicServer_BasicPut(t *testing.T) {
	qs, addr := newTestQuicServer(t)
	defer qs.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := quicclient.Dial(ctx, addr)
	require.NoError(t, err)
	defer client.Close()

	body := []byte("hello wedge")
	hash := sha256.Sum256([]byte("basic-put"))
	resp, err := client.Put(ctx, quicserver.PutRequest{
		Bucket:     "basic",
		Object:     "obj",
		ObjectHash: hash,
		ShardSize:  len(body),
		ShardIndex: 0,
	}, io.NopCloser(bytes.NewReader(body)))
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), resp.ShardSize)

	// Sanity: segment file was created on disk.
	entries, _ := os.ReadDir(qs.WalDir)
	require.NotEmpty(t, entries)
}
