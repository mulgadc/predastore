package quicserver_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/mulgadc/predastore/quic/quicclient"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/stretchr/testify/require"
)

// newFullTestQuicServer is newTestQuicServer (wedge_test.go) with the free-
// space watermark forced unsatisfiable, so every Append on this node's store
// fails with store.ErrStoreFull.
func newFullTestQuicServer(t *testing.T) (*quicserver.QuicServer, string) {
	t.Helper()
	dir := t.TempDir()
	port := 46000 + int(quicServerTestPortCounter.Add(1))
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	certPath, keyPath, pool := testcerts.Generate(t)
	quicclient.SetDefaultRootCAs(pool)
	t.Cleanup(func() { quicclient.SetDefaultRootCAs(nil) })
	qs, err := quicserver.NewWithRetry(filepath.Join(dir, "wal"), addr, 5,
		quicserver.WithMasterKey(storetest.TestKey()),
		quicserver.WithTLSCertFiles(certPath, keyPath),
		quicserver.WithFreeSpaceWatermark(0.9999, 0.9999),
	)
	require.NoError(t, err)
	return qs, addr
}

// TestHandlePUTShardDrainsBodyBeforeRejectingOverQUIC is a regression test
// for a bug where handlePUTShard replied with an error status without first
// draining the client's in-flight body. Returning early left the QUIC stream
// un-drained; when handleStream's deferred cleanup then called
// s.CancelRead(0), a client still blocked writing the body saw the stream
// reset instead of the response — the 507 out-of-space status never arrived.
//
// The body here (4 MiB) exceeds quicconf.InitialStreamReceiveWindow (2 MiB)
// and the server never reads application-level bytes from a rejected PUT, so
// the client's write blocks on flow control until the server actually drains
// it. That only happens post-fix, which is what makes this reproduce the bug
// without the drain: without it, client.Put fails with a transport-level
// stream error instead of wrapping quicclient.ErrInsufficientStorage.
func TestHandlePUTShardDrainsBodyBeforeRejectingOverQUIC(t *testing.T) {
	qs, addr := newFullTestQuicServer(t)
	defer qs.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := quicclient.Dial(ctx, addr)
	require.NoError(t, err)
	defer client.Close()

	const bodySize = 4 * 1024 * 1024
	body := bytes.Repeat([]byte{0x7}, bodySize)
	hash := sha256.Sum256([]byte("full-store-drain-regression"))

	_, err = client.Put(ctx, quicserver.PutRequest{
		Bucket:     "full",
		Object:     "obj",
		ObjectHash: hash,
		ShardSize:  bodySize,
		ShardIndex: 0,
	}, bytes.NewReader(body))

	require.Error(t, err, "PUT against a full store must fail")
	require.ErrorIs(t, err, quicclient.ErrInsufficientStorage,
		"client must observe StatusInsufficientStorage (507), not a stream reset/cancellation")
}
