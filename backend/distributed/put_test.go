package distributed

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	s3backend "github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/mulgadc/predastore/quic/quicclient"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/stretchr/testify/require"
)

// mapPutErr is the seam between a QUIC shard-write failure and the S3
// response a client sees. It must preserve the pool-full signal instead of
// collapsing every putObjectViaQUIC failure into the same generic 500.
func TestMapPutErrInsufficientStorage(t *testing.T) {
	err := fmt.Errorf("put failed: %w", quicclient.ErrInsufficientStorage)

	s3err := mapPutErr(err)

	if s3err.StatusCode != http.StatusInsufficientStorage {
		t.Fatalf("StatusCode = %d, want 507", s3err.StatusCode)
	}
	if s3err.Code != s3backend.ErrInsufficientStorage {
		t.Fatalf("Code = %s, want %s", s3err.Code, s3backend.ErrInsufficientStorage)
	}
}

func TestMapPutErrOtherErrorStaysInternalError(t *testing.T) {
	s3err := mapPutErr(errors.New("dial failed: connection refused"))

	if s3err.StatusCode != http.StatusInternalServerError {
		t.Fatalf("StatusCode = %d, want 500", s3err.StatusCode)
	}
	if s3err.Code != s3backend.ErrInternalError {
		t.Fatalf("Code = %s, want %s", s3err.Code, s3backend.ErrInternalError)
	}
}

// TestPutObjectFullPoolReturns507EndToEnd drives the real seam: five live
// QuicServer nodes, each opened with an unsatisfiable free-space watermark
// (0.9999 free-space fraction, above any real disk), so every shard PUT hits
// store.ErrStoreFull. That crosses handlePUTShard (quicproto.StatusInsufficientStorage),
// quicclient.Put (ErrInsufficientStorage), and mapPutErr (backend.S3Error),
// landing on Backend.PutObject as a real 507 — the same status handleError
// forwards verbatim to the HTTP client.
func TestPutObjectFullPoolReturns507EndToEnd(t *testing.T) {
	const bucket = "test-bucket-full"

	tmpDir := t.TempDir()
	testBasePort := 29981

	cfg := &Config{
		BadgerDir:      tmpDir,
		PartitionCount: 5,
		QuicBasePort:   testBasePort,
		Buckets: []BucketConfig{
			{Name: bucket, Region: "us-east-1"},
		},
	}
	b, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, b)
	defer b.Close()

	backend, ok := b.(*Backend)
	require.True(t, ok)

	dataDir := filepath.Join(tmpDir, "nodes")
	backend.SetDataDir(dataDir)

	certPath, keyPath, pool := testcerts.Generate(t)
	quicclient.SetDefaultRootCAs(pool)
	t.Cleanup(func() { quicclient.SetDefaultRootCAs(nil) })

	// Every node's store is opened full: whichever nodes the hash ring picks
	// for this object's data/parity shards, the write is rejected.
	quicServers := make([]*quicserver.QuicServer, 5)
	for i := range 5 {
		nodeDir := filepath.Join(dataDir, fmt.Sprintf("node-%d", i))
		require.NoError(t, os.MkdirAll(nodeDir, 0750))

		qs, err := quicserver.NewWithRetry(nodeDir, fmt.Sprintf("127.0.0.1:%d", testBasePort+i), 5,
			quicserver.WithMasterKey(storetest.TestKey()),
			quicserver.WithTLSCertFiles(certPath, keyPath),
			quicserver.WithFreeSpaceWatermark(0.9999, 0.9999),
		)
		require.NoError(t, err, "failed to start QUIC server for node %d", i)
		quicServers[i] = qs
	}
	defer func() {
		for _, qs := range quicServers {
			if qs != nil {
				_ = qs.Close()
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)

	ctx := context.Background()
	putReq := &s3backend.PutObjectRequest{
		Bucket: bucket,
		Key:    "full-pool-object",
		Body:   bytes.NewReader(bytes.Repeat([]byte{0x1}, 4096)),
	}

	_, err = backend.PutObject(ctx, putReq)
	require.Error(t, err, "PutObject against a full pool must fail")

	s3err, ok := s3backend.IsS3Error(err)
	require.True(t, ok, "error is not a *backend.S3Error: %v", err)
	require.Equal(t, 507, s3err.StatusCode, "PutObject against a full pool must surface 507")
	require.Equal(t, s3backend.ErrInsufficientStorage, s3err.Code)
}
