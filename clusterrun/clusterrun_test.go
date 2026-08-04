package clusterrun

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/internal/cluster"
	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/predastore/s3"
)

// testMasterKey writes a fresh 32-byte key to disk and loads it the way the
// entrypoint does.
func testMasterKey(t *testing.T) *masterkey.Key {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("rand: %v", err)
	}
	path := filepath.Join(t.TempDir(), "master.key")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	key, err := masterkey.Load(path)
	if err != nil {
		t.Fatalf("masterkey.Load: %v", err)
	}
	return key
}

// TestClusterRuntimeObjectRoundTrip runs a whole cluster — three storage
// nodes and one state replica — in one process over pipe streams, with no
// network sockets and no certs, and drives S3 object operations through the
// prepared backend. Each node has its own service and rpc server, so this
// also covers several servers coexisting on the shared pipe registry.
func TestClusterRuntimeObjectRoundTrip(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &s3.Config{
		RS: s3.RS{Data: 2, Parity: 1},
		Hosts: []cluster.Host{
			{ID: 1, BindAddr: "127.0.0.1:16660", PublicAddr: "127.0.0.1:16660", DataDir: dataDir},
		},
		ClusterNodes: []cluster.Node{
			{ID: 1, HostID: 1, Role: cluster.RoleShardStorage},
			{ID: 2, HostID: 1, Role: cluster.RoleShardStorage},
			{ID: 3, HostID: 1, Role: cluster.RoleShardStorage},
			{ID: 4, HostID: 1, Role: cluster.RoleStateReplica},
		},
	}

	// No certs: every node is local, so no network socket opens.
	rt, err := Build(cfg, []int{1, 2, 3, 4}, "", "", testMasterKey(t))
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	// Serve the nodes for the duration of the test, then drain them the way
	// a signal would.
	runCtx, stopRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- rt.Run(runCtx) }()
	t.Cleanup(func() {
		stopRun()
		select {
		case <-runDone:
		case <-time.After(30 * time.Second):
			t.Error("runtime did not drain")
		}
	})

	if err := rt.WaitReady(30 * time.Second); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	be := rt.Backend

	if _, err := be.CreateBucket(ctx, &backend.CreateBucketRequest{
		Bucket:    "it-bucket",
		Region:    "ap-southeast-2",
		OwnerID:   "AKIAIOSFODNN7EXAMPLE",
		AccountID: "123456789012",
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	payload := make([]byte, 1<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if _, err := be.PutObject(ctx, &backend.PutObjectRequest{
		Bucket:        "it-bucket",
		Key:           "dir/blob.bin",
		Body:          bytes.NewReader(payload),
		ContentLength: int64(len(payload)),
		ContentType:   "application/octet-stream",
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	resp, err := be.GetObject(ctx, &backend.GetObjectRequest{
		Bucket: "it-bucket", Key: "dir/blob.bin", RangeStart: -1, RangeEnd: -1,
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("object mismatch: got %d bytes, want %d", len(got), len(payload))
	}

	// Range read exercises the single-shard fast path over rpc.
	resp, err = be.GetObject(ctx, &backend.GetObjectRequest{
		Bucket: "it-bucket", Key: "dir/blob.bin", RangeStart: 100, RangeEnd: 1123,
	})
	if err != nil {
		t.Fatalf("GetObject range: %v", err)
	}
	got, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read range: %v", err)
	}
	if !bytes.Equal(got, payload[100:1124]) {
		t.Fatalf("range mismatch: got %d bytes", len(got))
	}

	if err := be.DeleteObject(ctx, &backend.DeleteObjectRequest{
		Bucket: "it-bucket", Key: "dir/blob.bin",
	}); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if _, err := be.GetObject(ctx, &backend.GetObjectRequest{
		Bucket: "it-bucket", Key: "dir/blob.bin", RangeStart: -1, RangeEnd: -1,
	}); err == nil {
		t.Fatal("GetObject after delete succeeded")
	}
}

// TestRelativeDataDirUsesBasePath pins the launcher contract: a config with a
// relative data_dir must land under -base-path, not the working directory.
// Getting this wrong writes cluster state into the repo.
func TestRelativeDataDirUsesBasePath(t *testing.T) {
	base := t.TempDir()
	cfg := &s3.Config{
		BasePath: base,
		RS:       s3.RS{Data: 2, Parity: 1},
		Hosts: []cluster.Host{
			{ID: 1, BindAddr: "127.0.0.1:16661", PublicAddr: "127.0.0.1:16661", DataDir: "data/host-1"},
		},
		ClusterNodes: []cluster.Node{
			{ID: 1, HostID: 1, Role: cluster.RoleShardStorage},
			{ID: 2, HostID: 1, Role: cluster.RoleShardStorage},
			{ID: 3, HostID: 1, Role: cluster.RoleStateReplica},
		},
	}

	rt, err := Build(cfg, []int{1, 2, 3}, "", "", testMasterKey(t))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer rt.Close()

	if _, err := os.Stat(filepath.Join(base, "data", "host-1", "node-1")); err != nil {
		t.Fatalf("shard store not under base path: %v", err)
	}
	if _, err := os.Stat(filepath.Join("data", "host-1")); err == nil {
		t.Fatal("relative data_dir leaked into the working directory")
	}
}
