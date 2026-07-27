package storage_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/storage"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/mulgadc/predastore/s3db"
	"github.com/mulgadc/predastore/store"
)

// startStorageProc hosts one storage node behind an rpc server on a pipe
// endpoint and returns a client reaching it.
func startStorageProc(t *testing.T, nodeID int, pipeName string) *storage.Client {
	t.Helper()

	st, err := store.Open(t.TempDir(), store.WithAEAD(storetest.TestAEAD()))
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	svc := storage.NewService()
	svc.AddNode(uint64(nodeID), st)
	mux := rpc.NewMux()
	svc.Register(mux)

	srv, err := rpc.NewServer(rpc.ServerConfig{
		Mux:          mux,
		Transports:   []transport.Transport{transport.NewPipeTransport(pipeName)},
		DrainTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srvCtx, srvCancel := context.WithCancel(context.Background())
	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		srv.Run(srvCtx)
	}()
	t.Cleanup(func() {
		srvCancel()
		<-srvDone
	})

	rpcClient := rpc.NewClient(rpc.ClientConfig{
		Transports: []transport.Transport{transport.NewPipeTransport(pipeName + "-client")},
	})
	cli, err := storage.NewClient(storage.ClientConfig{
		Client: rpcClient,
		Resolve: func(id int) (net.Addr, error) {
			if id != nodeID {
				return nil, fmt.Errorf("unknown storage node %d", id)
			}
			return transport.ResolveAddr(string(transport.NetworkPipe), pipeName)
		},
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return cli
}

func TestStorageShardRoundTrip(t *testing.T) {
	cli := startStorageProc(t, 7, "storage-svc-roundtrip")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const bucket, object = "bkt", "obj/a"
	shard := make([]byte, 256*1024)
	if _, err := rand.Read(shard); err != nil {
		t.Fatalf("rand: %v", err)
	}

	putResp, err := cli.PutShard(ctx, 7, quicserver.PutRequest{
		Bucket:     bucket,
		Object:     object,
		ObjectHash: s3db.GenObjectHash(bucket, object),
		ShardSize:  len(shard),
		ShardIndex: 0,
	}, bytes.NewReader(shard))
	if err != nil {
		t.Fatalf("PutShard: %v", err)
	}
	if putResp.ShardSize != int64(len(shard)) {
		t.Fatalf("committed %d bytes, want %d", putResp.ShardSize, len(shard))
	}

	// Full read.
	rc, err := cli.GetShard(ctx, 7, quicserver.ObjectRequest{
		Bucket: bucket, Object: object, ShardIndex: 0, RangeStart: -1, RangeEnd: -1,
	})
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("read shard: %v", err)
	}
	if !bytes.Equal(got, shard) {
		t.Fatalf("shard mismatch: got %d bytes", len(got))
	}

	// Range read, including an explicit zero start.
	rc, err = cli.GetShardRange(ctx, 7, quicserver.ObjectRequest{
		Bucket: bucket, Object: object, ShardIndex: 0, RangeStart: 0, RangeEnd: 1023,
	})
	if err != nil {
		t.Fatalf("GetShardRange: %v", err)
	}
	got, err = io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("read range: %v", err)
	}
	if !bytes.Equal(got, shard[:1024]) {
		t.Fatalf("range mismatch: got %d bytes", len(got))
	}

	// Delete, then the shard is gone.
	delResp, err := cli.DeleteShard(ctx, 7, quicserver.DeleteRequest{
		Bucket: bucket, Object: object,
		ObjectHash: s3db.GenObjectHash(bucket, object), ShardIndex: 0,
	})
	if err != nil {
		t.Fatalf("DeleteShard: %v", err)
	}
	if !delResp.Deleted {
		t.Fatal("DeleteShard reported nothing deleted")
	}
	if _, err := cli.GetShard(ctx, 7, quicserver.ObjectRequest{
		Bucket: bucket, Object: object, ShardIndex: 0, RangeStart: -1, RangeEnd: -1,
	}); !errors.Is(err, storage.ErrShardNotFound) {
		t.Fatalf("GetShard after delete: got %v, want ErrShardNotFound", err)
	}
}

func TestStorageGetMissingShard(t *testing.T) {
	cli := startStorageProc(t, 1, "storage-svc-missing")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := cli.GetShard(ctx, 1, quicserver.ObjectRequest{
		Bucket: "none", Object: "missing", ShardIndex: 0, RangeStart: -1, RangeEnd: -1,
	}); !errors.Is(err, storage.ErrShardNotFound) {
		t.Fatalf("got %v, want ErrShardNotFound", err)
	}
}

func TestStorageUnknownTargetNode(t *testing.T) {
	cli := startStorageProc(t, 1, "storage-svc-unknown")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := cli.DeleteShard(ctx, 99, quicserver.DeleteRequest{
		Bucket: "b", Object: "o", ShardIndex: 0,
	}); err == nil {
		t.Fatal("delete against unknown node succeeded")
	}
}
