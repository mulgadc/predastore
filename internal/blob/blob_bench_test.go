package blob_test

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/require"
)

const (
	benchHost       = "bench-test-host"
	benchServerPort = 6491
	benchClientPort = 6492
	benchNode       = config.NodeID(1)
)

var benchSizes = []struct {
	name string
	size int
}{
	{name: "1KiB", size: 1 << 10},
	{name: "1MiB", size: 1 << 20},
	{name: "64MiB", size: 64 << 20},
}

var benchEpochs atomic.Uint64

// startBenchNode runs a real blob node over a real engine store, so a
// benchmark measures the server and the store and not a fake.
func startBenchNode(b *testing.B) *blob.Client {
	b.Helper()

	clusterCfg := &config.Config{
		Hosts: []config.Host{{
			ID:   1,
			Addr: benchHost,
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleBlob, Port: benchServerPort},
				{ID: 2, Role: config.RoleBlob, Port: benchClientPort},
			},
		}},
	}

	serverTr := transport.NewPipeTransport(benchHost, benchServerPort)
	b.Cleanup(func() { serverTr.Close() })
	ln, err := serverTr.Listen()
	require.NoError(b, err)
	srv, err := blob.New(blob.Config{
		NodeID:    benchNode,
		DataDir:   b.TempDir(),
		AEAD:      storetest.TestAEAD(),
		Listeners: []transport.Listener{ln},
	})
	require.NoError(b, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	b.Cleanup(func() {
		cancel()
		<-done
	})

	clientTr := transport.NewPipeTransport(benchHost, benchClientPort)
	b.Cleanup(func() { clientTr.Close() })
	clientRes, err := rpc.NewResolver(clusterCfg, 2, clientTr)
	require.NoError(b, err)
	client, err := blob.NewClient(blob.ClientConfig{
		Client:          rpc.NewClient(rpc.NewConnPool(2, clientRes)),
		EnvelopeTimeout: 10 * time.Second,
		IdleTimeout:     10 * time.Second,
	})
	require.NoError(b, err)

	return client
}

func benchKey() [32]byte { return [32]byte{0x5b} }

// putCommitted writes and commits one shard and returns its epoch.
func putCommitted(b *testing.B, c *blob.Client, index uint32, body []byte) uint64 {
	epoch := benchEpochs.Add(1)
	ctx := context.Background()
	_, err := c.Put(ctx, benchNode, blob.PutRequest{
		Key: benchKey(), Index: index, Size: int64(len(body)), Epoch: epoch,
	}, bytes.NewReader(body))
	require.NoError(b, err)
	_, err = c.Commit(ctx, benchNode, blob.CommitRequest{Key: benchKey(), Index: index, Epoch: epoch})
	require.NoError(b, err)
	return epoch
}

func getShard(b *testing.B, c *blob.Client, index uint32, epoch uint64) {
	r, err := c.Get(context.Background(), benchNode, blob.GetRequest{
		Key: benchKey(), Index: index, RangeStart: -1, RangeEnd: -1, Epoch: epoch,
	})
	require.NoError(b, err)
	_, err = io.Copy(io.Discard, r)
	require.NoError(b, err)
	require.NoError(b, r.Close())
}

// BenchmarkBlobPut is the unconditional shard write: put, then commit.
func BenchmarkBlobPut(b *testing.B) {
	for _, sz := range benchSizes {
		b.Run(sz.name, func(b *testing.B) {
			c := startBenchNode(b)
			body := bytes.Repeat([]byte("p"), sz.size)
			b.SetBytes(int64(sz.size))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				putCommitted(b, c, 0, body)
			}
		})
	}
}

// BenchmarkBlobPutConcurrent is the same write from many callers at once,
// each on its own position.
func BenchmarkBlobPutConcurrent(b *testing.B) {
	c := startBenchNode(b)
	body := bytes.Repeat([]byte("p"), 1<<20)
	var next atomic.Uint32
	b.SetBytes(1 << 20)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		index := next.Add(1)
		for pb.Next() {
			putCommitted(b, c, index, body)
		}
	})
}

// BenchmarkBlobGet reads a committed shard at the epoch a record names.
func BenchmarkBlobGet(b *testing.B) {
	for _, sz := range benchSizes {
		b.Run(sz.name, func(b *testing.B) {
			c := startBenchNode(b)
			epoch := putCommitted(b, c, 0, bytes.Repeat([]byte("g"), sz.size))
			b.SetBytes(int64(sz.size))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				getShard(b, c, 0, epoch)
			}
		})
	}
}

// BenchmarkBlobGetConcurrent is the same read from many callers at once.
func BenchmarkBlobGetConcurrent(b *testing.B) {
	c := startBenchNode(b)
	epoch := putCommitted(b, c, 0, bytes.Repeat([]byte("g"), 1<<20))
	b.SetBytes(1 << 20)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			getShard(b, c, 0, epoch)
		}
	})
}

// BenchmarkBlobDelete times one shard delete of a committed 1 KiB shard, with
// the unscoped and the epoch-scoped opcode. The put that sets up each delete
// runs with the timer stopped.
func BenchmarkBlobDelete(b *testing.B) {
	body := bytes.Repeat([]byte{0x6d}, 1<<10)
	ctx := context.Background()
	b.Run("Unscoped", func(b *testing.B) {
		c := startBenchNode(b)
		for b.Loop() {
			b.StopTimer()
			putCommitted(b, c, 0, body)
			b.StartTimer()
			_, err := c.Delete(ctx, benchNode, blob.DeleteRequest{Key: benchKey(), Index: 0})
			require.NoError(b, err)
		}
	})
	b.Run("Epoch", func(b *testing.B) {
		c := startBenchNode(b)
		for b.Loop() {
			b.StopTimer()
			epoch := putCommitted(b, c, 0, body)
			b.StartTimer()
			_, err := c.DeleteEpoch(ctx, benchNode, blob.CommitRequest{Key: benchKey(), Index: 0, Epoch: epoch})
			require.NoError(b, err)
		}
	})
}
