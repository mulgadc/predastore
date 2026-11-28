package blob_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/stretchr/testify/require"
)

// BenchmarkBlobStat is the question repair asks of every position it owns,
// from one caller and from one caller for each CPU.
func BenchmarkBlobStat(b *testing.B) {
	b.Run("callers=1", func(b *testing.B) {
		c := startBenchNode(b)
		putCommitted(b, c, 0, bytes.Repeat([]byte("s"), 1<<10))
		req := blob.StatRequest{Key: benchKey(), Index: 0}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, err := c.Stat(context.Background(), benchNode, req)
			require.NoError(b, err)
		}
	})
	b.Run("callers=gomaxprocs", func(b *testing.B) {
		c := startBenchNode(b)
		putCommitted(b, c, 0, bytes.Repeat([]byte("s"), 1<<10))
		req := blob.StatRequest{Key: benchKey(), Index: 0}
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := c.Stat(context.Background(), benchNode, req); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}

// BenchmarkBlobCommitSameEpoch writes one shard again at the generation it
// already holds, as a retried write or a repair does.
func BenchmarkBlobCommitSameEpoch(b *testing.B) {
	c := startBenchNode(b)
	body := bytes.Repeat([]byte("e"), 1<<20)
	epoch := putCommitted(b, c, 0, body)
	ctx := context.Background()
	b.SetBytes(1 << 20)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, err := c.Put(ctx, benchNode, blob.PutRequest{Key: benchKey(), Index: 0, Size: int64(len(body)), Epoch: epoch}, bytes.NewReader(body))
		require.NoError(b, err)
		_, err = c.Commit(ctx, benchNode, blob.CommitRequest{Key: benchKey(), Index: 0, Epoch: epoch})
		require.NoError(b, err)
	}
}
