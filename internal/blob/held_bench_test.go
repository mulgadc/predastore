package blob_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/stretchr/testify/require"
)

// putHeld writes one held shard without committing it and returns its epoch.
func putHeld(b *testing.B, c *blob.Client, index uint32, body []byte) uint64 {
	epoch := benchEpochs.Add(1)
	ctx := context.Background()
	p, err := c.OpenHeld(ctx, benchNode, blob.PutRequest{
		Key: benchKey(), Index: index, Size: int64(len(body)), Epoch: epoch,
	})
	require.NoError(b, err)
	resp, err := p.Send(ctx, bytes.NewReader(body))
	require.NoError(b, err)
	require.True(b, resp.Holding)
	return epoch
}

// BenchmarkBlobPutHeld is the conditional shard write: a held put, which
// waits for Registered before its body, then the commit.
func BenchmarkBlobPutHeld(b *testing.B) {
	for _, sz := range benchSizes {
		b.Run(sz.name, func(b *testing.B) {
			c := startBenchNode(b)
			body := bytes.Repeat([]byte("h"), sz.size)
			ctx := context.Background()
			b.SetBytes(int64(sz.size))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				epoch := putHeld(b, c, 0, body)
				_, err := c.Commit(ctx, benchNode, blob.CommitRequest{Key: benchKey(), Index: 0, Epoch: epoch})
				require.NoError(b, err)
			}
		})
	}
}

// BenchmarkBlobGetRollForward reads a new position whose shard is still
// prepared, so the get commits it first. Each read takes a fresh position.
func BenchmarkBlobGetRollForward(b *testing.B) {
	c := startBenchNode(b)
	body := bytes.Repeat([]byte("r"), 1<<20)
	b.SetBytes(1 << 20)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		b.StopTimer()
		index := uint32(i)
		epoch := putHeld(b, c, index, body)
		b.StartTimer()
		getShard(b, c, index, epoch)
	}
}
