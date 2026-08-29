package handlers

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/require"
)

// BenchmarkGetReadPath measures a whole-object GET the way serveObject runs
// one: open the stripe reader, then stream to the writer. The destination is
// io.Discard rather than a buffer, because a buffer the size of the object
// would be the allocation this measures.
//
// The healthy case is the one to watch. It opens k shards, so the parity
// buffers a reader used to take up front are pure waste in it.
func BenchmarkGetReadPath(b *testing.B) {
	sizes := []struct {
		name string
		size int64
	}{
		{name: "64KiB", size: 64 << 10},
		{name: "1MiB", size: 1 << 20},
		{name: "8MiB", size: 8 << 20},
		{name: "32MiB", size: 32 << 20},
	}

	for _, sz := range sizes {
		b.Run(sz.name, func(b *testing.B) {
			f := newWriteFixture(2, 1)
			ctx := context.Background()
			objectHash := model.ObjectHash("bucket", "object.txt")

			body := bytes.Repeat([]byte("x"), int(sz.size))
			place, _, err := f.write(ctx, objectHash, bytes.NewReader(body), sz.size)
			require.NoError(b, err)

			b.SetBytes(sz.size)
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				r, err := newStripeReader(ctx, f.bc, f.cfg, objectHash, place, 0)
				require.NoError(b, err)
				require.NoError(b, pipeObject(ctx, r, io.Discard, sz.size))
				r.close(ctx)
			}
		})
	}
}
