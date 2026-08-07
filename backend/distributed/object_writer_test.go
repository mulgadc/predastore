package distributed

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/mulgadc/predastore/s3db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memoryShardClient struct {
	mu     sync.Mutex
	shards map[uint32][]byte
}

func (c *memoryShardClient) PutShard(_ context.Context, _ int, req quicserver.PutRequest, body io.Reader) (*quicserver.PutResponse, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shards[req.ShardIndex] = append([]byte(nil), data...)
	return &quicserver.PutResponse{}, nil
}

func (*memoryShardClient) GetShard(context.Context, int, quicserver.ObjectRequest) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

func (*memoryShardClient) GetShardRange(context.Context, int, quicserver.ObjectRequest) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

func (*memoryShardClient) DeleteShard(context.Context, int, quicserver.DeleteRequest) (*quicserver.DeleteResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func newObjectWriterTestBackend(t testing.TB, dataShards, parityShards int) (*Backend, *memoryShardClient) {
	t.Helper()
	client := &memoryShardClient{shards: make(map[uint32][]byte)}
	raw, err := New(&Config{
		BadgerDir:      t.TempDir(),
		DataShards:     dataShards,
		ParityShards:   parityShards,
		PartitionCount: dataShards + parityShards,
		ShardClient:    client,
	})
	require.NoError(t, err)
	backend := raw.(*Backend)
	t.Cleanup(func() { require.NoError(t, backend.Close()) })
	return backend, client
}

func TestPutObjectViaQUICSingleShardStreamsExactBody(t *testing.T) {
	backend, client := newObjectWriterTestBackend(t, 1, 0)
	data := bytes.Repeat([]byte("single-pass"), 1000)

	_, err := backend.putObjectViaQUIC(context.Background(), "bucket", "key",
		bytes.NewReader(data), int64(len(data)), s3db.GenObjectHash("bucket", "key"))
	require.NoError(t, err)
	assert.Equal(t, map[uint32][]byte{0: data}, client.shards)
}

func TestPutObjectViaQUICReedSolomonLayout(t *testing.T) {
	backend, client := newObjectWriterTestBackend(t, 3, 1)
	data := bytes.Repeat([]byte("range-friendly-data"), 1001)

	_, err := backend.putObjectViaQUIC(context.Background(), "bucket", "key",
		bytes.NewReader(data), int64(len(data)), s3db.GenObjectHash("bucket", "key"))
	require.NoError(t, err)

	shards := make([][]byte, 4)
	for i := range shards {
		shards[i] = client.shards[uint32(i)]
	}
	encoder, err := reedsolomon.New(3, 1)
	require.NoError(t, err)
	ok, err := encoder.Verify(shards)
	require.NoError(t, err)
	assert.True(t, ok)

	var restored bytes.Buffer
	require.NoError(t, encoder.Join(&restored, shards, len(data)))
	assert.Equal(t, data, restored.Bytes())
}

func TestPutObjectViaQUICRejectsLengthMismatch(t *testing.T) {
	backend, _ := newObjectWriterTestBackend(t, 1, 0)
	hash := s3db.GenObjectHash("bucket", "key")

	_, err := backend.putObjectViaQUIC(context.Background(), "bucket", "key", bytes.NewReader([]byte("abc")), 4, hash)
	assert.ErrorIs(t, err, errObjectBodyShort)

	_, err = backend.putObjectViaQUIC(context.Background(), "bucket", "key", bytes.NewReader([]byte("abcde")), 4, hash)
	assert.ErrorIs(t, err, errObjectBodyLong)
}

func BenchmarkPutObjectViaQUIC(b *testing.B) {
	for _, layout := range []struct {
		name   string
		data   int
		parity int
	}{
		{"RS1_0", 1, 0},
		{"RS3_1", 3, 1},
	} {
		b.Run(layout.name, func(b *testing.B) {
			backend, client := newObjectWriterTestBackend(b, layout.data, layout.parity)
			payload := bytes.Repeat([]byte{0x5a}, 1<<20)
			hash := s3db.GenObjectHash("bucket", "benchmark")
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				client.shards = make(map[uint32][]byte)
				if _, err := backend.putObjectViaQUIC(context.Background(), "bucket", "benchmark",
					bytes.NewReader(payload), int64(len(payload)), hash); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
