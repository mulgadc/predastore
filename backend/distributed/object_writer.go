package distributed

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/quic/quicserver"
	"golang.org/x/sync/errgroup"
)

var (
	errObjectBodyShort = errors.New("object body shorter than declared content length")
	errObjectBodyLong  = errors.New("object body longer than declared content length")
)

// putObjectViaQUIC consumes exactly size bytes from body, encodes them using
// the configured RS layout, and writes every shard through the injected shard
// client. Despite the historical name, the client may use an in-process
// transport; transport details remain outside this front-door helper.
func (b *Backend) putObjectViaQUIC(
	ctx context.Context,
	bucket string,
	object string,
	body io.Reader,
	size int64,
	objectHash [32]byte,
) (poolNearFull bool, err error) {
	if size < 0 {
		return false, fmt.Errorf("invalid object size %d", size)
	}
	if body == nil {
		body = bytes.NewReader(nil)
	}

	totalShards := b.rsDataShard + b.rsParityShard
	hashRingShards, err := b.hashRing.GetClosestN(objectHash[:], totalShards)
	if err != nil {
		return false, fmt.Errorf("select shard nodes: %w", err)
	}

	// Empty objects have no physical shard because the current shard protocol
	// requires a positive body length. Their authoritative zero size is stored
	// in object metadata and GET returns an empty stream without a shard RPC.
	if size == 0 {
		if err := requireReaderEOF(body); err != nil {
			return false, err
		}
		return false, nil
	}

	if b.rsDataShard == 1 && b.rsParityShard == 0 {
		return b.putSingleShard(ctx, bucket, object, body, size, objectHash, hashRingShards[0].String())
	}

	shards, err := b.encodeObject(body, size)
	if err != nil {
		return false, err
	}

	type shardResult struct {
		nearFull bool
	}
	results := make([]shardResult, len(shards))
	group, groupCtx := errgroup.WithContext(ctx)
	for i := range shards {
		idx := i
		shardData := shards[i]
		group.Go(func() error {
			nodeNum, err := NodeToUint32(hashRingShards[idx].String())
			if err != nil {
				return fmt.Errorf("resolve node for shard %d: %w", idx, err)
			}
			putReq := quicserver.PutRequest{
				Bucket:     bucket,
				Object:     object,
				ObjectHash: objectHash,
				ShardSize:  len(shardData),
				ShardIndex: uint32(idx), //nolint:gosec // idx is bounded by configured shard count.
			}
			resp, err := b.shards.PutShard(groupCtx, int(nodeNum), putReq, bytes.NewReader(shardData))
			if err != nil {
				return fmt.Errorf("write shard %d to node %d: %w", idx, nodeNum, err)
			}
			if resp == nil {
				return fmt.Errorf("write shard %d to node %d: nil response", idx, nodeNum)
			}
			results[idx].nearFull = resp.PoolNearFull
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return false, err
	}
	for _, result := range results {
		poolNearFull = poolNearFull || result.nearFull
	}
	return poolNearFull, nil
}

func (b *Backend) putSingleShard(
	ctx context.Context,
	bucket string,
	object string,
	body io.Reader,
	size int64,
	objectHash [32]byte,
	nodeName string,
) (bool, error) {
	maxInt := int64(^uint(0) >> 1)
	if size > maxInt {
		return false, fmt.Errorf("object size %d exceeds platform shard limit %d", size, maxInt)
	}
	nodeNum, err := NodeToUint32(nodeName)
	if err != nil {
		return false, fmt.Errorf("resolve single-shard node: %w", err)
	}

	limited := &io.LimitedReader{R: body, N: size}
	resp, err := b.shards.PutShard(ctx, int(nodeNum), quicserver.PutRequest{
		Bucket:     bucket,
		Object:     object,
		ObjectHash: objectHash,
		ShardSize:  int(size),
		ShardIndex: 0,
	}, limited)
	if err != nil {
		return false, fmt.Errorf("write single shard to node %d: %w", nodeNum, err)
	}
	if limited.N != 0 {
		return false, fmt.Errorf("%w: missing %d bytes", errObjectBodyShort, limited.N)
	}
	if err := requireReaderEOF(body); err != nil {
		return false, err
	}
	if resp == nil {
		return false, fmt.Errorf("write single shard to node %d: nil response", nodeNum)
	}
	return resp.PoolNearFull, nil
}

func (b *Backend) encodeObject(body io.Reader, size int64) ([][]byte, error) {
	dataShards := int64(b.rsDataShard)
	shardSize := size / dataShards
	if size%dataShards != 0 {
		shardSize++
	}
	maxInt := int64(^uint(0) >> 1)
	if shardSize > maxInt || shardSize > maxInt/dataShards {
		return nil, fmt.Errorf("encoded object size overflows platform int")
	}
	paddedSize := shardSize * dataShards
	dataBacking := make([]byte, int(paddedSize))

	n, err := io.ReadFull(body, dataBacking[:int(size)])
	if err != nil {
		return nil, fmt.Errorf("%w: read %d of %d bytes: %w", errObjectBodyShort, n, size, err)
	}
	if err := requireReaderEOF(body); err != nil {
		return nil, err
	}

	shards := make([][]byte, b.rsDataShard+b.rsParityShard)
	for i := range b.rsDataShard {
		start := int64(i) * shardSize
		shards[i] = dataBacking[int(start):int(start+shardSize)]
	}
	for i := range b.rsParityShard {
		shards[b.rsDataShard+i] = make([]byte, int(shardSize))
	}

	encoder, err := reedsolomon.New(b.rsDataShard, b.rsParityShard)
	if err != nil {
		return nil, fmt.Errorf("create Reed-Solomon encoder: %w", err)
	}
	if err := encoder.Encode(shards); err != nil {
		return nil, fmt.Errorf("encode Reed-Solomon shards: %w", err)
	}
	return shards, nil
}

func requireReaderEOF(body io.Reader) error {
	var extra [1]byte
	n, err := body.Read(extra[:])
	if n > 0 || err == nil {
		return errObjectBodyLong
	}
	if !errors.Is(err, io.EOF) {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return fmt.Errorf("%w: limit %d", errObjectBodyLong, maxBytesErr.Limit)
		}
		return fmt.Errorf("read object body terminator: %w", err)
	}
	return nil
}
