package handlers

import "sync"

// blockPool hands out the shard buffers both stream paths work in. They are the
// largest allocation either path makes — (k+m) of them per request — and they
// are all one constant size, so a single pool serves both and no size class is
// needed.
//
// A *[]byte is pooled rather than a []byte because Put takes an any: boxing a
// slice header allocates the very thing the pool exists to avoid.
var blockPool = sync.Pool{
	New: func() any {
		b := make([]byte, streamBlockSize)

		return &b
	},
}

// blockSet is one request's shard working set. Entries are taken on first use,
// so a healthy read never pays for the parity blocks it does not open.
//
// It is not safe for concurrent use. Both callers drive it from a single
// goroutine and hand the resulting slices to workers, which is the only shape
// that makes pooled buffers safe to reclaim at all.
type blockSet struct {
	size int64
	held []*[]byte
}

func newBlockSet(total int, size int64) *blockSet {
	return &blockSet{size: size, held: make([]*[]byte, total)}
}

// at returns shard i's block, taking one the first time it is asked for.
//
// An object recorded with a block larger than the pooled buffer comes from the
// heap instead. That is the pre-streaming layout, where the block is the whole
// shard and could be any size; pooling those would keep one arbitrarily large
// buffer alive for every size ever seen.
func (s *blockSet) at(i int) []byte {
	if s.held[i] == nil {
		if s.size > streamBlockSize {
			b := make([]byte, s.size)
			s.held[i] = &b
		} else {
			s.held[i], _ = blockPool.Get().(*[]byte)
		}
	}

	return (*s.held[i])[:s.size]
}

// spare is shard i's block resliced empty, which is how the encoder is told a
// shard is absent while keeping the capacity to reconstruct back into it. A
// block never taken stays untaken: reconstruction fills data shards, and a
// parity shard that was never opened has nothing to hold.
func (s *blockSet) spare(i int) []byte {
	if s.held[i] == nil {
		return nil
	}

	return (*s.held[i])[:0]
}

// release returns the pooled buffers. Oversized blocks are dropped rather than
// pooled, for the reason at is careful about them.
func (s *blockSet) release() {
	for i, b := range s.held {
		if b != nil && len(*b) == streamBlockSize {
			blockPool.Put(b)
		}
		s.held[i] = nil
	}
}
