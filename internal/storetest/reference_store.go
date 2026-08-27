package storetest

import (
	"bytes"
	"maps"
	"slices"
	"sync/atomic"

	"github.com/mulgadc/predastore/internal/blob/engine"
)

var fs = make(map[string]*RefStore)

func RemoveAll(dir string) {
	delete(fs, dir)
}

// refValue is a stored shard: its bytes and the epoch it was written under.
type refValue struct {
	data  []byte
	epoch uint64
}

type RefStore struct {
	state map[[36]byte]refValue
	// prepared holds shards written but not yet published, mirroring the
	// engine's prepared namespace so a lookup sees the previous generation
	// until a commit lands.
	prepared map[[36]byte]refValue
	closed   bool
}

func Open(dir string) *RefStore {
	if st, ok := fs[dir]; ok {
		if st.closed {
			st.closed = false
		}

		return st
	}

	st := &RefStore{
		state:    make(map[[36]byte]refValue),
		prepared: make(map[[36]byte]refValue),
	}

	fs[dir] = st

	return st
}

func (st *RefStore) Lookup(key [32]byte, index uint32) (r engine.Reader, err error) {
	if st.closed {
		return nil, engine.ErrClosedStore
	}

	value, ok := st.state[[36]byte(engine.MakeKey(key, index))]
	if !ok {
		return nil, engine.ErrKeyNotFound
	}

	return &refReader{
		Reader: bytes.NewReader(value.data),
		epoch:  value.epoch,
		closed: false,
	}, nil
}

func (st *RefStore) Append(key [32]byte, index uint32, size int64, epoch uint64) (w engine.Writer, err error) {
	if st.closed {
		return nil, engine.ErrClosedStore
	}

	return &refWriter{
		Buffer: new(bytes.Buffer),
		st:     st,
		key:    [36]byte(engine.MakeKey(key, index)),
		size:   size,
		epoch:  epoch,
		closed: false,
	}, nil
}

// Commit publishes a prepared value, idempotently against the epoch: a commit
// of one already published is success rather than a failed write that landed.
func (st *RefStore) Commit(key [32]byte, index uint32, epoch uint64) error {
	if st.closed {
		return engine.ErrClosedStore
	}

	idxKey := [36]byte(engine.MakeKey(key, index))
	if prepared, ok := st.prepared[idxKey]; ok && prepared.epoch == epoch {
		st.state[idxKey] = prepared
		delete(st.prepared, idxKey)

		return nil
	}
	if live, ok := st.state[idxKey]; ok && live.epoch == epoch {
		return nil
	}

	return engine.ErrNotPrepared
}

// Abort discards a prepared value. Aborting one that was never prepared is
// success: the caller asked that nothing be left pending, and nothing is.
func (st *RefStore) Abort(key [32]byte, index uint32, epoch uint64) error {
	if st.closed {
		return engine.ErrClosedStore
	}

	idxKey := [36]byte(engine.MakeKey(key, index))
	if prepared, ok := st.prepared[idxKey]; ok && prepared.epoch == epoch {
		delete(st.prepared, idxKey)
	}

	return nil
}

func (st *RefStore) Delete(key [32]byte, index uint32) (bool, error) {
	if st.closed {
		return false, engine.ErrClosedStore
	}

	idxKey := [36]byte(engine.MakeKey(key, index))
	_, existed := st.state[idxKey]
	delete(st.state, idxKey)
	// A surviving prepared value would resurrect the shard on a later commit.
	delete(st.prepared, idxKey)

	return existed, nil
}

// NearFull always reports false: the reference store lives in memory and has
// no free-space watermark to cross.
func (st *RefStore) NearFull() bool { return false }

func (st *RefStore) Len() int {
	return len(st.state)
}

func (st *RefStore) Keys() [][36]byte {
	// Sort for deterministic SampledFrom — Go randomizes map iteration,
	// which would otherwise break rapid's seed-based replay and shrinking.
	keys := slices.Collect(maps.Keys(st.state))
	slices.SortFunc(keys, func(a, b [36]byte) int {
		return bytes.Compare(a[:], b[:])
	})

	return keys
}

func (st *RefStore) Close() error {
	if st.closed {
		return engine.ErrClosedStore
	}

	st.closed = true

	return nil
}

func (st *RefStore) IsClosed() bool { return st.closed }

type refReader struct {
	*bytes.Reader

	epoch  uint64
	closed bool
}

func (r *refReader) Epoch() uint64 { return r.epoch }

func (r *refReader) Close() error {
	if r.closed {
		return engine.ErrClosedReader
	}

	r.closed = true

	return nil
}

type refWriter struct {
	*bytes.Buffer

	st     *RefStore
	key    [36]byte
	size   int64
	epoch  uint64
	closed bool
}

func (w *refWriter) Close() error {
	if w.closed {
		return engine.ErrClosedWriter
	}

	w.closed = true

	if int64(w.Len()) == w.size {
		w.st.prepared[w.key] = refValue{data: w.Bytes(), epoch: w.epoch}
	}

	return nil
}

// NextEpoch hands out distinct non-zero write epochs for tests. Zero is
// reserved as invalid, so a counter starting at one is enough.
func NextEpoch() uint64 { return testEpochs.Add(1) }

var testEpochs atomic.Uint64
