package storetest

import (
	"bytes"
	"maps"
	"slices"

	"github.com/mulgadc/predastore/store"
)

var fs = make(map[string]*RefStore)

func RemoveAll(dir string) {
	delete(fs, dir)
}

type RefStore struct {
	state  map[[36]byte][]byte
	closed bool
}

func Open(dir string) *RefStore {
	if st, ok := fs[dir]; ok {
		if st.closed {
			st.closed = false
		}

		return st
	}

	st := &RefStore{
		state: make(map[[36]byte][]byte),
	}

	fs[dir] = st

	return st
}

func (st *RefStore) Lookup(objectHash [32]byte, shardIndex uint32) (r store.Reader, err error) {
	if st.closed {
		return nil, store.ErrClosedStore
	}

	shard, ok := st.state[[36]byte(store.MakeShardKey(objectHash, shardIndex))]
	if !ok {
		return nil, store.ErrKeyNotFound
	}

	return &refReader{
		Reader: bytes.NewReader(shard),
		closed: false,
	}, nil
}

func (st *RefStore) Append(objectHash [32]byte, shardIndex uint32, size int64) (w store.Writer, err error) {
	if st.closed {
		return nil, store.ErrClosedStore
	}

	return &refWriter{
		Buffer: new(bytes.Buffer),
		st:     st,
		key:    [36]byte(store.MakeShardKey(objectHash, shardIndex)),
		size:   size,
		closed: false,
	}, nil
}

func (st *RefStore) Delete(objectHash [32]byte, shardIndex uint32) error {
	if st.closed {
		return store.ErrClosedStore
	}

	delete(st.state, [36]byte(store.MakeShardKey(objectHash, shardIndex)))

	return nil
}

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
		return store.ErrClosedStore
	}

	st.closed = true

	return nil
}

func (st *RefStore) IsClosed() bool { return st.closed }

type refReader struct {
	*bytes.Reader

	closed bool
}

func (r *refReader) Close() error {
	if r.closed {
		return store.ErrClosedReader
	}

	r.closed = true

	return nil
}

type refWriter struct {
	*bytes.Buffer

	st     *RefStore
	key    [36]byte
	size   int64
	closed bool
}

func (w *refWriter) Close() error {
	if w.closed {
		return store.ErrClosedWriter
	}

	w.closed = true

	if int64(w.Len()) == w.size {
		w.st.state[w.key] = w.Bytes()
	}

	return nil
}
