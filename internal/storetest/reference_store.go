package storetest

import (
	"bytes"
	"maps"
	"slices"

	"github.com/mulgadc/predastore/internal/blob/engine"
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

func (st *RefStore) Lookup(key [32]byte, index uint32) (r engine.Reader, err error) {
	if st.closed {
		return nil, engine.ErrClosedStore
	}

	value, ok := st.state[[36]byte(engine.MakeKey(key, index))]
	if !ok {
		return nil, engine.ErrKeyNotFound
	}

	return &refReader{
		Reader: bytes.NewReader(value),
		closed: false,
	}, nil
}

func (st *RefStore) Append(key [32]byte, index uint32, size int64) (w engine.Writer, err error) {
	if st.closed {
		return nil, engine.ErrClosedStore
	}

	return &refWriter{
		Buffer: new(bytes.Buffer),
		st:     st,
		key:    [36]byte(engine.MakeKey(key, index)),
		size:   size,
		closed: false,
	}, nil
}

func (st *RefStore) Delete(key [32]byte, index uint32) (bool, error) {
	if st.closed {
		return false, engine.ErrClosedStore
	}

	idxKey := [36]byte(engine.MakeKey(key, index))
	_, existed := st.state[idxKey]
	delete(st.state, idxKey)

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

	closed bool
}

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
	closed bool
}

func (w *refWriter) Close() error {
	if w.closed {
		return engine.ErrClosedWriter
	}

	w.closed = true

	if int64(w.Len()) == w.size {
		w.st.state[w.key] = w.Bytes()
	}

	return nil
}
