package handlers

import (
	"context"
	"sync"

	"github.com/mulgadc/predastore/internal/meta"
)

// laggingMeta is fakeMeta with the one property of the real client a map does
// not have: a read is answered by any replica that still holds the key, so a
// replica that has not applied a delete yet answers with the value
// (internal/meta/client.go, Client.Get).
//
// A plain map makes every delete visible to the next read, which is the
// friendliest possible timing and hides every read-after-delete defect in the
// package. This one serves the previous value once per deleted key, which is
// the least a lagging replica can do and enough to catch them.
type laggingMeta struct {
	*fakeMeta

	mu      sync.Mutex
	pending map[string][]byte
}

var _ MetaClient = (*laggingMeta)(nil)

func newLaggingMeta(mc *fakeMeta) *laggingMeta {
	return &laggingMeta{fakeMeta: mc, pending: map[string][]byte{}}
}

func (m *laggingMeta) Delete(ctx context.Context, key string) error {
	previous, err := m.fakeMeta.Get(ctx, key)
	if err == nil {
		m.mu.Lock()
		m.pending[key] = previous
		m.mu.Unlock()
	}
	return m.fakeMeta.Delete(ctx, key)
}

func (m *laggingMeta) Get(ctx context.Context, key string) ([]byte, error) {
	value, err := m.fakeMeta.Get(ctx, key)
	if err == nil {
		return value, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if stale, lagging := m.pending[key]; lagging {
		delete(m.pending, key)
		return stale, nil
	}
	return nil, meta.ErrNotFound
}

// Put lands everywhere, so a key written again is no longer lagging behind a
// delete that preceded it.
func (m *laggingMeta) Put(ctx context.Context, key string, value []byte) error {
	m.mu.Lock()
	delete(m.pending, key)
	m.mu.Unlock()
	return m.fakeMeta.Put(ctx, key, value)
}
