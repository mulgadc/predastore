package blob

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/mulgadc/predastore/internal/blob/engine"
)

// KeyVerifier is a store that can report whether its master key is the key
// that sealed its data. A store that cannot report marks no shard suspect.
type KeyVerifier interface {
	KeyVerified() bool
}

var _ KeyVerifier = (*engine.Store)(nil)

// suspectCap bounds the suspect set. A full set evicts its oldest entry, so
// entries for shards that are gone never block a new failure.
const suspectCap = 4096

// shardPosition is one shard position on this node.
type shardPosition struct {
	key   [32]byte
	index uint32
}

type suspectEntry struct {
	pos   shardPosition
	epoch uint64
}

// suspectSet holds, for each position, the generation whose read failed
// authentication on this node. It is in memory only: a restart forgets it,
// and the next failed read finds the shard again.
type suspectSet struct {
	mu      sync.Mutex
	order   *list.List
	entries map[shardPosition]*list.Element
	evicted atomic.Uint64
}

func newSuspectSet() *suspectSet {
	return &suspectSet{order: list.New(), entries: make(map[shardPosition]*list.Element)}
}

// add records the generation of a position and reports whether it was new.
func (s *suspectSet) add(pos shardPosition, epoch uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[pos]; ok {
		entry := e.Value.(*suspectEntry)
		if entry.epoch == epoch {
			return false
		}
		entry.epoch = epoch
		s.order.MoveToBack(e)
		return true
	}
	if s.order.Len() >= suspectCap {
		oldest := s.order.Front()
		delete(s.entries, s.order.Remove(oldest).(*suspectEntry).pos)
		s.evicted.Add(1)
	}
	s.entries[pos] = s.order.PushBack(&suspectEntry{pos: pos, epoch: epoch})
	return true
}

// suspect reports whether the generation of a position is in the set. A
// different generation there is stale, and is dropped.
func (s *suspectSet) suspect(pos shardPosition, epoch uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[pos]
	if !ok {
		return false
	}
	if e.Value.(*suspectEntry).epoch == epoch {
		return true
	}
	s.order.Remove(e)
	delete(s.entries, pos)
	return false
}

// clear forgets a position, or with a non-zero epoch only that generation.
func (s *suspectSet) clear(pos shardPosition, epoch uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[pos]
	if !ok || (epoch != 0 && e.Value.(*suspectEntry).epoch != epoch) {
		return
	}
	s.order.Remove(e)
	delete(s.entries, pos)
}

func (s *suspectSet) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.order.Len()
}
