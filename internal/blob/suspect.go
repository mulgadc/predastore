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

// suspectSet holds the generations of shard positions whose read failed
// authentication on this node, each generation on its own. It is in memory
// only: a restart forgets it, and the next failed read finds the shard again.
type suspectSet struct {
	mu      sync.Mutex
	order   *list.List
	entries map[shardPosition]map[uint64]*list.Element
	evicted atomic.Uint64
}

// entryOf is the entry an element holds; the list holds nothing else.
func entryOf(e *list.Element) *suspectEntry {
	entry, _ := e.Value.(*suspectEntry)
	return entry
}

func newSuspectSet() *suspectSet {
	return &suspectSet{order: list.New(), entries: make(map[shardPosition]map[uint64]*list.Element)}
}

// add records one generation of a position and reports whether it was new.
func (s *suspectSet) add(pos shardPosition, epoch uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entries[pos][epoch]; ok {
		return false
	}
	if s.order.Len() >= suspectCap {
		s.removeLocked(s.order.Front())
		s.evicted.Add(1)
	}
	if s.entries[pos] == nil {
		s.entries[pos] = make(map[uint64]*list.Element, 1)
	}
	s.entries[pos][epoch] = s.order.PushBack(&suspectEntry{pos: pos, epoch: epoch})
	return true
}

// suspect reports whether the live generation of a position is in the set,
// and forgets the position's other generations, which a stat has shown are no
// longer live.
func (s *suspectSet) suspect(pos shardPosition, live uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	found := false
	for epoch, e := range s.entries[pos] {
		if epoch == live {
			found = true
			continue
		}
		s.removeLocked(e)
	}
	return found
}

// clearEpoch forgets one generation of a position.
func (s *suspectSet) clearEpoch(pos shardPosition, epoch uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[pos][epoch]; ok {
		s.removeLocked(e)
	}
}

// clearPosition forgets every generation of a position.
func (s *suspectSet) clearPosition(pos shardPosition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.entries[pos] {
		s.removeLocked(e)
	}
}

func (s *suspectSet) removeLocked(e *list.Element) {
	entry := entryOf(e)
	s.order.Remove(e)
	delete(s.entries[entry.pos], entry.epoch)
	if len(s.entries[entry.pos]) == 0 {
		delete(s.entries, entry.pos)
	}
}

func (s *suspectSet) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.order.Len()
}
