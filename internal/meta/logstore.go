package meta

import (
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.etcd.io/bbolt"
)

// trimBatch is how many entries one write transaction deletes when a
// snapshot trims the log. bolt has one writer, so appends wait behind each
// transaction; a batch this size holds them for milliseconds, not seconds.
const trimBatch = 1000

var (
	_ raft.LogStore    = (*logStore)(nil)
	_ raft.StableStore = (*logStore)(nil)
)

// logStore is the raft log and stable store. It deletes a strict prefix of
// the log in short transactions so appends interleave with a snapshot's trim.
type logStore struct {
	*raftboltdb.BoltStore
	batch       uint64
	deleteRange func(from, to uint64) error
}

// openLogStore opens the raft log without syncing bolt's freelist. Synced, the
// freelist is rewritten on every commit and holds every page a snapshot's log
// truncation freed, which made each one-entry commit cost megabytes.
func openLogStore(path string) (*logStore, error) {
	bolt, err := raftboltdb.New(raftboltdb.Options{
		Path: path,
		BoltOptions: &bbolt.Options{
			NoFreelistSync: true,
			FreelistType:   bbolt.FreelistMapType,
		},
	})
	if err != nil {
		return nil, err
	}
	return &logStore{BoltStore: bolt, batch: trimBatch, deleteRange: bolt.DeleteRange}, nil
}

// DeleteRange splits only a strict prefix: one that starts at the first entry
// and ends before the last. Raft also deletes a conflicting suffix or the whole
// log; those stay one transaction. An interrupted prefix leaves a shorter log.
func (s *logStore) DeleteRange(from, to uint64) error {
	first, err := s.FirstIndex()
	if err != nil {
		return err
	}
	last, err := s.LastIndex()
	if err != nil {
		return err
	}
	if from != first || to >= last {
		return s.deleteRange(from, to)
	}
	for lo := from; lo <= to; lo += s.batch {
		if err := s.deleteRange(lo, lo+min(s.batch-1, to-lo)); err != nil {
			return err
		}
	}
	return nil
}
