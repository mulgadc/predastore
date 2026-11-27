package meta

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/hashicorp/raft"
)

// snapshotStore keeps incoming snapshots out of Raft's recovery list and
// retention policy until the FSM has installed them. Raft closes an incoming
// sink before opening it for restore, so Close alone cannot publish it.
type snapshotStore struct {
	active *raft.FileSnapshotStore
	path   string
	stage  string
	log    io.Writer

	mu      sync.Mutex
	pending map[string]*stagedSnapshot
}

func newSnapshotStore(base string, retain int, log io.Writer) (*snapshotStore, error) {
	active, err := raft.NewFileSnapshotStore(base, retain, log)
	if err != nil {
		return nil, err
	}
	stage := filepath.Join(base, "incoming")
	// The caller holds the local metadata database open exclusively. A
	// staged transfer from an earlier process is never a recovery point.
	if err := os.RemoveAll(stage); err != nil {
		return nil, err
	}
	if err := os.Mkdir(stage, 0700); err != nil {
		return nil, err
	}
	return &snapshotStore{
		active: active, path: filepath.Join(base, "snapshots"), stage: stage,
		log: log, pending: make(map[string]*stagedSnapshot),
	}, nil
}

func (s *snapshotStore) List() ([]*raft.SnapshotMeta, error) {
	return s.active.List()
}

func (s *snapshotStore) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration, configurationIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {
	// Use a separate FileSnapshotStore per sink. Closing one staged sink
	// can then only reap files in that sink's own working directory.
	dir, err := os.MkdirTemp(s.stage, "snapshot-")
	if err != nil {
		return nil, err
	}
	files, err := raft.NewFileSnapshotStore(dir, 1, s.log)
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(dir))
	}
	sink, err := files.Create(version, index, term, configuration, configurationIndex, trans)
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(dir))
	}
	return &stagedSnapshot{SnapshotSink: sink, owner: s, files: files, dir: dir}, nil
}

func (s *snapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	s.mu.Lock()
	staged := s.pending[id]
	delete(s.pending, id)
	s.mu.Unlock()
	if staged == nil {
		return s.active.Open(id)
	}

	meta, source, err := staged.files.Open(id)
	if err != nil {
		return nil, nil, errors.Join(err, os.RemoveAll(staged.dir))
	}
	return meta, &snapshotInstall{ReadCloser: source, staged: staged}, nil
}

type stagedSnapshot struct {
	raft.SnapshotSink
	owner *snapshotStore
	files *raft.FileSnapshotStore
	dir   string
	local bool
	done  bool
}

func (s *stagedSnapshot) Close() error {
	if s.done {
		return nil
	}
	s.done = true
	if err := s.SnapshotSink.Close(); err != nil {
		return errors.Join(err, os.RemoveAll(s.dir))
	}
	if s.local {
		return errors.Join(s.publish(), os.RemoveAll(s.dir))
	}

	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	id := s.ID()
	if s.owner.pending[id] != nil {
		return errors.Join(fmt.Errorf("snapshot %s is already staged", id), os.RemoveAll(s.dir))
	}
	s.owner.pending[id] = s
	return nil
}

func (s *stagedSnapshot) Cancel() error {
	if s.done {
		return nil
	}
	s.done = true
	return errors.Join(s.SnapshotSink.Cancel(), os.RemoveAll(s.dir))
}

func (s *stagedSnapshot) publish() error {
	store := s.owner
	store.mu.Lock()
	defer store.mu.Unlock()

	from := filepath.Join(s.dir, "snapshots", s.ID())
	to := filepath.Join(store.path, s.ID())
	dir, err := os.Open(store.path)
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := os.Rename(from, to); err != nil {
		return err
	}
	// A published snapshot must survive a power loss before Raft is told
	// it can advance its stable snapshot and compact the logs.
	if err := dir.Sync(); err != nil {
		return err
	}
	if err := store.active.ReapSnapshots(); err != nil {
		slog.Warn("meta: unable to reap old snapshots", "error", err)
	}
	return nil
}

type snapshotInstall struct {
	io.ReadCloser
	staged *stagedSnapshot
	closed bool
}

func (s *snapshotInstall) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	err := errors.Join(s.ReadCloser.Close(), os.RemoveAll(s.staged.dir))
	if err != nil {
		slog.Warn("meta: unable to discard staged snapshot files", "snapshot", s.staged.ID(), "error", err)
	}
	return err
}

func publishInstalledSnapshot(rc io.ReadCloser) error {
	// Raft wraps the source to track restore progress.
	for {
		switch source := rc.(type) {
		case *snapshotInstall:
			return source.staged.publish()
		case raft.ReadCloserWrapper:
			rc = source.WrappedReadCloser()
		default:
			return nil
		}
	}
}
