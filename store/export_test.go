package store

// File aliases the unexported file interface so external test
// packages can implement and inject it.
type File = file

// SetOpenFile swaps the package-level segment opener and returns a restore
// function. Intended for fault-injection tests. create reports whether the
// caller is on the append path (may create the file) or a read path (must
// report a missing file as an error).
func SetOpenFile(f func(path string, create bool) (File, error)) (restore func()) {
	prev := openFile
	openFile = f
	return func() { openFile = prev }
}

// CompactOnce runs a single compaction cycle synchronously. The background
// compactor is wired separately; tests drive cycles deterministically here.
func (store *Store) CompactOnce() error { return store.compactOnce() }
