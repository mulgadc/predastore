package store

// SegmentFile aliases the unexported segmentFile interface so external test
// packages can implement and inject it.
type SegmentFile = segmentFile

// SetOpenFile swaps the package-level segment opener and returns a restore
// function. Intended for fault-injection tests.
func SetOpenFile(f func(path string) (SegmentFile, error)) (restore func()) {
	prev := openFile
	openFile = f
	return func() { openFile = prev }
}
