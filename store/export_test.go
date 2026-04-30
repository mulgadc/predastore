package store

// File aliases the unexported file interface so external test
// packages can implement and inject it.
type File = file

// SetOpenFile swaps the package-level segment opener and returns a restore
// function. Intended for fault-injection tests.
func SetOpenFile(f func(path string) (File, error)) (restore func()) {
	prev := openFile
	openFile = f
	return func() { openFile = prev }
}
