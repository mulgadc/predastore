package handlers

import (
	"os"
	"testing"
)

// TestMain removes the fixtures that benchmarks share across a whole run.
func TestMain(m *testing.M) {
	code := m.Run()
	closeLargeBucket()
	os.Exit(code)
}
