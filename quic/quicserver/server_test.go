package quicserver

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mulgadc/predastore/internal/storetest"
)

// TestNewWithRetryBindsListener exercises the server-side quic.Config
// construction (including the widened flow-control window fields) by
// standing up a QUIC listener on an ephemeral port and immediately
// shutting it down.
func TestNewWithRetryBindsListener(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "wal")
	if err := os.MkdirAll(walDir, 0o750); err != nil {
		t.Fatalf("mkdir walDir: %v", err)
	}

	qs, err := NewWithRetry(walDir, "127.0.0.1:0", 1, WithMasterKey(storetest.TestKey()))
	if err != nil {
		t.Fatalf("NewWithRetry: %v", err)
	}
	if err := qs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
