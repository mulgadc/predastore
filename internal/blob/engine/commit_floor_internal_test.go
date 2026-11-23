package engine

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"testing"
)

// A commit for an epoch this node never prepared, with a newer generation
// live, must not read as overtaken: the gate counts overtaken shards toward
// its write floor, and this one holds nothing.
func TestCommitOfAGenerationNeverHeldBehindANewerOneIsNotPrepared(t *testing.T) {
	st, _ := openTestStore(t)
	oh := [32]byte{0x61}

	never := nextTestEpoch()
	live := prepareValue(t, st, oh, 0, []byte("the newer generation"))
	if _, err := st.Commit(oh, 0, live); err != nil {
		t.Fatalf("commit the newer epoch: %v", err)
	}

	published, err := st.Commit(oh, 0, never)
	if !errors.Is(err, ErrNotPrepared) {
		t.Fatalf("Commit(never prepared, newer live) = (%v, %v), want ErrNotPrepared", published, err)
	}
}

// The overtaken answer still stands for a generation the node does hold, in
// both of the ways it can come to be held behind a newer one.
func TestCommitOfAGenerationHeldBehindANewerOneIsOvertaken(t *testing.T) {
	t.Run("prepared and overtaken before it was live", func(t *testing.T) {
		st, _ := openTestStore(t)
		oh := [32]byte{0x62}
		older := prepareValue(t, st, oh, 0, []byte("older"))
		newer := prepareValue(t, st, oh, 0, []byte("newer"))
		if _, err := st.Commit(oh, 0, newer); err != nil {
			t.Fatalf("commit newer: %v", err)
		}
		published, err := st.Commit(oh, 0, older)
		if err != nil || published {
			t.Fatalf("Commit(overtaken) = (%v, %v), want (false, nil)", published, err)
		}
		// The retry of that same commit must answer the same way.
		published, err = st.Commit(oh, 0, older)
		if err != nil || published {
			t.Fatalf("retried Commit(overtaken) = (%v, %v), want (false, nil)", published, err)
		}
	})

	t.Run("published and later demoted", func(t *testing.T) {
		st, _ := openTestStore(t)
		oh := [32]byte{0x63}
		older := prepareValue(t, st, oh, 0, []byte("older"))
		if _, err := st.Commit(oh, 0, older); err != nil {
			t.Fatalf("commit older: %v", err)
		}
		newer := prepareValue(t, st, oh, 0, []byte("newer"))
		if _, err := st.Commit(oh, 0, newer); err != nil {
			t.Fatalf("commit newer: %v", err)
		}
		published, err := st.Commit(oh, 0, older)
		if err != nil || published {
			t.Fatalf("Commit(demoted) = (%v, %v), want (false, nil)", published, err)
		}
	})
}

// quietLogs discards logging for one benchmark, so the store's open lines do
// not split a result from its name.
func quietLogs(b *testing.B) {
	b.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	b.Cleanup(func() { slog.SetDefault(prev) })
}

// BenchmarkCommitOvertaken measures the one commit branch that gained a read:
// a retry for a generation held behind a newer one, which now proves the
// retained row before it answers overtaken.
func BenchmarkCommitOvertaken(b *testing.B) {
	quietLogs(b)
	st, err := Open(b.TempDir(), WithFreeSpaceWatermark(0, 0), WithAEAD(testAEAD(b)))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = st.Close() })
	oh := [32]byte{0x64}
	older := prepareValue(b, st, oh, 0, []byte("older"))
	newer := prepareValue(b, st, oh, 0, []byte("newer"))
	if _, err := st.Commit(oh, 0, newer); err != nil {
		b.Fatalf("commit newer: %v", err)
	}
	if _, err := st.Commit(oh, 0, older); err != nil {
		b.Fatalf("commit older: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := st.Commit(oh, 0, older); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}
}

// BenchmarkCommitPublish is the healthy path, for comparison: prepare and
// publish a fresh generation, which the change does not touch.
func BenchmarkCommitPublish(b *testing.B) {
	quietLogs(b)
	st, err := Open(b.TempDir(), WithFreeSpaceWatermark(0, 0), WithAEAD(testAEAD(b)))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	b.Cleanup(func() { _ = st.Close() })
	body := bytes.Repeat([]byte("x"), 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		oh := [32]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
		epoch := prepareValue(b, st, oh, 0, body)
		if _, err := st.Commit(oh, 0, epoch); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}
}
