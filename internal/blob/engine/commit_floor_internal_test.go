package engine

import (
	"errors"
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
