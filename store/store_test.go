package store_test

import (
	"os"
	"testing"

	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/store"
	"pgregory.net/rapid"
)

func TestStore(t *testing.T) {
	// Hoist AEAD construction outside rapid.Check: cipher.AEAD is
	// concurrency-safe per stdlib contract, and constructing a fresh one per
	// iteration retains badger arenas through the cipher's escape graph,
	// blowing peak RSS from ~40 MB to >1 GB.
	aead := storetest.TestAEAD()
	rapid.Check(t, func(rt *rapid.T) {
		dir, err := os.MkdirTemp("", "store-pbt-*") //nolint:usetesting // t.TempDir lives for the whole test; rapid.Check needs a fresh dir per iteration.
		if err != nil {
			t.Fatalf("temp dir: %v", err)
		}

		refSt := storetest.Open(dir)
		realSt, err := store.Open(
			dir,
			store.WithMaxSegSize(rapid.Uint64Range(16*store.KiB, 1*store.MiB).Draw(rt, "maxSegSize")),
			store.WithAEAD(aead),
		)
		if err != nil {
			rt.Fatalf("open: %v", err)
		}

		sm := newBaseSM(dir, refSt, realSt, aead, nil)

		defer func() {
			sm.Ref.Close()
			sm.Real.Close()

			storetest.RemoveAll(dir)
			os.RemoveAll(dir)
		}()

		rt.Repeat(rapid.StateMachineActions(sm))
	})
}
