package store_test

import (
	"os"
	"testing"

	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/store"
	"pgregory.net/rapid"
)

func TestStore(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		dir, err := os.MkdirTemp("", "store-pbt-*") //nolint:usetesting // t.TempDir lives for the whole test; rapid.Check needs a fresh dir per iteration.
		if err != nil {
			t.Fatalf("temp dir: %v", err)
		}

		refSt := storetest.Open(dir)
		realSt, err := store.Open(
			dir,
			store.WithMaxSegSize(rapid.Uint64Range(16*store.KiB, 1*store.MiB).Draw(rt, "maxSegSize")),
			store.WithAEAD(storetest.TestAEAD()),
		)
		if err != nil {
			rt.Fatalf("open: %v", err)
		}

		sm := newBaseSM(dir, refSt, realSt, nil)

		defer func() {
			sm.Ref.Close()
			sm.Real.Close()

			storetest.RemoveAll(dir)
			os.RemoveAll(dir)
		}()

		rt.Repeat(rapid.StateMachineActions(sm))
	})
}
