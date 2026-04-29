package store_test

import (
	"errors"
	"os"
	"testing"

	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/store"
	"pgregory.net/rapid"
)

var errInjected = errors.New("injected fault")

// faultState arms one-shot failures on the next IO of a given kind across all
// segment files. Reset between rapid iterations.
type faultState struct {
	failNextWriteAt bool
	failNextReadAt  bool
	failNextSync    bool
	failNextTrunc   bool
	triggered       bool
}

var state faultState

type faultFile struct{ *os.File }

func (f *faultFile) WriteAt(p []byte, off int64) (int, error) {
	if state.failNextWriteAt {
		state.failNextWriteAt = false
		state.triggered = true

		return 0, errInjected
	}

	return f.File.WriteAt(p, off)
}

func (f *faultFile) ReadAt(p []byte, off int64) (int, error) {
	if state.failNextReadAt {
		state.failNextReadAt = false
		state.triggered = true

		return 0, errInjected
	}

	return f.File.ReadAt(p, off)
}

func (f *faultFile) Sync() error {
	if state.failNextSync {
		state.failNextSync = false
		state.triggered = true

		return errInjected
	}

	return f.File.Sync()
}

func (f *faultFile) Truncate(size int64) error {
	if state.failNextTrunc {
		state.failNextTrunc = false
		state.triggered = true

		return errInjected
	}

	return f.File.Truncate(size)
}

// faultSM extends BaseSM with fault-arming actions. Conformance checks in
// BaseSM are gated on a strict() that flips off once a fault has triggered.
type faultSM struct{ *baseSM }

func (sm *faultSM) FailNextWriteAt(t *rapid.T)  { state.failNextWriteAt = true }
func (sm *faultSM) FailNextReadAt(t *rapid.T)   { state.failNextReadAt = true }
func (sm *faultSM) FailNextSync(t *rapid.T)     { state.failNextSync = true }
func (sm *faultSM) FailNextTruncate(t *rapid.T) { state.failNextTrunc = true }

// TestStoreFaults runs the same operation alphabet as TestStore but interleaves
// fault-arming actions that cause the next IO of a chosen kind to fail. Once
// any injected fault has triggered, conformance with the reference oracle is
// relaxed: divergent error returns are tolerated, but the strict no-corruption
// invariant remains — when both stores succeed, sizes and bytes must match.
func TestStoreFaults(t *testing.T) {
	defer store.SetOpenFile(func(path string) (store.SegmentFile, error) {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}

		return &faultFile{File: f}, nil
	})()

	rapid.Check(t, func(rt *rapid.T) {
		state = faultState{}

		dir, err := os.MkdirTemp("", "store-fault-pbt-*") //nolint:usetesting // rapid.Check needs a fresh dir per iteration.
		if err != nil {
			t.Fatalf("temp dir: %v", err)
		}

		refSt := storetest.Open(dir)
		realSt, err := store.Open(
			dir,
			store.WithMaxSegSize(rapid.Uint64Range(16*store.KiB, 1*store.MiB).Draw(rt, "maxSegSize")),
		)
		if err != nil {
			rt.Fatalf("open: %v", err)
		}

		sm := &faultSM{
			baseSM: newBaseSM(dir, refSt, realSt, func() bool { return !state.triggered }),
		}

		defer func() {
			sm.Ref.Close()
			sm.Real.Close()

			storetest.RemoveAll(dir)
			os.RemoveAll(dir)
		}()

		rt.Repeat(rapid.StateMachineActions(sm))
	})
}
