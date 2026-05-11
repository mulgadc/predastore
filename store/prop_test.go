package store_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/store"
	"pgregory.net/rapid"
)

const (
	pbtFragBodySize = 8 * store.KiB
	pbtMaxShardSize = 64 * store.KiB
)

// readBody randomizes between io.ReadAll and WriteTo so both reader paths
// get exercised under property tests. tag is included in the rapid Draw
// label so failure traces show which call site produced the choice.
func readBody(t *rapid.T, tag string, refR, realR store.Reader) (refBody, realBody []byte, refErr, realErr error) {
	if rapid.Bool().Draw(t, tag+"/useWriteTo") {
		var refBuf, realBuf bytes.Buffer
		_, refErr = refR.WriteTo(&refBuf)
		_, realErr = realR.WriteTo(&realBuf)
		return refBuf.Bytes(), realBuf.Bytes(), refErr, realErr
	}

	refBody, refErr = io.ReadAll(refR)
	realBody, realErr = io.ReadAll(realR)
	return refBody, realBody, refErr, realErr
}

// baseSM is the shared rapid.StateMachine-compatible base. Per-test SMs
// embed *baseSM and add their own actions (e.g. fault-arming).
//
// Strict reports whether ref/real conformance must match exactly. Set it to
// nil for always-strict, or to a closure that flips to false once a fault
// has triggered.
type baseSM struct {
	Ref    *storetest.RefStore
	Real   *store.Store
	Strict func() bool
	Dir    string
}

func newBaseSM(dir string, refSt *storetest.RefStore, realSt *store.Store, strict func() bool) *baseSM {
	if strict == nil {
		strict = func() bool { return true }
	}

	return &baseSM{
		Ref:    refSt,
		Real:   realSt,
		Strict: strict,
		Dir:    dir,
	}
}

func (sm *baseSM) drawKey(t *rapid.T, tag string) [36]byte {
	if sm.Ref.Len() > 0 && rapid.Bool().Draw(t, tag+"/useExisting") {
		return rapid.SampledFrom(sm.Ref.Keys()).Draw(t, tag+"/existingKey")
	}

	return [36]byte(store.MakeShardKey(
		[32]byte(rapid.SliceOfN(rapid.Byte(), 32, 32).Draw(t, tag+"/objectHash")),
		rapid.Uint32().Draw(t, tag+"/shardIndex"),
	))
}

func (sm *baseSM) drawBody(t *rapid.T, tag string) []byte {
	return rapid.OneOf(
		rapid.SliceOfN(rapid.Byte(), 0, 0),
		rapid.SliceOfN(rapid.Byte(), 1, 1),
		rapid.SliceOfN(rapid.Byte(), pbtFragBodySize, pbtFragBodySize),
		rapid.SliceOfN(rapid.Byte(), pbtFragBodySize+1, pbtFragBodySize+1),
		rapid.SliceOfN(rapid.Byte(), 2*pbtFragBodySize, 2*pbtFragBodySize),
		rapid.SliceOfN(rapid.Byte(), 0, pbtMaxShardSize),
	).Draw(t, tag+"/body")
}

func (sm *baseSM) Open(t *rapid.T) {
	if !sm.Ref.IsClosed() {
		t.Skip("already open")
	}

	realSt, err := store.Open(sm.Dir, store.WithAEAD(storetest.TestAEAD()))
	if err != nil {
		if sm.Strict() {
			t.Fatalf("open: real=%v", err)
		}

		t.Skip("real open failed under fault")
	}

	sm.Ref = storetest.Open(sm.Dir)
	sm.Real = realSt
}

func (sm *baseSM) Close(t *rapid.T) {
	refErr := sm.Ref.Close()
	realErr := sm.Real.Close()
	if sm.Strict() && !errors.Is(realErr, refErr) {
		t.Fatalf("close: ref=%v real=%v", refErr, realErr)
	}
}

func (sm *baseSM) Lookup(t *rapid.T) {
	key := sm.drawKey(t, "lookup")
	sm.checkKey(t, "lookup", key)
}

func (sm *baseSM) Append(t *rapid.T) {
	key := sm.drawKey(t, "append")
	objectHash := [32]byte(key[:32])
	shardIndex := binary.BigEndian.Uint32(key[32:])
	body := sm.drawBody(t, "append")

	refW, refErr := sm.Ref.Append(objectHash, shardIndex, int64(len(body)))
	realW, realErr := sm.Real.Append(objectHash, shardIndex, int64(len(body)))
	defer func() {
		if refW != nil {
			refW.Close()
		}
		if realW != nil {
			realW.Close()
		}
	}()

	if refErr != nil || realErr != nil {
		if sm.Strict() && !errors.Is(realErr, refErr) {
			t.Fatalf("append: ref=%v real=%v", refErr, realErr)
		}

		return
	}

	if rapid.Bool().Draw(t, "append/useReadFrom") {
		refN, refErr := refW.ReadFrom(bytes.NewReader(body))
		realN, realErr := realW.ReadFrom(bytes.NewReader(body))
		if sm.Strict() && !errors.Is(realErr, refErr) {
			t.Fatalf("append readfrom: ref=%v real=%v", refErr, realErr)
		}
		if sm.Strict() && refN != realN {
			t.Fatalf("append readfrom count: ref=%d real=%d", refN, realN)
		}
	} else {
		refN, refErr := refW.Write(body)
		realN, realErr := realW.Write(body)
		if sm.Strict() && !errors.Is(realErr, refErr) {
			t.Fatalf("append write: ref=%v real=%v", refErr, realErr)
		}
		if sm.Strict() && refN != realN {
			t.Fatalf("append write count: ref=%d real=%d", refN, realN)
		}
	}

	// Lookup before commit: writer.Close hasn't run yet, so neither store
	// should have committed the new extent.
	sm.checkKey(t, "append/precommit", key)

	refErr = refW.Close()
	realErr = realW.Close()
	if sm.Strict() && !errors.Is(realErr, refErr) {
		t.Fatalf("append writerclose: ref=%v real=%v", refErr, realErr)
	}
}

func (sm *baseSM) Delete(t *rapid.T) {
	key := sm.drawKey(t, "delete")
	objectHash := [32]byte(key[:32])
	shardIndex := binary.BigEndian.Uint32(key[32:])

	refErr := sm.Ref.Delete(objectHash, shardIndex)
	realErr := sm.Real.Delete(objectHash, shardIndex)
	if sm.Strict() && !errors.Is(realErr, refErr) {
		t.Fatalf("delete: ref=%v real=%v", refErr, realErr)
	}
}

// Check is the invariant: every key present in the reference must read back
// identical bytes from the real store. Under relaxed mode (post-fault),
// per-key divergence is tolerated.
func (sm *baseSM) Check(t *rapid.T) {
	for _, key := range sm.Ref.Keys() {
		sm.checkKey(t, "check", key)
	}
}

// checkKey verifies one shard's ref/real conformance. tag is included in the
// failure messages and rapid Draw labels so traces show which call site (the
// Check invariant scan, or Append's pre-commit lookup) hit the failure.
func (sm *baseSM) checkKey(t *rapid.T, tag string, key [36]byte) {
	objectHash := [32]byte(key[:32])
	shardIndex := binary.BigEndian.Uint32(key[32:])

	refR, refErr := sm.Ref.Lookup(objectHash, shardIndex)
	realR, realErr := sm.Real.Lookup(objectHash, shardIndex)
	defer func() {
		if refR != nil {
			refR.Close()
		}
		if realR != nil {
			realR.Close()
		}
	}()

	if refErr != nil || realErr != nil {
		if sm.Strict() && !errors.Is(realErr, refErr) {
			t.Fatalf("%s lookup: ref=%v real=%v", tag, refErr, realErr)
		}

		return
	}

	if sm.Strict() && refR.Size() != realR.Size() {
		t.Fatalf("%s size: ref=%d real=%d", tag, refR.Size(), realR.Size())
	}

	refBody, realBody, refErr, realErr := readBody(t, tag, refR, realR)
	if refErr != nil || realErr != nil {
		if sm.Strict() && !errors.Is(realErr, refErr) {
			t.Fatalf("%s read: ref=%v real=%v", tag, refErr, realErr)
		}

		return
	}

	if sm.Strict() && !bytes.Equal(refBody, realBody) {
		t.Fatalf("%s body: ref=%d bytes real=%d bytes", tag, len(refBody), len(realBody))
	}
}
