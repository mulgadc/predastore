// Command badgerdump prints the contents of one of predastore's two Badger
// stores, for reading what a cluster actually recorded rather than inferring
// it from the write path.
//
//	go run ./scripts/bench/badgerdump -mode meta <dir>/badger
//	go run ./scripts/bench/badgerdump -mode blob <dir>/db
//
// The cluster must be stopped: Badger takes a directory lock.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	"github.com/dgraph-io/badger/v4"
)

// The blob engine's on-disk shapes, restated here because they are not
// exported. A shard key is objectHash ‖ shardIndex and a tombstone key is a
// prefix byte ‖ segment ‖ offset, so length alone separates them.
const (
	shardKeySize     = 36
	extentSize       = 32
	tombstoneKeySize = 17
	tombstonePrefix  = 'd'
)

// The meta store's placement record header, restated for the same reason.
const (
	placementMagic     = 0x00
	placementVersion   = 0x01
	placementFixedSize = 19
)

func main() {
	mode := flag.String("mode", "meta", "meta or blob")
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: badgerdump -mode meta|blob <badger dir>")
		os.Exit(2)
	}

	db, err := badger.Open(badger.DefaultOptions(flag.Arg(0)).
		WithLoggingLevel(badger.ERROR).WithReadOnly(true))
	if err != nil {
		fmt.Fprintf(os.Stderr, "open %s: %v\n", flag.Arg(0), err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().KeyCopy(nil)
			v, verr := it.Item().ValueCopy(nil)
			if verr != nil {
				return verr
			}
			if *mode == "blob" {
				fmt.Println(formatBlob(k, v))
			} else {
				fmt.Println(formatMeta(k, v))
			}
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "scan: %v\n", err)
		os.Exit(1)
	}
}

// printable renders a key readably: text as text, binary as hex, and the
// object table's mixed "objects/"+sha256 form as both.
func printable(k []byte) string {
	if i := bytes.IndexByte(k, '/'); i >= 0 && isText(k[:i]) {
		rest := k[i+1:]
		if isText(rest) {
			return string(k)
		}
		return fmt.Sprintf("%s/<%s>", k[:i], hex.EncodeToString(rest))
	}
	if isText(k) {
		return string(k)
	}
	return "<" + hex.EncodeToString(k) + ">"
}

func isText(b []byte) bool {
	for _, c := range b {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return len(b) > 0
}

// formatMeta renders one row of the cluster-wide meta store. The two rows an
// object costs have different value shapes, so each is decoded on its own
// terms and anything else falls back to a hex preview.
func formatMeta(k, v []byte) string {
	name := printable(k)
	if placement, ok := formatPlacement(v); ok {
		return fmt.Sprintf("%-80s  %d bytes  %s", name, len(v), placement)
	}
	if len(v) == 32 {
		return fmt.Sprintf("%-80s  %d bytes  -> objecthash %s", name, len(v), hex.EncodeToString(v))
	}
	return fmt.Sprintf("%-80s  %d bytes  %s", name, len(v), preview(v))
}

// formatPlacement decodes a placement record from its bytes rather than
// through the handlers package, so the dumper reports the layout that is on
// disk instead of whatever the current struct happens to say.
func formatPlacement(v []byte) (string, bool) {
	if len(v) < placementFixedSize || v[0] != placementMagic || v[1] != placementVersion {
		return "", false
	}
	k := int(v[2])
	size := binary.BigEndian.Uint64(v[3:11])
	epoch := binary.BigEndian.Uint64(v[11:19])

	var ids []uint64
	for rest := v[placementFixedSize:]; len(rest) > 0; {
		id, n := binary.Uvarint(rest)
		if n <= 0 {
			return "", false
		}
		ids = append(ids, id)
		rest = rest[n:]
	}
	if len(ids) < k {
		return "", false
	}
	return fmt.Sprintf("placement size=%d epoch=%016x data=%v parity=%v",
		size, epoch, ids[:k], ids[k:]), true
}

// formatBlob renders one row of a blob node's private index. The two kinds of
// row are told apart by key length alone, which is the same discrimination
// the engine itself makes. Widths are converted to arrays so every field
// below is indexed within a length the compiler can see.
func formatBlob(k, v []byte) string {
	switch {
	case len(k) == shardKeySize && len(v) == extentSize:
		return formatShardRow([shardKeySize]byte(k), [extentSize]byte(v))
	case len(k) == tombstoneKeySize && len(v) == 8 && k[0] == tombstonePrefix:
		return formatTombstoneRow([tombstoneKeySize]byte(k), [8]byte(v))
	default:
		return fmt.Sprintf("other  key=%s (%d bytes)  %s", printable(k), len(k), preview(v))
	}
}

// formatShardRow prints the extent one shard of one object occupies. Offsets
// and sizes are shown unsigned, as the bytes hold them, so a corrupt row reads
// as an implausible number rather than as a plausible negative one.
func formatShardRow(k [shardKeySize]byte, v [extentSize]byte) string {
	hash := hex.EncodeToString(k[:32])
	return fmt.Sprintf("shard  key=%s.. index=%d  extent{seg=%d off=%d psize=%d lsize=%d}",
		hash[:16], binary.BigEndian.Uint32(k[32:]),
		binary.BigEndian.Uint64(v[0:8]), binary.BigEndian.Uint64(v[8:16]),
		binary.BigEndian.Uint64(v[16:24]), binary.BigEndian.Uint64(v[24:32]))
}

func formatTombstoneRow(k [tombstoneKeySize]byte, v [8]byte) string {
	return fmt.Sprintf("tomb   seg=%d off=%d  psize=%d",
		binary.BigEndian.Uint64(k[1:9]), binary.BigEndian.Uint64(k[9:17]),
		binary.BigEndian.Uint64(v[:]))
}

func preview(v []byte) string {
	if len(v) > 48 {
		return hex.EncodeToString(v[:48]) + ".."
	}
	return hex.EncodeToString(v)
}
