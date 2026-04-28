package store_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"maps"
	"os"
	"slices"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/mulgadc/predastore/store"
	"pgregory.net/rapid"
)

// fragBodySize must match store.fragBodySize (segment.go) so the OneOf below
// pins the actual single/multi-fragment boundary.
const (
	fragBodySize = 8 * store.KiB
	maxShardSize = 64 * store.KiB
)

func TestStore(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		dir, err := os.MkdirTemp("", "store-pbt-*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}

		model := make(map[[36]byte][]byte)
		impl, err := store.Open(dir)
		if err != nil {
			rt.Fatalf("expected nil, got %v", err)
		}

		closed := false

		defer func() {
			if !closed {
				impl.Close()
			}

			os.RemoveAll(dir)
		}()

		shardKeyGen := rapid.Custom(func(rt *rapid.T) [36]byte {
			if len(model) > 0 && rapid.Bool().Draw(rt, "useExisting") {
				// Sort for deterministic SampledFrom — Go randomizes map iteration,
				// which would otherwise break rapid's seed-based replay and shrinking.
				keys := slices.Collect(maps.Keys(model))
				slices.SortFunc(keys, func(a, b [36]byte) int { return bytes.Compare(a[:], b[:]) })
				return rapid.SampledFrom(keys).Draw(rt, "existingKey")
			}

			var key [36]byte
			copy(key[:32], rapid.SliceOfN(rapid.Byte(), 32, 32).Draw(rt, "objectHash"))
			binary.BigEndian.PutUint32(key[32:], rapid.Uint32().Draw(rt, "shardIndex"))

			return key
		})

		shardBodyGen := rapid.OneOf(
			// Pin lengths that straddle fragment boundaries.
			rapid.SliceOfN(rapid.Byte(), 0, 0),
			rapid.SliceOfN(rapid.Byte(), 1, 1),
			rapid.SliceOfN(rapid.Byte(), fragBodySize, fragBodySize),
			rapid.SliceOfN(rapid.Byte(), fragBodySize+1, fragBodySize+1),
			rapid.SliceOfN(rapid.Byte(), 2*fragBodySize, 2*fragBodySize),
			// Random spread for breadth — rapid biases toward small here, which is fine.
			rapid.SliceOfN(rapid.Byte(), 0, maxShardSize),
		)

		rt.Repeat(map[string]func(*rapid.T){
			"open": func(rt *rapid.T) {
				switch {
				case !closed:
					rt.Skip("already open")

				default:
					if impl, err = store.Open(dir); err != nil {
						rt.Fatalf("expected nil, got %v", err)
					}

					closed = false
				}
			},

			"close": func(rt *rapid.T) {
				switch {
				case impl == nil:
					rt.Skip("already nil")

				case !closed:
					if err := impl.Close(); err != nil {
						rt.Fatalf("expected nil, got %v", err)
					}

					closed = true

				default:
					if err := impl.Close(); err != store.ErrStoreClosed {
						rt.Fatalf("expected ErrStoreClosed, got %v", err)
					}

					impl = nil
				}
			},

			"lookup": func(rt *rapid.T) {
				key := shardKeyGen.Draw(rt, "key")
				objectHash := [32]byte(key[:32])
				shardIndex := binary.BigEndian.Uint32(key[32:])

				switch {
				case impl == nil:
					rt.Skip("store is nil")

				case closed:
					if _, err := impl.Lookup(objectHash, shardIndex); err != store.ErrStoreClosed {
						rt.Fatalf("expected ErrStoreClosed, got %v", err)
					}

				default:
					sr, err := impl.Lookup(objectHash, shardIndex)
					want, inModel := model[key]
					if !inModel {
						if !errors.Is(err, badger.ErrKeyNotFound) {
							rt.Fatalf("expected ErrKeyNotFound, got %v", err)
						}

						return
					}

					if err != nil {
						rt.Fatalf("expected nil, got %v", err)
					}

					got, err := io.ReadAll(sr)
					if err != nil {
						rt.Fatalf("read shard: expected nil, got %v", err)
					}

					if !bytes.Equal(got, want) {
						rt.Fatalf("shard body mismatch: got %d bytes, want %d bytes", len(got), len(want))
					}

					if err := sr.Close(); err != nil {
						rt.Fatalf("close reader: expected nil, got %v", err)
					}
				}
			},

			"append": func(rt *rapid.T) {
				key := shardKeyGen.Draw(rt, "key")
				objectHash := [32]byte(key[:32])
				shardIndex := binary.BigEndian.Uint32(key[32:])
				body := shardBodyGen.Draw(rt, "body")

				switch {
				case impl == nil:
					rt.Skip("store is nil")

				case closed:
					if _, err := impl.Append(objectHash, shardIndex, int64(len(body))); err != store.ErrStoreClosed {
						rt.Fatalf("expected ErrStoreClosed, got %v", err)
					}

				default:
					sw, err := impl.Append(objectHash, shardIndex, int64(len(body)))
					if err != nil {
						rt.Fatalf("expected nil, got %v", err)
					}

					n, err := sw.Write(body)
					if err != nil {
						rt.Fatalf("expected nil, got %v", err)
					}

					if n != len(body) {
						rt.Fatalf("shard body mismatch: wrote %d bytes, expected %d bytes", n, len(body))
					}

					if err := sw.Close(); err != nil {
						rt.Fatalf("expected nil, got %v", err)
					}

					model[key] = body
				}
			},

			"delete": func(rt *rapid.T) {
				key := shardKeyGen.Draw(rt, "key")
				objectHash := [32]byte(key[:32])
				shardIndex := binary.BigEndian.Uint32(key[32:])

				switch {
				case impl == nil:
					rt.Skip("store is nil")

				case closed:
					if err := impl.Delete(objectHash, shardIndex); err != store.ErrStoreClosed {
						rt.Fatalf("expected ErrStoreClosed, got %v", err)
					}

				default:
					if err := impl.Delete(objectHash, shardIndex); err != nil {
						rt.Fatalf("expected nil, got %v", err)
					}

					delete(model, key)
				}
			},

			"": func(rt *rapid.T) {
				if impl == nil || closed {
					return
				}

				for key, want := range model {
					objectHash := [32]byte(key[:32])
					shardIndex := binary.BigEndian.Uint32(key[32:])

					sr, err := impl.Lookup(objectHash, shardIndex)
					if err != nil {
						rt.Fatalf("invariant: lookup %x: %v", key, err)
					}

					got, err := io.ReadAll(sr)
					if err != nil {
						rt.Fatalf("invariant: read %x: %v", key, err)
					}

					if !bytes.Equal(got, want) {
						rt.Fatalf("invariant: body mismatch for %x: got %d bytes, want %d bytes", key, len(got), len(want))
					}

					if err := sr.Close(); err != nil {
						rt.Fatalf("invariant: close reader: %v", err)
					}
				}
			},
		})
	})
}
