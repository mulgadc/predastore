package meta

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func BenchmarkMapHeadLookup(b *testing.B) {
	for _, count := range []int{16, 16_384} {
		for _, authority := range []bool{false, true} {
			mode := "ordinary"
			if authority {
				mode = "authority"
			}
			b.Run(fmt.Sprintf("heads=%d/%s", count, mode), func(b *testing.B) {
				benchmarkMapHeadLookup(b, count, authority)
			})
		}
	}
}

func benchmarkMapHeadLookup(b *testing.B, count int, authority bool) {
	svc := benchReplica(b)
	const prefix = "objects/arn:aws:s3:::vol/map/head."
	err := svc.fsm.db.Update(func(txn *badger.Txn) error {
		for id := range count {
			key := fmt.Appendf(nil, "%s%020d.%020d", prefix, 1, id)
			if err := txn.Set(key, []byte("object")); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(b, err)
	ordinary := func() error {
		return svc.fsm.ScanFrom(prefix, "", func(string, []byte) error { return nil })
	}
	lookup := ordinary
	if authority {
		index, ok := any(svc.fsm).(interface {
			LastKey(string) (string, error)
		})
		if !ok {
			b.Skip("baseline has no bounded lookup")
		}
		lookup = func() error {
			if code := svc.leaderRead(); code != "" {
				return fmt.Errorf("leader barrier: %s", code)
			}
			_, err := index.LastKey(prefix)
			return err
		}
	}
	b.Run("lookup", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := lookup(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("concurrent-writes", func(b *testing.B) {
		benchmarkWritesWithMapHeadReads(b, svc, ordinary, lookup, 0)
	})
	b.Run("paced-writes", func(b *testing.B) {
		benchmarkWritesWithMapHeadReads(b, svc, ordinary, lookup, 4*time.Millisecond)
	})
}

func benchmarkWritesWithMapHeadReads(b *testing.B, svc *Server, ordinary, lookup func() error, interval time.Duration) {
	stop := make(chan struct{})
	var readers sync.WaitGroup
	var reads atomic.Int64
	for _, read := range []func() error{ordinary, lookup, lookup} {
		readers.Go(func() {
			if interval > 0 {
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
						if err := read(); err != nil {
							b.Error(err)
							return
						}
						reads.Add(1)
					}
				}
			}
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := read(); err != nil {
					b.Error(err)
					return
				}
				reads.Add(1)
			}
		})
	}

	var writeLatency []time.Duration
	b.ReportAllocs()
	for b.Loop() {
		start := time.Now()
		err := svc.apply(Command{Type: CommandPut, Key: []byte("objects/write-control"), Value: []byte("value")})
		writeLatency = append(writeLatency, time.Since(start))
		if err != nil {
			b.Error(err)
			break
		}
	}
	close(stop)
	readers.Wait()
	slices.Sort(writeLatency)
	if len(writeLatency) > 0 {
		b.ReportMetric(float64(writeLatency[len(writeLatency)/2]), "write-p50-ns")
		b.ReportMetric(float64(writeLatency[(len(writeLatency)-1)*99/100]), "write-p99-ns")
		b.ReportMetric(float64(len(writeLatency))/b.Elapsed().Seconds(), "writes/s")
		b.ReportMetric(float64(reads.Load())/b.Elapsed().Seconds(), "lookups/s")
	}
}
