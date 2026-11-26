package meta

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
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
	const prefix = "objects/arn:aws:s3:::predastore/vol/map/head."
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
	if index, ok := any(svc.fsm).(interface{ rebuildMapHeads() error }); ok {
		require.NoError(b, index.rebuildMapHeads())
	}
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

func BenchmarkMapHeadIndexRebuild(b *testing.B) {
	for _, n := range []int{16, 16_384} {
		b.Run(fmt.Sprintf("heads=%d", n), func(b *testing.B) {
			db, err := badger.Open(badger.DefaultOptions(b.TempDir()).WithLoggingLevel(badger.WARNING))
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, db.Close()) })
			batch := db.NewWriteBatch()
			defer batch.Cancel()
			for i := range n {
				key := fmt.Appendf(nil, "%s%020d.%020d", "objects/arn:aws:s3:::predastore/vol/map/head.", 1, i)
				require.NoError(b, batch.Set(key, []byte("head")))
			}
			require.NoError(b, batch.Flush())

			fsm := NewFSM(db)
			index, ok := any(fsm).(interface{ rebuildMapHeads() error })
			if !ok {
				b.Skip("baseline has no materialized map-head view")
			}
			b.ReportAllocs()
			for b.Loop() {
				if err := index.rebuildMapHeads(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMapHeadMetadataRPC(b *testing.B) {
	const prefix = "objects/arn:aws:s3:::predastore/vol/map/head."
	for _, count := range []int{16, 16_384} {
		b.Run(fmt.Sprintf("heads=%d", count), func(b *testing.B) {
			svc := benchReplica(b)
			batch := svc.fsm.db.NewWriteBatch()
			defer batch.Cancel()
			for id := range count {
				key := fmt.Appendf(nil, "%s%020d.%020d", prefix, 1, id)
				require.NoError(b, batch.Set(key, []byte("object")))
			}
			require.NoError(b, batch.Flush())
			if index, ok := any(svc.fsm).(interface{ rebuildMapHeads() error }); ok {
				require.NoError(b, index.rebuildMapHeads())
			}
			cfg := &config.Config{Hosts: []config.Host{{
				ID: 1, Addr: "bench-host", Nodes: []config.Node{
					{ID: 1, Role: config.RoleMeta, Port: 6301},
					{ID: 2, Role: config.RoleGate, Port: 6302},
				},
			}}}
			tr := transport.NewPipeTransport("map-head-rpc-bench-client", 0)
			b.Cleanup(func() { tr.Close() })
			resolver, err := rpc.NewResolver(cfg, 2, tr)
			require.NoError(b, err)
			pool := rpc.NewConnPool(2, resolver)
			b.Cleanup(func() { pool.Close() })
			client, err := NewClient(ClientConfig{Client: rpc.NewClient(pool), Replicas: []config.NodeID{1}})
			require.NoError(b, err)
			b.Run("ordinary-page", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := client.Scan(b.Context(), prefix, 1); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("authority", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if key, err := client.LeaderLastKey(b.Context(), prefix); err != nil || key == "" {
						b.Fatalf("head %q: %v", key, err)
					}
				}
			})
			b.Run("insert-delete", func(b *testing.B) {
				ctx := b.Context()
				key := prefix + "99999999999999999999.00000000000000000000"
				b.ReportAllocs()
				for b.Loop() {
					if err := client.Put(ctx, key, []byte("head")); err != nil {
						b.Fatal(err)
					}
					if err := client.Delete(ctx, key); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkThreeVoterMapHeadWritePressure(b *testing.B) {
	const prefix = "objects/arn:aws:s3:::predastore/vol/map/head."
	for _, count := range []int{16, 16_384} {
		b.Run(fmt.Sprintf("heads=%d", count), func(b *testing.B) {
			cluster := startMapHeadTestCluster(b)
			client := cluster.client(cluster.voters...)
			leader := cluster.leader(b, client)
			for _, replica := range cluster.replicas {
				batch := replica.fsm.db.NewWriteBatch()
				for id := range count {
					key := fmt.Appendf(nil, "%s%020d.%020d", prefix, 1, id)
					require.NoError(b, batch.Set(key, []byte("object")))
				}
				require.NoError(b, batch.Flush())
				if index, ok := any(replica.fsm).(interface{ rebuildMapHeads() error }); ok {
					require.NoError(b, index.rebuildMapHeads())
				}
			}
			ordinary := func() error {
				_, err := client.Scan(b.Context(), prefix, 1)
				return err
			}
			authority := func() error {
				_, err := client.LeaderLastKey(b.Context(), prefix)
				return err
			}
			for _, sample := range []struct {
				name     string
				read     func() error
				interval time.Duration
			}{
				{"ordinary/pace", ordinary, 4 * time.Millisecond},
				{"authority/pace", authority, 4 * time.Millisecond},
				{"ordinary/saturated", ordinary, 0},
				{"authority/saturated", authority, 0},
			} {
				b.Run(sample.name, func(b *testing.B) {
					benchmarkWritesWithMapHeadReads(b, cluster.replicas[leader], ordinary, sample.read, sample.interval)
				})
			}
		})
	}
}
