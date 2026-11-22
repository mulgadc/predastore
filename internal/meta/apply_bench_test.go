package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/require"
)

// benchReplica starts a single-voter replica on its production stores and
// waits until it leads, so an apply measures the log store, raft and the FSM
// together, as every metadata write in an S3 PUT or DELETE pays them.
func benchReplica(b *testing.B) *Server {
	b.Helper()
	cfg := &config.Config{Hosts: []config.Host{{
		ID: 1, Addr: "bench-host",
		Nodes: []config.Node{{ID: 1, Role: config.RoleMeta, Port: 6301}},
	}}}
	tr := transport.NewPipeTransport("bench-host", 6301)
	b.Cleanup(func() { tr.Close() })
	ln, err := tr.Listen()
	require.NoError(b, err)
	res, err := rpc.NewResolver(cfg, 1, tr)
	require.NoError(b, err)

	svc, err := New(Config{
		NodeID: 1, DataDir: b.TempDir(), Peers: []config.NodeID{1}, Bootstrap: true,
		Listeners: []transport.Listener{ln}, Resolver: res,
		HeartbeatTimeout: 50 * time.Millisecond, ElectionTimeout: 50 * time.Millisecond,
		LeaderLeaseTimeout: 50 * time.Millisecond, CommitTimeout: 5 * time.Millisecond,
	})
	require.NoError(b, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	b.Cleanup(func() {
		cancel()
		<-done
	})
	require.Eventually(b, svc.LeaderKnown, 10*time.Second, 10*time.Millisecond)
	return svc
}

// BenchmarkMetaApply is the metadata half of the S3 write path: a Put through
// raft, one caller at a time and from concurrent callers, at the size of a
// placement record and of a large bucket metadata value.
func BenchmarkMetaApply(b *testing.B) {
	for _, size := range []int{48, 4 << 10} {
		value := bytes.Repeat([]byte{0x70}, size)
		for _, callers := range []int{1, 16} {
			b.Run(fmt.Sprintf("value=%dB/callers=%d", size, callers), func(b *testing.B) {
				svc := benchReplica(b)
				var n atomic.Uint64
				b.ReportAllocs()
				b.ResetTimer()
				// Exactly callers goroutines share b.N applies, so the load is the
				// same on every host rather than scaled by GOMAXPROCS.
				var wg sync.WaitGroup
				for range callers {
					wg.Go(func() {
						for {
							i := n.Add(1)
							if i > uint64(b.N) {
								return
							}
							key := fmt.Sprintf("objects/%d", i)
							if err := svc.apply(Command{Type: CommandPut, Key: []byte(key), Value: value}); err != nil {
								b.Error(err)
								return
							}
						}
					})
				}
				wg.Wait()
			})
		}
	}
}

// BenchmarkFSMApplyCommitted applies a run of committed entries to a store
// opened as production opens it, synced writes included, the way raft hands
// them over: as one batch when the FSM takes batches, else one at a time.
func BenchmarkFSMApplyCommitted(b *testing.B) {
	for _, batch := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("entries=%d", batch), func(b *testing.B) {
			db, err := badger.Open(badger.DefaultOptions(b.TempDir()).
				WithLoggingLevel(badger.WARNING).WithSyncWrites(true))
			require.NoError(b, err)
			b.Cleanup(func() { db.Close() })
			fsm := NewFSM(db)
			batching, isBatching := any(fsm).(raft.BatchingFSM)

			value := bytes.Repeat([]byte{0x70}, 48)
			logs := make([]*raft.Log, batch)
			var index uint64
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				for i := range logs {
					index++
					logs[i] = commandLogB(b, index, Command{Type: CommandPut, Key: fmt.Appendf(nil, "objects/%d", index), Value: value})
				}
				b.StartTimer()
				if isBatching {
					batching.ApplyBatch(logs)
					continue
				}
				for _, l := range logs {
					fsm.Apply(l)
				}
			}
		})
	}
}

// BenchmarkLogStoreCommit times one single-entry commit to the raft log, on a
// fresh log and on one whose front a snapshot has just truncated, which is
// where a synced freelist costs every commit the most.
func BenchmarkLogStoreCommit(b *testing.B) {
	for _, truncated := range []bool{false, true} {
		b.Run(fmt.Sprintf("truncated=%v", truncated), func(b *testing.B) {
			store, err := openLogStore(filepath.Join(b.TempDir(), "raft.db"))
			require.NoError(b, err)
			b.Cleanup(func() { store.Close() })

			const entries = 40_000
			data := bytes.Repeat([]byte{0x6c}, 700)
			batch := make([]*raft.Log, 0, 1000)
			for i := uint64(1); i <= entries; i++ {
				batch = append(batch, &raft.Log{Index: i, Term: 1, Type: raft.LogCommand, Data: data})
				if len(batch) == cap(batch) {
					require.NoError(b, store.StoreLogs(batch))
					batch = batch[:0]
				}
			}
			if truncated {
				require.NoError(b, store.DeleteRange(1, 30_000))
			}
			index := uint64(entries)
			b.ResetTimer()
			for range b.N {
				index++
				if err := store.StoreLog(&raft.Log{Index: index, Term: 1, Type: raft.LogCommand, Data: data}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// commandLogB encodes cmd as a committed entry for a benchmark.
func commandLogB(b *testing.B, index uint64, cmd Command) *raft.Log {
	b.Helper()
	data, err := json.Marshal(cmd)
	require.NoError(b, err)
	return &raft.Log{Index: index, Term: 1, Type: raft.LogCommand, Data: data}
}
