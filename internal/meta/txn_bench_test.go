package meta

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

// BenchmarkFSMApplyTxn applies committed transactions shaped like a conditional
// create, an absent check and two writes, on the store BenchmarkFSMApplyCommitted
// uses. Each transaction commits alone, so compare it with entries=1 there.
func BenchmarkFSMApplyTxn(b *testing.B) {
	for _, batch := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("entries=%d", batch), func(b *testing.B) {
			db, err := badger.Open(badger.DefaultOptions(b.TempDir()).
				WithLoggingLevel(badger.WARNING).WithSyncWrites(true))
			require.NoError(b, err)
			b.Cleanup(func() { db.Close() })
			fsm := NewFSM(db)

			value := bytes.Repeat([]byte{0x70}, 48)
			logs := make([]*raft.Log, batch)
			var index uint64
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				b.StopTimer()
				for i := range logs {
					index++
					key := fmt.Appendf(nil, "objects/%d", index)
					logs[i] = commandLogB(b, index, Command{
						Type:   CommandTxn,
						Checks: []TxnCheck{{Key: key, Absent: true}},
						Writes: []TxnWrite{
							{Key: key, Value: value},
							{Key: fmt.Appendf(nil, "intents/%d", index), Value: value},
						},
					})
				}
				b.StartTimer()
				for _, resp := range fsm.ApplyBatch(logs) {
					if r, ok := resp.(TxnResult); !ok || !r.Applied {
						b.Fatalf("transaction not applied: %#v", resp)
					}
				}
			}
		})
	}
}
