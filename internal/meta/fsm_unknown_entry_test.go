package meta

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stopValue runs fn and returns what it panicked with, or nil when it returned.
func stopValue(fn func()) (stopped any) {
	defer func() { stopped = recover() }()
	fn()
	return nil
}

// rawLog is a committed command entry whose bytes are given as they are on disk.
func rawLog(index uint64, data string) *raft.Log {
	return &raft.Log{Index: index, Term: 1, Type: raft.LogCommand, Data: []byte(data)}
}

// A committed entry that this release cannot read stops the replica. The
// store keeps the applied row of the last commit before it, so neither the
// entry nor any entry after it is skipped, and the next start applies it.
func TestFSM_ApplyBatch_StopsOnAnEntryItCannotRead(t *testing.T) {
	// "c2VjcmV0LWtleQ==" is "secret-key". The value is "dg==", "v".
	entries := map[string]string{
		"unknown type": `{"type":200,"key":"c2VjcmV0LWtleQ==","value":"dg=="}`,
		"type 255":     `{"type":255,"key":"c2VjcmV0LWtleQ==","value":"dg=="}`,
		"no type":      `{"key":"c2VjcmV0LWtleQ==","value":"dg=="}`,
		"null type":    `{"type":null,"key":"c2VjcmV0LWtleQ==","value":"dg=="}`,
		"undecodable":  `{not json`,
	}
	for name, data := range entries {
		t.Run(name, func(t *testing.T) {
			opts := smallTxnOptions(t.TempDir())
			db := openStore(t, opts)
			fsm := NewFSM(db)
			fsm.ApplyBatch([]*raft.Log{commandLog(t, 1, Command{Type: CommandPut, Key: []byte("a"), Value: []byte("1")})})
			require.Equal(t, uint64(1), storedApplied(t, db))

			stopped := stopValue(func() {
				fsm.ApplyBatch([]*raft.Log{
					commandLog(t, 2, Command{Type: CommandPut, Key: []byte("b"), Value: []byte("2")}),
					rawLog(3, data),
					commandLog(t, 4, Command{Type: CommandPut, Key: []byte("d"), Value: []byte("4")}),
				})
			})

			require.NotNil(t, stopped, "the replica must stop, not answer the entry and pass it")
			msg := fmt.Sprint(stopped)
			assert.Contains(t, msg, "entry 3")
			assert.NotContains(t, msg, "secret", "the stop names no key")
			switch name {
			case "unknown type":
				assert.Contains(t, msg, "type 200")
			case "type 255":
				assert.Contains(t, msg, "type 255", "an explicit type is named, even the one the decode fills in")
			case "no type", "null type":
				assert.Contains(t, msg, "no type")
			}
			db, _ = restart(t, db, opts)
			assert.Equal(t, uint64(1), storedApplied(t, db), "the row stays below the entry")
			assertStored(t, db, map[string]*string{
				"a": str("1"), "b": nil, "secret-key": nil, "d": nil,
			})
		})
	}
}

// The zero command type is CommandPut, and every release writes it as an
// explicit number. That encoding still applies as a put.
func TestFSM_ApplyBatch_AnExplicitZeroTypeIsAPut(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(db)

	got := fsm.ApplyBatch([]*raft.Log{rawLog(1, `{"type":0,"key":"YQ==","value":"MQ=="}`)})

	assert.Equal(t, []any{nil}, got)
	assertStored(t, db, map[string]*string{"a": str("1")})
	assert.Equal(t, uint64(1), storedApplied(t, db))
}

// knowsType200 is a later release, for which type 200 is a put. It records the
// index of every command entry raft gives it.
type knowsType200 struct {
	*FSM

	mu   sync.Mutex
	seen []uint64
}

func (k *knowsType200) ApplyBatch(logs []*raft.Log) []any {
	read := make([]*raft.Log, len(logs))
	for i, log := range logs {
		read[i] = log
		var cmd Command
		if log.Type != raft.LogCommand || json.Unmarshal(log.Data, &cmd) != nil {
			continue
		}
		k.mu.Lock()
		k.seen = append(k.seen, log.Index)
		k.mu.Unlock()
		if cmd.Type == CommandType(200) {
			data, err := json.Marshal(Command{Type: CommandPut, Key: cmd.Key, Value: cmd.Value})
			if err != nil {
				panic(err)
			}
			copied := *log
			copied.Data = data
			read[i] = &copied
		}
	}
	return k.FSM.ApplyBatch(read)
}

func (k *knowsType200) indexes() []uint64 {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]uint64(nil), k.seen...)
}

var _ raft.BatchingFSM = (*knowsType200)(nil)

// A replica stops on an entry of a type it does not know. Restarted on the
// same store and log by a release that knows the type, raft gives it the
// entry again, and it applies that entry and the ones after it in order.
func TestFSM_ARestartAppliesTheEntryTheReplicaStoppedOn(t *testing.T) {
	disk := newRaftDisk(t)
	logs := []*raft.Log{
		{Index: 1, Term: 1, Type: raft.LogConfiguration, Data: raft.EncodeConfiguration(disk.conf)},
		commandLog(t, 2, Command{Type: CommandPut, Key: []byte("a"), Value: []byte("1")}),
		rawLog(3, `{"type":200,"key":"Yw==","value":"Mw=="}`),
		commandLog(t, 4, Command{Type: CommandPut, Key: []byte("b"), Value: []byte("4")}),
	}
	require.NoError(t, disk.logs.StoreLogs(logs))
	require.NoError(t, disk.logs.SetUint64([]byte("CurrentTerm"), 1))

	opts := smallTxnOptions(t.TempDir())
	db := openStore(t, opts)
	fsm := NewFSM(db)
	fsm.ApplyBatch(logs[:2])
	require.Equal(t, uint64(2), storedApplied(t, db))
	require.NotNil(t, stopValue(func() { fsm.ApplyBatch(logs[2:]) }), "the old release must stop on index 3")

	db, fsm = restart(t, db, opts)
	later := &knowsType200{FSM: fsm}
	r, err := disk.start(t, later)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return r.State() == raft.Leader }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, r.Barrier(5*time.Second).Error())

	assert.Equal(t, []uint64{2, 3, 4}, later.indexes(), "raft replays the log from the start, the stopped entry included")
	assertStored(t, db, map[string]*string{"a": str("1"), "c": str("3"), "b": str("4")})
	assert.Equal(t, uint64(4), storedApplied(t, db))
}
