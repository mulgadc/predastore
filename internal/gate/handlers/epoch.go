package handlers

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mulgadc/predastore/internal/config"
)

// A write epoch carries the moment the object was written:
//
//	 63                    20 19        10 9         0
//	+------------------------+------------+-----------+
//	|  milliseconds (44)     | node (10)  | seq (10)  |
//	+------------------------+------------+-----------+
//
// The shard path compares it for equality and nothing else, exactly as it did
// when this was eight bytes of crypto/rand, and the milliseconds are what
// LastModified reports. Spending the same eight bytes on both is what makes a
// real modification time cost no storage: the alternative was an mtime field
// beside the epoch, taking the placement record from 27 bytes to 35.
//
// The node id is not decoration. Uniqueness is the one property the shard path
// depends on -- an epoch that repeats for a key lets a surviving shard from an
// older write be accepted as current, and the object reads back as a mixture
// of two writes with nothing recording it. A bare timestamp gives two gates
// writing the same key in the same millisecond the same value, which is the
// contended case rather than a rare one. Including the minting node makes
// uniqueness structural.
const (
	epochSeqBits  = 10
	epochNodeBits = 10
	epochMsBits   = 64 - epochNodeBits - epochSeqBits

	epochSeqMax  = 1<<epochSeqBits - 1
	epochNodeMax = 1<<epochNodeBits - 1
	epochMsMax   = 1<<epochMsBits - 1
)

// epochDriftWarnMs is how far ahead of its own clock the minter may run before
// it says so, and epochWarnEveryMs bounds how often it repeats itself.
const (
	epochDriftWarnMs = 5_000
	epochWarnEveryMs = 60_000
)

var errNoEpochMinter = errors.New("gate config has no epoch minter")

// EpochMinter issues write epochs for one gate. One per gate process: two
// minters for the same node would each start a millisecond at sequence zero
// and collide, which is the whole thing the node field exists to prevent.
type EpochMinter struct {
	node uint64
	now  func() time.Time

	mu         sync.Mutex
	lastMs     int64
	seq        uint64
	lastWarnMs int64
}

// NewEpochMinter builds the minter for one gate node.
func NewEpochMinter(node config.NodeID) (*EpochMinter, error) {
	if node == 0 || uint64(node) > epochNodeMax {
		return nil, fmt.Errorf("gate node id %d does not fit in %d bits", node, epochNodeBits)
	}

	return &EpochMinter{node: uint64(node), now: time.Now}, nil
}

// Next issues the next epoch.
//
// It never returns a value at or below one it has already returned. That is
// what makes a clock that steps backwards safe: the minter holds its position
// and spends sequence numbers instead, so an overwrite cannot reuse an epoch
// an earlier write already had.
func (m *EpochMinter) Next() (uint64, error) {
	if m == nil {
		return 0, errNoEpochMinter
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	ms := m.now().UnixMilli()
	switch {
	case ms > m.lastMs:
		m.lastMs, m.seq = ms, 0
	case m.seq < epochSeqMax:
		m.seq++
	default:
		// The sequence is spent for this millisecond, so take the next one.
		// Borrowing from the future keeps the minter monotonic without
		// blocking a write on the clock.
		m.lastMs++
		m.seq = 0
	}

	if m.lastMs <= 0 || m.lastMs > epochMsMax {
		return 0, fmt.Errorf("write epoch millisecond %d does not fit in %d bits", m.lastMs, epochMsBits)
	}
	m.warnOnDrift(ms)

	return uint64(m.lastMs)<<(epochNodeBits+epochSeqBits) | m.node<<epochSeqBits | m.seq, nil
}

// warnOnDrift reports a minter issuing epochs from further ahead than the
// clock justifies, which is either a write rate past the sequence width or a
// clock that has moved. Callers hold the lock.
func (m *EpochMinter) warnOnDrift(ms int64) {
	drift := m.lastMs - ms
	if drift <= epochDriftWarnMs || m.lastMs-m.lastWarnMs < epochWarnEveryMs {
		return
	}
	m.lastWarnMs = m.lastMs

	slog.Warn("gate: write epochs are running ahead of the clock",
		"node", m.node, "drift_ms", drift)
}

// EpochTime is the moment an epoch was minted. Milliseconds are finer than S3
// reports on either surface -- Last-Modified is RFC 1123 seconds and
// ListObjectsV2 is ISO 8601 with milliseconds -- so nothing is lost here.
func EpochTime(epoch uint64) time.Time {
	return time.UnixMilli(int64(epoch >> (epochNodeBits + epochSeqBits))).UTC()
}
