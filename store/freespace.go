package store

import (
	"fmt"
	"syscall"
	"time"
)

const (
	// defaultNearfullFreeFrac is the free-space fraction below which Append
	// kicks an immediate compaction pass but still accepts writes.
	defaultNearfullFreeFrac = 0.15

	// defaultFullFreeFrac is the free-space fraction below which Append
	// rejects writes with ErrStoreFull. The 5% reserve protects the OS
	// (journald, process forking, OVN/OVS) and leaves compaction its own
	// read-modify-write scratch space.
	defaultFullFreeFrac = 0.05

	// statfsThrottleInterval bounds how often Append pays for a statfs
	// syscall. Free space does not move fast enough at shard-write
	// granularity to justify measuring it on every call.
	statfsThrottleInterval = 1 * time.Second

	// statfsBytesInterval forces a fresh statfs once this many bytes have been
	// reserved since the last measurement, regardless of statfsThrottleInterval.
	// Free space tracks write volume, not wall-clock time: a burst writing
	// hundreds of MB/s blows straight through the full watermark inside a single
	// throttle window if the reading is only refreshed on the timer. Bounding it
	// by bytes caps the worst-case overshoot between measurements at roughly this
	// value, well inside the reserve the full watermark protects.
	statfsBytesInterval = 64 << 20 // 64 MiB
)

// statfsFree reports the free-space fraction of the filesystem backing dir,
// as Bavail/Blocks — space available to an unprivileged writer over total
// capacity. Using Bavail rather than Bfree means a filesystem's own reserved-
// blocks headroom (e.g. ext4's default 5% root reserve) is already excluded,
// so the watermark measures the same budget an actual write would see.
//
// A package-level var so tests can substitute a synthetic implementation
// without depending on the real disk's free space.
var statfsFree = func(dir string) (float64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", dir, err)
	}
	if stat.Blocks == 0 {
		return 1, nil
	}
	return float64(stat.Bavail) / float64(stat.Blocks), nil
}

// freeSpaceFraction returns the store's cached free-space fraction, refreshing
// it via statfsFree at most once per statfsThrottleInterval. Callers must
// hold store.mutex — Append is the only caller today, and it already does.
func (store *Store) freeSpaceFraction() (float64, error) {
	now := time.Now()
	// Serve the cache only while BOTH bounds hold: the throttle interval has not
	// elapsed AND not enough bytes have been reserved since the last measurement
	// to have moved free space materially. The byte bound is what stops a fast
	// write burst from racing past the watermark inside the 1s window — statfs
	// alone lags the reserve rate, and the store can reach 0 free before the next
	// timed refresh fires.
	if !store.statfsAt.IsZero() &&
		now.Sub(store.statfsAt) < statfsThrottleInterval &&
		store.bytesSinceStatfs < statfsBytesInterval {
		return store.freeFrac, nil
	}

	frac, err := statfsFree(store.dir)
	if err != nil {
		return 0, err
	}

	store.freeFrac = frac
	store.statfsAt = now
	store.bytesSinceStatfs = 0
	return frac, nil
}

// NearFull reports whether the store's backing filesystem is in the
// nearfull band: below the nearfull watermark but still above full, so
// writes are still accepted. Used to signal early backpressure to clients.
func (store *Store) NearFull() bool {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	frac, err := store.freeSpaceFraction()
	if err != nil {
		return false
	}
	return frac < store.nearfullFreeFrac && frac >= store.fullFreeFrac
}
