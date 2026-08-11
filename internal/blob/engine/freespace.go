package engine

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
	// rejects writes with ErrStoreFull, reserving headroom for the OS and
	// for compaction's own read-modify-write scratch space.
	defaultFullFreeFrac = 0.05

	// statfsThrottleInterval bounds how often Append pays for a statfs syscall.
	statfsThrottleInterval = 1 * time.Second

	// statfsBytesInterval forces a fresh statfs once this many bytes have been
	// reserved since the last measurement, regardless of statfsThrottleInterval.
	// Free space tracks write volume, not wall-clock time, so a fast burst
	// needs a byte bound to keep from overshooting the watermark between
	// timed refreshes.
	statfsBytesInterval = 64 << 20 // 64 MiB
)

// statfsFree reports the free-space fraction of the filesystem backing dir,
// as Bavail/Blocks — space available to an unprivileged writer, excluding a
// filesystem's own reserved-blocks headroom.
//
// A package-level var so tests can substitute a synthetic implementation.
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
	// Cache is valid only while both the time and byte bounds hold.
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
