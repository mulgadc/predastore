package utils

import "math"

// IntToUint32 converts int to uint32, clamping to [0, math.MaxUint32].
func IntToUint32(v int) uint32 {
	if v < 0 {
		return 0
	}
	if v > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}

// IntToUint64 converts int to uint64, returning 0 if negative.
func IntToUint64(v int) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

// Int64ToUint64 converts int64 to uint64, returning 0 if negative.
func Int64ToUint64(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

// Uint64ToInt64 converts uint64 to int64, capping at math.MaxInt64.
func Uint64ToInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

// Uint64ToInt converts uint64 to int, capping at math.MaxInt.
func Uint64ToInt(v uint64) int {
	if v > uint64(math.MaxInt) {
		return math.MaxInt
	}
	return int(v)
}
