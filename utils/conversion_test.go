package utils

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntToUint32(t *testing.T) {
	tests := []struct {
		name string
		in   int
		want uint32
	}{
		{"Zero", 0, 0},
		{"Positive", 42, 42},
		{"MaxUint32", math.MaxUint32, math.MaxUint32},
		{"Above MaxUint32", math.MaxUint32 + 1, math.MaxUint32},
		{"Negative", -1, 0},
		{"Large negative", -999, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IntToUint32(tt.in))
		})
	}
}

func TestIntToUint64(t *testing.T) {
	tests := []struct {
		name string
		in   int
		want uint64
	}{
		{"Zero", 0, 0},
		{"Positive", 42, 42},
		{"Negative", -1, 0},
		{"Large negative", -999, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IntToUint64(tt.in))
		})
	}
}

func TestInt64ToUint64(t *testing.T) {
	tests := []struct {
		name string
		in   int64
		want uint64
	}{
		{"Zero", 0, 0},
		{"Positive", 42, 42},
		{"MaxInt64", math.MaxInt64, uint64(math.MaxInt64)},
		{"Negative", -1, 0},
		{"MinInt64", math.MinInt64, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Int64ToUint64(tt.in))
		})
	}
}

func TestUint64ToInt64(t *testing.T) {
	tests := []struct {
		name string
		in   uint64
		want int64
	}{
		{"Zero", 0, 0},
		{"Normal", 42, 42},
		{"MaxInt64", uint64(math.MaxInt64), math.MaxInt64},
		{"Above MaxInt64", uint64(math.MaxInt64) + 1, math.MaxInt64},
		{"MaxUint64", math.MaxUint64, math.MaxInt64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Uint64ToInt64(tt.in))
		})
	}
}

func TestUint64ToInt(t *testing.T) {
	tests := []struct {
		name string
		in   uint64
		want int
	}{
		{"Zero", 0, 0},
		{"Normal", 42, 42},
		{"MaxInt", uint64(math.MaxInt), math.MaxInt},
		{"Above MaxInt", uint64(math.MaxInt) + 1, math.MaxInt},
		{"MaxUint64", math.MaxUint64, math.MaxInt},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, Uint64ToInt(tt.in))
		})
	}
}
