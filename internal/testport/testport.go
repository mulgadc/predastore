// Package testport allocates blocks of localhost ports for tests that bind real
// listeners. Production code never imports this.
//
// Ports come from below the kernel's ephemeral range (net.ipv4.ip_local_port_range,
// 32768-60999 on Linux by default). A listener port picked from inside that range
// can already be held by an autobound client socket — notably this process's own
// pooled QUIC clients, which keep their ports for the life of the test binary —
// which turns a fixed port into an intermittent "bind: address already in use".
package testport

import (
	"fmt"
	"math/rand/v2"
	"net"
	"sync/atomic"
	"testing"
)

const (
	bandStart   = 10000
	bandEnd     = 32768
	blockStride = 32
	blocks      = (bandEnd - bandStart) / blockStride
)

// nextBlock is process-wide, so blocks never overlap within one test binary. It
// starts somewhere random in the band so concurrent package binaries under
// `go test ./...` don't all contend for the bottom of it.
var nextBlock atomic.Int32

func init() {
	nextBlock.Store(rand.Int32N(blocks)) //nolint:gosec // spaces out test port blocks, not security-sensitive.
}

// Block reserves size consecutive ports and returns the first. The block lies
// below the ephemeral floor, is disjoint from every other block this process has
// handed out, and was free when returned.
//
// Ports are probed rather than held, so a concurrent test binary could still take
// one before the caller binds. That residual race is unavoidable without a
// cross-process lock.
func Block(t *testing.T, size int) int {
	t.Helper()

	if size <= 0 || size > blockStride {
		t.Fatalf("testport: block size must be in [1, %d], got %d", blockStride, size)
	}

	// One full lap of the band: if every block is occupied, fail rather than spin.
	for range blocks {
		base := bandStart + int(nextBlock.Add(1)-1)%blocks*blockStride
		if blockFree(base, size) {
			return base
		}
	}

	t.Fatalf("testport: no free block of %d ports in [%d, %d)", size, bandStart, bandEnd)
	return 0
}

// blockFree reports whether every port in the block is bindable. TCP and UDP have
// separate port spaces and callers use both, so a block must be clean for each.
func blockFree(base, size int) bool {
	for port := base; port < base+size; port++ {
		addr := fmt.Sprintf("127.0.0.1:%d", port)

		ln, err := net.Listen("tcp4", addr)
		if err != nil {
			return false
		}
		if err := ln.Close(); err != nil {
			return false
		}

		pc, err := net.ListenPacket("udp4", addr)
		if err != nil {
			return false
		}
		if err := pc.Close(); err != nil {
			return false
		}
	}
	return true
}
