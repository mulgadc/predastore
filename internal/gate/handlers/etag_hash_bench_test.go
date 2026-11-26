package handlers

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/minio/crc64nvme"
	"github.com/mulgadc/predastore/internal/gate/model"
)

// cpuTime is the user and system CPU this process has used so far.
func cpuTime(b *testing.B) time.Duration {
	b.Helper()
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		b.Fatalf("getrusage: %v", err)
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// runStreams runs b.N calls of op across streams goroutines and reports the
// process CPU per call beside the wall time, since a hasher that packs
// streams onto fewer cores shows up in CPU and not in latency.
func runStreams(b *testing.B, streams int, op func(i int)) {
	b.Helper()
	var next atomic.Int64
	var wg sync.WaitGroup
	start := cpuTime(b)
	b.ResetTimer()
	for range streams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= b.N {
					return
				}
				op(i)
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(cpuTime(b)-start)/float64(b.N), "cpu-ns/op")
}

// BenchmarkPartETagHasher is the ETag hash alone over 4 MiB bodies written in
// 32 KiB pieces, as io.Copy feeds the tee, at one stream and at viperblock's
// sixteen upload workers.
func BenchmarkPartETagHasher(b *testing.B) {
	body := bytes.Repeat([]byte("x"), 4<<20)
	for _, streams := range []int{1, 16} {
		b.Run(fmt.Sprintf("4MiB/streams=%d", streams), func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			runStreams(b, streams, func(int) {
				h := model.NewPartETagHasher()
				for off := 0; off < len(body); off += 32 << 10 {
					_, _ = h.Write(body[off : off+32<<10])
				}
				_ = model.PartETagFrom(h)
			})
		})
	}
}

// BenchmarkPutObjectETagHash is the whole PutObject handler over real blob
// stores for a viperblock chunk, one at a time and sixteen at once, with the
// process CPU each PUT costs. The ebs mode sends the chunk as viperblock does,
// with its CRC64NVME and the EBS opt-in header.
func BenchmarkPutObjectETagHash(b *testing.B) {
	body := bytes.Repeat([]byte("x"), 4<<20+10)
	sum := make([]byte, 8)
	binary.BigEndian.PutUint64(sum, crc64nvme.Checksum(body))
	modes := []struct {
		name   string
		header http.Header
	}{
		{"plain", nil},
		{"ebs", http.Header{
			"X-Spinifex-Ebs":           {"true"},
			"X-Amz-Checksum-Crc64nvme": {base64.StdEncoding.EncodeToString(sum)},
		}},
	}
	for _, mode := range modes {
		for _, streams := range []int{1, 16} {
			b.Run(fmt.Sprintf("RS(2,1)/4MiB/%s/streams=%d", mode.name, streams), func(b *testing.B) {
				quietLogs(b)
				g := newRealStoreGate(b, rsProfiles[0])
				h := PutObject(g.mc, g.bc, g.ring, g.cache, g.cfg)
				b.SetBytes(int64(len(body)))
				b.ReportAllocs()
				runStreams(b, streams, func(i int) {
					key := fmt.Sprintf("k%09d", i)
					if rr := g.serve(h, http.MethodPut, "/bkt/"+key, key, body, mode.header); rr.Code != http.StatusOK {
						b.Errorf("put: %d %s", rr.Code, rr.Body.String())
					}
				})
			})
		}
	}
}
