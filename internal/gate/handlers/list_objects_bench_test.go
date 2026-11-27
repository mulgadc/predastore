package handlers

import (
	"context"
	"slices"
	"time"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
)

var listBenchSizes = []int{1_000, 100_000}

// listBenchBucket is a bucket of n objects with their placement rows, on a
// real meta FSM.
func listBenchBucket(b *testing.B, n int) (http.Handler, *fsmMeta, []string) {
	b.Helper()
	mc := newFSMMeta(b)
	keys := numberedKeys(n)
	mc.storeListing(b, "bkt", keys, true)
	return ListObjects(mc, bucketCache()), mc, keys
}

func benchListPage(b *testing.B, h http.Handler, query url.Values) {
	b.Helper()
	for b.Loop() {
		if rr := listPage(h, query); rr.Code != http.StatusOK {
			b.Fatalf("status %d: %s", rr.Code, rr.Body.String())
		}
	}
}

func BenchmarkListObjectsFirstPage(b *testing.B) {
	for _, n := range listBenchSizes {
		h, _, _ := listBenchBucket(b, n)
		for _, maxKeys := range []int{1, 1000} {
			b.Run(fmt.Sprintf("keys=%d/max-keys=%d", n, maxKeys), func(b *testing.B) {
				benchListPage(b, h, url.Values{"list-type": {"2"}, "max-keys": {strconv.Itoa(maxKeys)}})
			})
		}
	}
}

func BenchmarkListObjectsMiddlePage(b *testing.B) {
	for _, n := range listBenchSizes {
		h, _, keys := listBenchBucket(b, n)
		b.Run(fmt.Sprintf("keys=%d/max-keys=1000", n), func(b *testing.B) {
			benchListPage(b, h, url.Values{
				"list-type": {"2"}, "max-keys": {"1000"},
				"continuation-token": {base64.StdEncoding.EncodeToString([]byte(keys[n/2]))},
			})
		})
	}
}

func BenchmarkListObjectsFullWalk(b *testing.B) {
	for _, n := range listBenchSizes {
		h, _, _ := listBenchBucket(b, n)
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			for b.Loop() {
				query := url.Values{"list-type": {"2"}, "max-keys": {"1000"}}
				for {
					rr := listPage(h, query)
					if rr.Code != http.StatusOK {
						b.Fatalf("status %d: %s", rr.Code, rr.Body.String())
					}
					page := parsePage(b, rr.Body.Bytes())
					if !page.IsTruncated {
						break
					}
					query.Set("continuation-token", page.NextContinuationToken)
				}
			}
		})
	}
}

// BenchmarkMetaApplyDuringListing measures one metadata write on a replica
// while four clients list a large bucket on it.
func BenchmarkMetaApplyDuringListing(b *testing.B) {
	const n = 100_000
	h, mc, _ := listBenchBucket(b, n)
	b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) { writeDuringListing(b, h, mc) })
}

// writeDuringListing times one metadata put per op while four clients list
// the first page in a loop, and reports the median and the 99th percentile.
func writeDuringListing(b *testing.B, h http.Handler, mc MetaClient) {
	b.Helper()
	var stop atomic.Bool
	var listed, failed atomic.Int64
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for !stop.Load() {
				if listPage(h, url.Values{"list-type": {"2"}, "max-keys": {"1000"}}).Code != http.StatusOK {
					failed.Add(1)
				}
				listed.Add(1)
			}
		})
	}
	var lat []time.Duration
	i := 0
	for b.Loop() {
		i++
		start := time.Now()
		if err := mc.Put(context.Background(), "bench/apply/"+strconv.Itoa(i), []byte("v")); err != nil {
			b.Fatal(err)
		}
		lat = append(lat, time.Since(start))
	}
	stop.Store(true)
	wg.Wait()
	if failed.Load() > 0 || listed.Load() == 0 {
		b.Fatalf("listings: %d sent, %d failed", listed.Load(), failed.Load())
	}
	slices.Sort(lat)
	b.ReportMetric(float64(lat[len(lat)/2].Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(lat[min(len(lat)-1, len(lat)*99/100)].Nanoseconds()), "p99-ns")
}

// BenchmarkListObjectsAfterDeletes lists the first page of a bucket whose
// first keys were just deleted, so the scan passes over their tombstones.
func BenchmarkListObjectsAfterDeletes(b *testing.B) {
	const n, deleted = 100_000, 99_000
	h, mc, keys := listBenchBucket(b, n)
	cmds := make([]meta.Command, 0, 1000)
	for _, key := range keys[:deleted] {
		cmds = append(cmds, meta.Command{Type: meta.CommandDelete, Key: []byte(TableKey(model.TableObjects, objectARN("bkt", key)))})
		if len(cmds) == cap(cmds) {
			mc.apply(cmds...)
			cmds = cmds[:0]
		}
	}
	mc.apply(cmds...)
	b.Run(fmt.Sprintf("keys=%d/deleted=%d/max-keys=1000", n, deleted), func(b *testing.B) {
		benchListPage(b, h, url.Values{"list-type": {"2"}, "max-keys": {"1000"}})
	})
}
