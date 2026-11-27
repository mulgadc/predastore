package handlers

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/mulgadc/predastore/internal/meta"
)

// largeListKeys is the size of the large bucket, all under one prefix.
const largeListKeys = 1_000_000

var (
	largeOnce sync.Once
	largeMC   *countingMeta
	largeKeys []string
)

// largeBucket builds one bucket of a million objects under vol/, with their
// placement rows, once per test binary: building it costs more than any run.
func largeBucket(b *testing.B) (*countingMeta, []string) {
	b.Helper()
	largeOnce.Do(func() {
		dir, err := os.MkdirTemp("", "list-large-") //nolint:usetesting // Built once and shared by every benchmark, so it outlives any one b.TempDir.
		if err != nil {
			b.Fatal(err)
		}
		db, err := badger.Open(badger.DefaultOptions(dir).WithLoggingLevel(badger.WARNING))
		if err != nil {
			b.Fatal(err)
		}
		fsm, err := meta.OpenFSM(db)
		if err != nil {
			b.Fatal(err)
		}
		mc := &fsmMeta{fsm: fsm}
		keys := make([]string, largeListKeys)
		for i := range keys {
			keys[i] = fmt.Sprintf("vol/%08d", i)
		}
		mc.storeListing(b, "bkt", keys, true)
		largeMC, largeKeys = &countingMeta{fsmMeta: mc}, keys
	})
	if largeMC == nil {
		b.Fatal("the large bucket was not built")
	}
	return largeMC, largeKeys
}

// benchLargePage lists one page per op and reports the rows the scans read.
func benchLargePage(b *testing.B, mc *countingMeta, query url.Values) {
	b.Helper()
	h := ListObjects(mc, bucketCache())
	mc.reset()
	for b.Loop() {
		if rr := listPage(h, query); rr.Code != http.StatusOK {
			b.Fatalf("status %d: %s", rr.Code, rr.Body.String())
		}
	}
	b.ReportMetric(float64(mc.rows)/float64(b.N), "rows/op")
}

func BenchmarkListObjectsLarge(b *testing.B) {
	mc, keys := largeBucket(b)
	prefix := fmt.Sprintf("keys=%d", largeListKeys)
	b.Run(prefix+"/first-page", func(b *testing.B) {
		benchLargePage(b, mc, url.Values{"list-type": {"2"}, "prefix": {"vol/"}, "max-keys": {"1000"}})
	})
	b.Run(prefix+"/page-after-900000", func(b *testing.B) {
		benchLargePage(b, mc, url.Values{
			"list-type": {"2"}, "prefix": {"vol/"}, "max-keys": {"1000"},
			"continuation-token": {base64.StdEncoding.EncodeToString([]byte(keys[900_000]))},
		})
	})
	b.Run(prefix+"/full-walk", func(b *testing.B) {
		h := ListObjects(mc, bucketCache())
		mc.reset()
		pages := 0
		for b.Loop() {
			query := url.Values{"list-type": {"2"}, "prefix": {"vol/"}, "max-keys": {"1000"}}
			for {
				rr := listPage(h, query)
				if rr.Code != http.StatusOK {
					b.Fatalf("status %d: %s", rr.Code, rr.Body.String())
				}
				pages++
				page := parsePage(b, rr.Body.Bytes())
				if !page.IsTruncated {
					break
				}
				query.Set("continuation-token", page.NextContinuationToken)
			}
		}
		b.ReportMetric(float64(mc.rows)/float64(pages), "rows/page")
	})
}

// BenchmarkMetaWriteDuringLargeListing is a metadata write while four clients
// list the first page of the large bucket.
func BenchmarkMetaWriteDuringLargeListing(b *testing.B) {
	mc, _ := largeBucket(b)
	b.Run(fmt.Sprintf("keys=%d", largeListKeys), func(b *testing.B) {
		writeDuringListing(b, ListObjects(mc, bucketCache()), mc)
	})
}
