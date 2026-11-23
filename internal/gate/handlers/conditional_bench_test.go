package handlers

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// BenchmarkEvalPreconditions measures the condition check every conditional
// GET, HEAD and copy runs, from one ETag up to a list near the header limit.
func BenchmarkEvalPreconditions(b *testing.B) {
	const etag = `"9b2cf535f27731c974343645a3985328"`
	modified := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	since := modified.Add(-time.Hour).Format(http.TimeFormat)

	long := func(n int) string {
		tags := make([]string, 0, n+1)
		for range n {
			tags = append(tags, `"00000000000000000000000000000000"`)
		}
		return strings.Join(append(tags, etag), ", ")
	}

	cases := []struct {
		name string
		p    preconditions
	}{
		{name: "if-match", p: preconditions{ifMatch: etag}},
		{name: "if-none-match-star", p: preconditions{ifNoneMatch: "*"}},
		{name: "if-match-and-unmodified", p: preconditions{ifMatch: etag, ifUnmodifiedSince: since}},
		{name: "if-modified-since", p: preconditions{ifModifiedSince: since}},
		{name: "list-16", p: preconditions{ifMatch: long(15)}},
		{name: "list-200", p: preconditions{ifMatch: long(199)}},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				evalPreconditions(etag, true, modified, true, c.p)
			}
		})
	}
}
