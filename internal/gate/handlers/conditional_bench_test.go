package handlers

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// BenchmarkEvalPreconditions measures the condition check every conditional
// GET, HEAD and copy runs, from one ETag up to the gate's 1 MiB header limit.
func BenchmarkEvalPreconditions(b *testing.B) {
	const etag = `"9b2cf535f27731c974343645a3985328"`
	const other = `"00000000000000000000000000000000"`
	modified := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	since := modified.Add(-time.Hour).Format(http.TimeFormat)

	// 28,000 tags of 36 bytes fill a field to just under the 1 MiB limit.
	const nearLimit = 28_000
	tags := func(n int, last string) []string {
		list := make([]string, 0, n)
		for range n - 1 {
			list = append(list, other)
		}
		return append(list, last)
	}
	field := func(values ...string) http.Header { return http.Header{"If-Match": values} }

	cases := []struct {
		name   string
		header http.Header
	}{
		{"if-match", field(etag)},
		{"if-none-match-star", http.Header{"If-None-Match": {"*"}}},
		{"if-match-and-unmodified", http.Header{"If-Match": {etag}, "If-Unmodified-Since": {since}}},
		{"if-modified-since", http.Header{"If-Modified-Since": {since}}},
		{"list-16", field(strings.Join(tags(16, etag), ", "))},
		{"list-200", field(strings.Join(tags(200, etag), ", "))},
		{"list-1MiB", field(strings.Join(tags(nearLimit, etag), ", "))},
		{"list-1MiB-miss", field(strings.Join(tags(nearLimit, other), ", "))},
		{"fields-28000", field(tags(nearLimit, etag)...)},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				evalPreconditions(etag, true, modified, true, readPreconditions(c.header))
			}
		})
	}
}
