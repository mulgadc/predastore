package distributed

import (
	"strings"

	"github.com/mulgadc/predastore/internal/state"
)

// Table names for global state. The state replicas are a plain key-value
// store, so this taxonomy is the gateway's alone: it composes a table into
// every key it stores and strips it back off every key it scans.
const (
	TableObjects   = "objects"   // Object metadata (hash -> shard locations)
	TableBuckets   = "buckets"   // Bucket metadata
	TableMultipart = "multipart" // Multipart upload metadata (uploadID -> metadata)
	TableParts     = "parts"     // Part metadata (uploadID:partNumber -> part info)
)

// tablePrefix is what a table contributes to the front of a stored key.
func tablePrefix(table string) string { return table + "/" }

// tableKey composes the stored key for a table row. Keys may hold arbitrary
// bytes — object metadata is keyed by a raw sha256 — so this concatenates
// rather than formats.
func tableKey(table, key string) string { return tablePrefix(table) + key }

// stateGet reads one row of a table.
func (b *Backend) stateGet(table, key string) ([]byte, error) {
	return b.globalState.Get(tableKey(table, key))
}

// statePut writes one row of a table.
func (b *Backend) statePut(table, key string, value []byte) error {
	return b.globalState.Put(tableKey(table, key), value)
}

// stateDelete removes one row of a table.
func (b *Backend) stateDelete(table, key string) error {
	return b.globalState.Delete(tableKey(table, key))
}

// stateScan lists rows of a table whose key starts with prefix. The state
// replicas return keys exactly as stored, so the table prefix is stripped back
// off here and callers see the same keys they wrote.
func (b *Backend) stateScan(table, prefix string, limit int) ([]state.Item, error) {
	items, err := b.globalState.Scan(tableKey(table, prefix), limit)
	if err != nil {
		return nil, err
	}
	for i := range items {
		items[i].Key = strings.TrimPrefix(items[i].Key, tablePrefix(table))
	}
	return items, nil
}
