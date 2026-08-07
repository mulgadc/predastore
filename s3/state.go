package s3

import (
	"errors"
	"strings"

	"github.com/mulgadc/predastore/internal/state"
)

// errNoStateClient is returned when a gateway built without a state client is
// asked for work that needs one. It surfaces as InternalError rather than a
// nil-pointer panic in the handler.
var errNoStateClient = errors.New("gateway has no state client")

// stateStore is the slice of the state client the gateway uses. It is an
// interface only so tests can stand a map in for a raft cluster; production
// always holds a *state.Client.
type stateStore interface {
	Get(key string) ([]byte, error)
	Put(key string, value []byte) error
	Delete(key string) error
	Scan(prefix string, limit int) ([]state.Item, error)
}

var _ stateStore = (*state.Client)(nil)

// arnObjectPrefix starts the key an object's hash is listed under, so that a
// bucket's contents are one prefix scan: arn:aws:s3:::<bucket>/<key>.
const arnObjectPrefix = "arn:aws:s3:::"

// objectARN composes the listing key for an object.
func objectARN(bucket, key string) string { return arnObjectPrefix + bucket + "/" + key }

// tablePrefix is what a table contributes to the front of a stored key.
func tablePrefix(table string) string { return table + "/" }

// tableKey composes the stored key for a table row. Keys may hold arbitrary
// bytes — object metadata is keyed by a raw sha256 — so this concatenates
// rather than formats.
func tableKey(table, key string) string { return tablePrefix(table) + key }

// stateGet reads one row of a table.
func (s *HTTP2Server) stateGet(table, key string) ([]byte, error) {
	if s.globalState == nil {
		return nil, errNoStateClient
	}
	return s.globalState.Get(tableKey(table, key))
}

// statePut writes one row of a table.
func (s *HTTP2Server) statePut(table, key string, value []byte) error {
	if s.globalState == nil {
		return errNoStateClient
	}
	return s.globalState.Put(tableKey(table, key), value)
}

// stateDelete removes one row of a table.
func (s *HTTP2Server) stateDelete(table, key string) error {
	if s.globalState == nil {
		return errNoStateClient
	}
	return s.globalState.Delete(tableKey(table, key))
}

// stateScan lists rows of a table whose key starts with prefix. The state
// replicas return keys exactly as stored, so the table prefix is stripped back
// off here and callers see the same keys they wrote.
func (s *HTTP2Server) stateScan(table, prefix string, limit int) ([]state.Item, error) {
	if s.globalState == nil {
		return nil, errNoStateClient
	}
	items, err := s.globalState.Scan(tableKey(table, prefix), limit)
	if err != nil {
		return nil, err
	}
	for i := range items {
		items[i].Key = strings.TrimPrefix(items[i].Key, tablePrefix(table))
	}
	return items, nil
}
