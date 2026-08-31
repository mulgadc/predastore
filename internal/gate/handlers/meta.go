package handlers

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
)

// errNoMetaClient is returned when a gate built without a meta client is
// asked for work that needs one. It surfaces as InternalError rather than a
// nil-pointer panic in the handler.
var errNoMetaClient = errors.New("gate has no meta client")

// MetaClient is the slice of the meta client the handlers use. It is an
// interface only so tests can stand a map in for a raft cluster; production
// always holds a *meta.Client.
type MetaClient interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) error
	PutMax(ctx context.Context, key string, value []byte, epoch uint64) error
	Delete(ctx context.Context, key string) error
	Scan(ctx context.Context, prefix string, limit int) ([]meta.Item, error)
}

var _ MetaClient = (*meta.Client)(nil)

// arnObjectPrefix starts the key an object's hash is listed under, so that a
// bucket's contents are one prefix scan: arn:aws:s3:::<bucket>/<key>.
const arnObjectPrefix = "arn:aws:s3:::"

// objectARN composes the listing key for an object.
func objectARN(bucket, key string) string { return arnObjectPrefix + bucket + "/" + key }

// ObjectHashOfKey recovers the object hash from a stored objects-table key, and
// reports false for every other row the table holds.
//
// The table mixes whole-object placement records, keyed by a raw 32-byte hash,
// with textual rows: the ARN listing, delete tombstones and in-flight multipart
// parts. One of those can be exactly 32 bytes long, so length alone is not a
// discriminator and the reserved prefixes are checked as well.
func ObjectHashOfKey(stored string) ([32]byte, bool) {
	key, found := strings.CutPrefix(stored, tablePrefix(model.TableObjects))
	if !found || len(key) != sha256.Size {
		return [32]byte{}, false
	}
	for _, reserved := range []string{arnObjectPrefix, deletedObjectPrefix, partKeyPrefix} {
		if strings.HasPrefix(key, reserved) {
			return [32]byte{}, false
		}
	}

	return [32]byte([]byte(key)), true
}

// tablePrefix is what a table contributes to the front of a stored key.
func tablePrefix(table string) string { return table + "/" }

// TableKey composes the stored key for a table row. Keys may hold arbitrary
// bytes — object metadata is keyed by a raw sha256 — so this concatenates
// rather than formats.
func TableKey(table, key string) string { return tablePrefix(table) + key }

// metaGet reads one row of a table.
func metaGet(ctx context.Context, mc MetaClient, table, key string) ([]byte, error) {
	if mc == nil {
		return nil, errNoMetaClient
	}
	return mc.Get(ctx, TableKey(table, key))
}

// metaPut writes one row of a table.
func metaPut(ctx context.Context, mc MetaClient, table, key string, value []byte) error {
	if mc == nil {
		return errNoMetaClient
	}
	return mc.Put(ctx, TableKey(table, key), value)
}

func metaPutMax(ctx context.Context, mc MetaClient, table, key string, value []byte, epoch uint64) error {
	if mc == nil {
		return errNoMetaClient
	}
	return mc.PutMax(ctx, TableKey(table, key), value, epoch)
}

// metaDelete removes one row of a table.
func metaDelete(ctx context.Context, mc MetaClient, table, key string) error {
	if mc == nil {
		return errNoMetaClient
	}
	return mc.Delete(ctx, TableKey(table, key))
}

// metaScan lists rows of a table whose key starts with prefix. The state
// replicas return keys exactly as stored, so the table prefix is stripped back
// off here and callers see the same keys they wrote.
func metaScan(ctx context.Context, mc MetaClient, table, prefix string, limit int) ([]meta.Item, error) {
	if mc == nil {
		return nil, errNoMetaClient
	}
	items, err := mc.Scan(ctx, TableKey(table, prefix), limit)
	if err != nil {
		return nil, err
	}
	for i := range items {
		items[i].Key = strings.TrimPrefix(items[i].Key, tablePrefix(table))
	}
	return items, nil
}
