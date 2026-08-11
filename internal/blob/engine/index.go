package engine

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// openIndex opens the badger instance backing the store's on-disk index,
// which maps keys to extents and carries the tombstone namespace.
func openIndex(dir string) (*badger.DB, error) {
	return badger.Open(badger.DefaultOptions(dir).WithLoggingLevel(badger.WARNING))
}

// indexGet returns a copy of the value stored under key. A missing key
// surfaces as a wrapped badger.ErrKeyNotFound, which callers branch on.
func (store *Store) indexGet(key []byte) ([]byte, error) {
	var value []byte

	return value, store.index.View(
		func(tx *badger.Txn) error {
			item, err := tx.Get(key)
			if err != nil {
				return fmt.Errorf("getting value: %w", err)
			}
			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("copying value: %w", err)
			}
			value = valCopy
			return nil
		})
}

// indexDelete removes a single key in its own transaction.
func (store *Store) indexDelete(key []byte) error {
	return store.index.Update(
		func(txn *badger.Txn) error {
			return txn.Delete(key)
		})
}

// indexScan iterates over keys with the given prefix and calls fn for each
// key-value pair, inside one read transaction.
func (store *Store) indexScan(prefix []byte, fn func(key, value []byte) error) error {
	return store.index.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := tx.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)

			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			if err := fn(key, value); err != nil {
				return err
			}
		}
		return nil
	})
}
