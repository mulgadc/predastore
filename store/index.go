package store

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// indexDB is the badger handle behind the store's on-disk index, mapping
// shard keys to extents.
type indexDB struct {
	Badger *badger.DB
}

// newIndexDB opens the index in dir.
func newIndexDB(dir string) (idx *indexDB, err error) {
	idx = &indexDB{}
	idx.Badger, err = badger.Open(badger.DefaultOptions(dir).WithLoggingLevel(badger.WARNING))
	if err != nil {
		return nil, err
	}
	return idx, nil
}

func (idx *indexDB) Close() (err error) {
	err = idx.Badger.Close()
	if err != nil {
		return err
	}
	return nil
}

func (idx *indexDB) Exists(key []byte) (bool, error) {
	var exists bool
	err := idx.Badger.View(
		func(tx *badger.Txn) error {
			if val, err := tx.Get(key); err != nil {
				return err
			} else if val != nil {
				exists = true
			}
			return nil
		})
	if errors.Is(err, badger.ErrKeyNotFound) {
		err = nil
	}
	return exists, err
}

func (idx *indexDB) Get(key []byte) ([]byte, error) {
	var value []byte

	return value, idx.Badger.View(
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

func (idx *indexDB) Set(key, value []byte) error {
	return idx.Badger.Update(
		func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
}

func (idx *indexDB) Delete(key []byte) error {
	return idx.Badger.Update(
		func(txn *badger.Txn) error {
			return txn.Delete(key)
		})
}

func (idx *indexDB) ListKeys(prefix []byte) ([][]byte, error) {
	var keys [][]byte
	err := idx.Badger.View(
		func(tx *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			it := tx.NewIterator(opts)
			defer it.Close()

			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				key := item.Key()
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)
				keys = append(keys, keyCopy)
			}
			return nil
		})
	return keys, err
}

// Scan iterates over keys with the given prefix and calls the callback for each key-value pair.
func (idx *indexDB) Scan(prefix []byte, fn func(key, value []byte) error) error {
	return idx.Badger.View(func(tx *badger.Txn) error {
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
