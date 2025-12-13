package s3db

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

type S3DB struct {
	Badger *badger.DB
}

// DB functions
func New(dir string) (s3db *S3DB, err error) {
	s3db = &S3DB{}
	s3db.Badger, err = badger.Open(badger.DefaultOptions(dir).WithLoggingLevel(badger.WARNING))
	if err != nil {
		return nil, err
	}
	return s3db, nil
}

func (s3db *S3DB) Close() (err error) {
	err = s3db.Badger.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s3db *S3DB) Exists(key []byte) (bool, error) {
	var exists bool
	err := s3db.Badger.View(
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

func (s3db *S3DB) Get(key []byte) ([]byte, error) {
	var value []byte

	return value, s3db.Badger.View(
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

func (s3db *S3DB) Set(key, value []byte) error {
	return s3db.Badger.Update(
		func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
}

func (s3db *S3DB) Delete(key []byte) error {
	return s3db.Badger.Update(
		func(txn *badger.Txn) error {
			return txn.Delete(key)
		})
}
