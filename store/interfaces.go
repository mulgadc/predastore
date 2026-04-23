package store

import "io"

type ObjectStore interface {
	io.Closer

	Lookup(key string) (obj ObjectReader, err error)
	Append(key string, size int64) (obj ObjectWriter, err error)
	Delete(key string) error
}

type ObjectReader interface {
	io.ReaderAt
	io.WriterTo
	io.Closer

	Size() int64
}

type ObjectWriter interface {
	io.WriteCloser
	io.ReaderFrom
}
