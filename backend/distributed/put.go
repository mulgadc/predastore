package distributed

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
	"github.com/mulgadc/predastore/backend"
	s3db "github.com/mulgadc/predastore/s3db"
)

// PutObject stores an object using Reed-Solomon encoding across multiple nodes
func (b *Backend) PutObject(ctx context.Context, req *backend.PutObjectRequest) (*backend.PutObjectResponse, error) {
	if req.Bucket == "" {
		return nil, backend.ErrNoSuchBucketError.WithResource(req.Bucket)
	}
	if req.Key == "" {
		return nil, backend.ErrNoSuchKeyError.WithResource(req.Key)
	}

	objectHash := s3db.GenObjectHash(req.Bucket, req.Key)

	objectToShardNodes := ObjectToShardNodes{}

	// Check if existing
	data, err := b.db.Get(objectHash[:])

	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	} else if errors.Is(err, badger.ErrKeyNotFound) {
		// Set the defaults for new object
		objectToShardNodes = ObjectToShardNodes{
			Object:           objectHash,
			DataShardNodes:   make([]uint32, b.rsDataShard),
			ParityShardNodes: make([]uint32, b.rsParityShard),
		}
	} else {
		// Decode existing metadata
		r := bytes.NewReader(data)
		dec := gob.NewDecoder(r)

		if err := dec.Decode(&objectToShardNodes); err != nil {
			return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
		}
	}

	// Write object to a temporary file for putObjectToWAL
	tmpFile, err := os.CreateTemp("", "distributed-put-*")
	if err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Copy body to temp file
	if req.Body != nil {
		_, err = io.Copy(tmpFile, req.Body)
		if err != nil {
			return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
		}
	}
	tmpFile.Close()

	// Split and write to WAL across nodes
	_, _, size, err := b.putObjectToWAL(req.Bucket, tmpFile.Name(), objectHash)
	if err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	objectToShardNodes.Size = size

	// Get hash ring placement
	_, file := filepath.Split(tmpFile.Name())
	key := []byte(fmt.Sprintf("%s/%s", req.Bucket, file))
	hashRingShards, err := b.hashRing.GetClosestN(key, b.rsDataShard+b.rsParityShard)
	if err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	// Record which nodes have data shards
	for i := 0; i < b.rsDataShard; i++ {
		objectToShardNodes.DataShardNodes[i], err = NodeToUint32(hashRingShards[i].String())
		if err != nil {
			return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
		}
	}

	// Record which nodes have parity shards
	for i := 0; i < b.rsParityShard; i++ {
		objectToShardNodes.ParityShardNodes[i], err = NodeToUint32(hashRingShards[b.rsDataShard+i].String())
		if err != nil {
			return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
		}
	}

	// Store object metadata in Badger
	err = b.db.Badger.Update(func(txn *badger.Txn) error {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)

		if err := enc.Encode(objectToShardNodes); err != nil {
			return err
		}

		e := badger.NewEntry(objectHash[:], buf.Bytes())
		return txn.SetEntry(e)
	})

	if err != nil {
		return nil, backend.NewS3Error(backend.ErrInternalError, err.Error(), 500)
	}

	return &backend.PutObjectResponse{
		ETag: generateDistributedETag(req.Bucket, req.Key),
	}, nil
}

// PutObjectFromPath stores an object from a file path (used internally and for testing)
func (b *Backend) PutObjectFromPath(ctx context.Context, bucket, objectPath string) error {
	objectHash := s3db.GenObjectHash(bucket, objectPath)

	objectToShardNodes := ObjectToShardNodes{}

	// Check if existing
	data, err := b.db.Get(objectHash[:])

	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	} else if errors.Is(err, badger.ErrKeyNotFound) {
		// Set the defaults for new object
		objectToShardNodes = ObjectToShardNodes{
			Object:           objectHash,
			DataShardNodes:   make([]uint32, b.rsDataShard),
			ParityShardNodes: make([]uint32, b.rsParityShard),
		}
	} else {
		// Decode existing metadata
		r := bytes.NewReader(data)
		dec := gob.NewDecoder(r)

		if err := dec.Decode(&objectToShardNodes); err != nil {
			return err
		}
	}

	// Split and write to WAL across nodes
	_, _, size, err := b.putObjectToWAL(bucket, objectPath, objectHash)
	if err != nil {
		return err
	}

	objectToShardNodes.Size = size

	// Get hash ring placement
	_, file := filepath.Split(objectPath)
	key := []byte(fmt.Sprintf("%s/%s", bucket, file))
	hashRingShards, err := b.hashRing.GetClosestN(key, b.rsDataShard+b.rsParityShard)
	if err != nil {
		return err
	}

	// Record which nodes have data shards
	for i := 0; i < b.rsDataShard; i++ {
		objectToShardNodes.DataShardNodes[i], err = NodeToUint32(hashRingShards[i].String())
		if err != nil {
			return err
		}
	}

	// Record which nodes have parity shards
	for i := 0; i < b.rsParityShard; i++ {
		objectToShardNodes.ParityShardNodes[i], err = NodeToUint32(hashRingShards[b.rsDataShard+i].String())
		if err != nil {
			return err
		}
	}

	// Store object metadata in Badger
	return b.db.Badger.Update(func(txn *badger.Txn) error {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)

		if err := enc.Encode(objectToShardNodes); err != nil {
			return err
		}

		e := badger.NewEntry(objectHash[:], buf.Bytes())
		return txn.SetEntry(e)
	})
}

// GetFromPath retrieves an object and writes to the provided writer (used for testing)
func (b *Backend) GetFromPath(ctx context.Context, bucket, objectPath string, out *bytes.Buffer) error {
	req := &backend.GetObjectRequest{
		Bucket: bucket,
		Key:    objectPath,
	}

	resp, err := b.GetObject(ctx, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}
