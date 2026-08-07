package storage

import (
	"crypto/sha256"
	"fmt"
)

// GenObjectHash is the shard store's key for an object: the sha256 of
// "bucket/object".
//
// This is a temporary home. It is an S3 concept rather than a storage-protocol
// one, but it cannot live under backend, which internal must not import. It
// moves to model as ObjectHash once the storage protocol stops carrying bucket
// and object.
func GenObjectHash(bucket string, object string) [32]byte {
	objectKey := fmt.Sprintf("%s/%s", bucket, object)
	return sha256.Sum256([]byte(objectKey))
}
