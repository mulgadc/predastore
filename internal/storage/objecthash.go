package storage

import (
	"crypto/sha256"
	"fmt"
)

// GenObjectHash names the shard set an object is stored as: the sha256 of
// "bucket/object". Callers compute it and hand it to storage nodes, which only
// ever see 32 opaque bytes.
//
// This is a temporary home. It is an S3 concept rather than a storage-protocol
// one — nothing in this package uses it — but it cannot live under backend,
// which internal must not import. It belongs in model as ObjectHash.
func GenObjectHash(bucket string, object string) [32]byte {
	objectKey := fmt.Sprintf("%s/%s", bucket, object)
	return sha256.Sum256([]byte(objectKey))
}
