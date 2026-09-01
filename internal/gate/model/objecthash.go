package model

import (
	"crypto/sha256"
	"fmt"
)

// ObjectHash names the shard set an object is stored as: the sha256 of
// "bucket/object". Callers compute it and hand it to blob nodes, which only
// ever see 32 opaque bytes.
func ObjectHash(bucket string, object string) [32]byte {
	objectKey := fmt.Sprintf("%s/%s", bucket, object)
	return sha256.Sum256([]byte(objectKey))
}
