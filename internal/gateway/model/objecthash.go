package model

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// ObjectHash names the shard set an object is stored as: the sha256 of
// "bucket/object". Callers compute it and hand it to storage nodes, which only
// ever see 32 opaque bytes.
func ObjectHash(bucket string, object string) [32]byte {
	objectKey := fmt.Sprintf("%s/%s", bucket, object)
	return sha256.Sum256([]byte(objectKey))
}

// ObjectETag is the entity tag S3 clients see for a whole object: the first
// half of its object hash, hex encoded. It is derived from the name rather
// than the content, so it identifies the object but not its version.
func ObjectETag(bucket string, object string) string {
	hash := ObjectHash(bucket, object)
	return hex.EncodeToString(hash[:16])
}
