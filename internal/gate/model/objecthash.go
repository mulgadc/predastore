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

// VersionHash names the shard set one version of an object is stored as.
//
// Each version is its own live shard set rather than a retained generation of
// one: the blob nodes reclaim a superseded generation on age alone, by design,
// so a version held that way would be destroyed minutes after it was written.
//
// The NUL separator is load-bearing. A key may hold any byte sequence, and
// without it a caller could name a key that hashes onto another key's version.
func VersionHash(bucket, object, versionID string) [32]byte {
	return sha256.Sum256([]byte(bucket + "/" + object + "\x00" + versionID))
}
