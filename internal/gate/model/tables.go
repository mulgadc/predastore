package model

// Table names for global state. The meta replicas are a plain key-value
// store, so this taxonomy is the gate's alone: it composes a table into
// every key it stores and strips it back off every key it scans.
const (
	TableObjects = "objects" // Object metadata (hash -> shard locations)
	TableBuckets = "buckets" // Bucket metadata
	// TableBucketTags holds bucket tag sets separately from the bucket record,
	// so a config-declared bucket — which has no record — can still be tagged
	// without being given one, and an owner with it.
	TableBucketTags = "buckettags" // Bucket tags (bucket -> tag set)
	// TableBucketVersioning holds each bucket's versioning state, separately
	// from the bucket record for the same reason the tags are: a config-declared
	// bucket has no record to put it in.
	TableBucketVersioning = "bucketversioning" // Bucket versioning state (bucket -> Enabled|Suspended)
	// TableObjectVersions indexes the versions of every key. Its rows sort
	// newest-first within a key, so one prefix scan answers both "what is the
	// current version of this key" and ListObjectVersions over a whole bucket.
	TableObjectVersions = "objectversions" // Object versions (bucket/key -> version records)
	TableMultipart      = "multipart"      // Multipart upload metadata (uploadID -> metadata)
	TableParts          = "parts"          // Part metadata (uploadID:partNumber -> part info)
)
