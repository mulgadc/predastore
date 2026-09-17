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
	TableMultipart  = "multipart"  // Multipart upload metadata (uploadID -> metadata)
	TableParts      = "parts"      // Part metadata (uploadID:partNumber -> part info)
)
