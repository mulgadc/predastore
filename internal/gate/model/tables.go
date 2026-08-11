package model

// Table names for global state. The meta replicas are a plain key-value
// store, so this taxonomy is the gate's alone: it composes a table into
// every key it stores and strips it back off every key it scans.
const (
	TableObjects   = "objects"   // Object metadata (hash -> shard locations)
	TableBuckets   = "buckets"   // Bucket metadata
	TableMultipart = "multipart" // Multipart upload metadata (uploadID -> metadata)
	TableParts     = "parts"     // Part metadata (uploadID:partNumber -> part info)
)
