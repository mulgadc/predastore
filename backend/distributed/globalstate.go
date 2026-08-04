package distributed

// Table names for global state.
const (
	TableObjects   = "objects"   // Object metadata (hash -> shard locations)
	TableBuckets   = "buckets"   // Bucket metadata
	TableMultipart = "multipart" // Multipart upload metadata (uploadID -> metadata)
	TableParts     = "parts"     // Part metadata (uploadID:partNumber -> part info)
)
