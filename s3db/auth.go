package s3db

// SigV4 credential-scope defaults when ServerConfig leaves them unset.
const (
	DefaultRegion = "us-east-1"

	// DefaultService is "s3", not "s3db", so requests take sigv4's header content-hash
	// path (the client sends X-Amz-Content-Sha256) rather than body hashing. Temporary —
	// SigV4 is being removed from intra-cluster comms.
	DefaultService = "s3"
)
