// Package gate is predastore's S3 frontend: the HTTPS listener, the SigV4 +
// IAM middleware chain and the route table. The operations themselves live in
// the handlers subpackage, credential resolution in auth, and shard placement
// in placement. Configuration arrives already parsed, from the root package.
package gate

import (
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/pkg/ratelimit"
)

// RS fixes the erasure code the gate places objects with.
type RS struct {
	Data   int
	Parity int
}

// Config is the gate's slice of the product configuration, already parsed
// and resolved. The on-disk TOML surface belongs to the root package, which
// converts it to this; the gate sees settings, never a file.
type Config struct {
	Region string

	RS RS

	// Buckets are the config-defined buckets.
	Buckets []handlers.BucketConfig

	// BlobNodeIDs are the blob nodes the placement ring spreads
	// objects across.
	BlobNodeIDs []config.NodeID

	// TODO: Move to IAM
	Auth []auth.Entry

	// IAM authentication via NATS KV (optional, enables multi-account S3 access)
	IAM *auth.IAMConfig

	// API request throttling
	RateLimit ratelimit.Config
}

// handlerConfig is the slice of the config the handlers read.
func (s3 *Config) handlerConfig() handlers.Config {
	return handlers.Config{
		Region:       s3.Region,
		DataShards:   s3.RS.Data,
		ParityShards: s3.RS.Parity,
		Buckets:      s3.Buckets,
	}
}
