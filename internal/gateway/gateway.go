// Package gateway is predastore's S3 frontend: the HTTPS listener, the SigV4 +
// IAM middleware chain and the route table. The operations themselves live in
// the handlers subpackage, credential resolution in auth, and shard placement
// in placement. Configuration arrives already parsed, from the root package.
package gateway

import (
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gateway/auth"
	"github.com/mulgadc/predastore/internal/gateway/handlers"
	"github.com/mulgadc/predastore/pkg/ratelimit"
)

// RS fixes the erasure code the gateway places objects with.
type RS struct {
	Data   int
	Parity int
}

// Config is the gateway's slice of the product configuration, already parsed
// and resolved. The on-disk TOML surface belongs to the root package, which
// converts it to this; the gateway sees settings, never a file.
type Config struct {
	Region string

	RS RS

	// Buckets are the config-defined buckets, with pathnames already resolved
	// to absolute directories.
	Buckets []handlers.BucketConfig

	// StorageNodeIDs are the shard-storage nodes the placement ring spreads
	// objects across.
	StorageNodeIDs []config.NodeID

	// TODO: Move to IAM
	Auth []auth.Entry

	// IAM authentication via NATS KV (optional, enables multi-account S3 access)
	IAM *auth.IAMConfig

	Debug          bool
	DisableLogging bool

	// API request throttling
	RateLimit ratelimit.Config
}

// handlerConfig is the slice of the config the handlers read. Shard counts fall
// back to the erasure-code defaults so a config that omits them still places
// objects the way the rest of the cluster expects.
func (s3 *Config) handlerConfig() handlers.Config {
	cfg := handlers.Config{
		Region:       s3.Region,
		DataShards:   s3.RS.Data,
		ParityShards: s3.RS.Parity,
		Buckets:      s3.Buckets,
	}
	if cfg.DataShards == 0 {
		cfg.DataShards = handlers.DefaultDataShards
	}
	if cfg.ParityShards == 0 {
		cfg.ParityShards = handlers.DefaultParityShards
	}
	return cfg
}
