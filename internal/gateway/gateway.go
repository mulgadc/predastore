// Package gateway is predastore's S3 frontend: the TOML configuration, the
// HTTPS listener, the SigV4 + IAM middleware chain and the route table. The
// operations themselves live in the handlers subpackage, credential resolution
// in auth, and shard placement in placement.
package gateway

import (
	"github.com/mulgadc/predastore/internal/gateway/auth"
	"github.com/mulgadc/predastore/internal/gateway/handlers"
	"github.com/mulgadc/predastore/internal/topology"
	"github.com/mulgadc/predastore/ratelimit"
)

type ACL struct {
	AccessKeyId     string `toml:"access_key_id"`
	SecretAccessKey string `toml:"secret_access_key"`
	Owner           string `toml:"owner"`
	Permissions     int    `toml:"permissions"`
}

type RS struct {
	Data   int `toml:"data"`
	Parity int `toml:"parity"`
}

// Compaction holds tuning for the QUIC shard store's background compactor.
type Compaction struct {
	IntervalSeconds int `toml:"interval_seconds"`
}

type Config struct {
	ConfigPath string // Path to config file
	Version    string `toml:"version"`
	Region     string `toml:"region"`

	RS RS `toml:"rs"`

	// Cluster topology: hosts are processes owning a socket and a data
	// directory, cluster nodes are roles pinned to hosts. Everything
	// per-node derives from the host base and the node id.
	Hosts        []topology.Host `toml:"host"`
	ClusterNodes []topology.Node `toml:"node"`

	// Compaction tuning for the QUIC shard store (optional; store defaults apply).
	Compaction Compaction `toml:"compaction"`

	Buckets []handlers.BucketConfig `toml:"buckets"`

	// TODO: Move to IAM
	Auth                  []auth.Entry `toml:"auth"`
	AllowAnonymousListing bool         `toml:"allow_anonymous_listing"`
	AllowAnonymousAccess  bool         `toml:"allow_anonymous_access"`

	// IAM authentication via NATS KV (optional, enables multi-account S3 access)
	IAM *auth.IAMConfig `toml:"iam"`

	// The gateway's listen address comes from ServerConfig, not this file: a
	// `host` key here would collide with the [[host]] topology table, which
	// TOML rejects outright.

	Debug          bool   `toml:"debug"`
	BasePath       string `toml:"base_path"`
	DisableLogging bool   `toml:"disable_logging"`

	// API request throttling
	RateLimit ratelimit.Config `toml:"ratelimit"`
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

// storageNodeIDs returns the ids of the cluster's shard-storage nodes: the
// members the gateway places shards across.
func (s3 *Config) storageNodeIDs() []int {
	var ids []int
	for _, n := range s3.ClusterNodes {
		if n.Role == topology.RoleShardStorage {
			ids = append(ids, n.ID)
		}
	}
	return ids
}

func New(ConfigSettings *Config) *Config {
	return ConfigSettings
}
