// Package gate is predastore's S3 frontend: the HTTPS listener, the SigV4 +
// IAM middleware chain and the route table. The operations themselves live in
// the handlers subpackage, credential resolution in auth, and shard placement
// in placement. Configuration arrives already parsed, from the root package.
package gate

import (
	"github.com/mulgadc/bluebottle/pkg/ratelimit"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
)

// RS fixes the erasure code the gate places objects with.
type RS struct {
	Data   int
	Parity int
}

// Config is everything one S3 gate runs on: its slice of the product
// configuration, already parsed and resolved, and the wiring the process
// supplies. The on-disk TOML surface belongs to the root package, which
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

	// Addr and Port are the S3 listen address. Zero values default to
	// 0.0.0.0:8443.
	Addr string
	Port int

	// TLSCert and TLSKey are required: the gate only serves HTTPS.
	TLSCert string
	TLSKey  string

	// EnableHTTP2 advertises h2 over ALPN. Off leaves http/1.1 as the only
	// offer, so a client that would otherwise multiplex falls back to a
	// connection pool.
	//
	// Already resolved: the config default is on, and the zero value here is
	// off, so a caller building this by hand supplies it rather than
	// inheriting it.
	EnableHTTP2 bool

	// Meta reaches the replicas holding bucket, object and upload metadata,
	// and Blob the nodes holding shards. Both are required: the gate runs the
	// S3 frontend only, and the process that runs the cluster nodes owns the
	// transports underneath them.
	Meta handlers.MetaClient
	Blob handlers.BlobClient

	// CredProv stands in for the credential chain New resolves from Auth and
	// IAM. Test seam: production leaves it nil.
	CredProv auth.CredentialProvider
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
