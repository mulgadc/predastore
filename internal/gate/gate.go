// Package gate is predastore's S3 frontend: the HTTPS listener, the SigV4 +
// IAM middleware chain and the route table. The operations themselves live in
// the handlers subpackage, credential resolution in auth, and shard placement
// in placement. Configuration arrives already parsed, from the root package.
package gate

import (
	"context"
	"time"

	"github.com/mulgadc/bluebottle/pkg/ratelimit"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/repair"
	"github.com/mulgadc/predastore/internal/meta"
)

// MetaClient is the metadata surface a gate needs: everything the request
// handlers read and write, plus the cursor scan the repair sweep pages the
// object table with, which no request path asks for.
type MetaClient interface {
	handlers.MetaClient
	ScanFrom(ctx context.Context, prefix, after string, limit int) ([]meta.Item, error)
}

// BlobClient is the shard surface a gate needs: the request path's reads and
// writes, plus the stat repair asks a node which generation it holds with.
type BlobClient interface {
	handlers.BlobClient
	Stat(ctx context.Context, node config.NodeID, req blob.StatRequest) (*blob.StatResponse, error)
}

var (
	_ MetaClient = (*meta.Client)(nil)
	_ BlobClient = (*blob.Client)(nil)

	_ repair.MetaClient = MetaClient(nil)
	_ repair.BlobClient = BlobClient(nil)
)

// RS fixes the erasure code the gate places objects with.
type RS struct {
	Data   int
	Parity int

	// DegradedWrites acknowledges a write once Data shards are durable rather
	// than requiring the full stripe.
	DegradedWrites bool
}

// RepairConfig tunes the background repair sweep. It is off by default: it
// exists to close the redundancy window degraded writes open, and a cluster
// running neither is in the state it has always been in.
type RepairConfig struct {
	Enabled bool

	// Workers bounds concurrent shard rebuilds, PageSize how many placement
	// records a scan asks for at a time, and Interval the gap between passes.
	// Zero takes the repair package's own default for each.
	Workers  int
	PageSize int
	Interval time.Duration
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

	// LocalBlobNodeIDs are the blob nodes running in this process. They are the
	// ones this gate repairs for: every blob node is repaired by exactly one
	// coordinator, the gate that shares its disk, which settles ownership
	// without an election.
	LocalBlobNodeIDs []config.NodeID

	// Repair sweeps for shards a local blob node owns but does not hold at the
	// generation its record names.
	Repair RepairConfig

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
	Meta MetaClient
	Blob BlobClient

	// CredProv stands in for the credential chain New resolves from Auth and
	// IAM. Test seam: production leaves it nil.
	CredProv auth.CredentialProvider
}

// handlerConfig is the slice of the config the handlers read.
func (s3 *Config) handlerConfig() handlers.Config {
	return handlers.Config{
		Region:         s3.Region,
		DataShards:     s3.RS.Data,
		ParityShards:   s3.RS.Parity,
		DegradedWrites: s3.RS.DegradedWrites,
		Buckets:        s3.Buckets,
	}
}
