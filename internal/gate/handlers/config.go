// Package handlers implements the S3 REST operations. Each operation is a
// factory returning an http.Handler that closes over the cluster clients it
// needs — there is no server struct, and nothing here owns a listener or a
// route table.
package handlers

// Config is the gate's slice of the TOML config: the settings the handlers
// read, without the topology, transport or process settings that only the
// server assembly cares about.
type Config struct {
	// Region is the region regional operations must be signed against, and the
	// one a bucket is created in when the request names none.
	Region string

	// DataShards and ParityShards fix the erasure code. They must match what
	// the cluster was written with, so they come from config rather than a
	// per-request choice.
	DataShards   int
	ParityShards int

	// DegradedWrites accepts a write once DataShards of the stripe are durable
	// rather than requiring every shard, so one node down does not refuse
	// writes. It opens a redundancy window that only repair closes, so it is
	// off unless the operator turns it on.
	DegradedWrites bool

	// Buckets are the config-defined buckets: static, known at startup, and
	// never removed.
	Buckets []BucketConfig
}

// TotalShards is the number of nodes an object is spread across.
func (c Config) TotalShards() int { return c.DataShards + c.ParityShards }

// MinShards is how many of a stripe's shards have to be durable before the
// write is acknowledged. Any DataShards of the stripe reconstruct the object,
// so that is the floor; requiring one more cannot buy redundancy at RS(2,1)
// and would block three quarters of writes with a single node down.
func (c Config) MinShards() int {
	if c.DegradedWrites {
		return c.DataShards
	}

	return c.TotalShards()
}

// BucketConfig is a bucket declared in the configuration rather than created
// through the API.
type BucketConfig struct {
	Name      string
	Region    string
	Public    bool
	AccountID string
}

// Find returns the named config-defined bucket.
func (c Config) Find(bucket string) (BucketConfig, bool) {
	for _, b := range c.Buckets {
		if b.Name == bucket {
			return b, true
		}
	}
	return BucketConfig{}, false
}
