package predastore

import (
	"cmp"
	"slices"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
)

// The TOML file is this package's contract with operators, so every type it
// names is re-exported here. They are aliases rather than copies: the shape an
// operator writes is the shape the cluster is derived from, and a mirror would
// only be something to keep in sync. The host and node tables are named for
// the file because Host is already this package's running process.

// NodeID and HostID identify the two levels of the cluster. They are distinct
// types so that a host id cannot be passed where a node id belongs.
type (
	NodeID = config.NodeID
	HostID = config.HostID
)

// Role is the function a node performs in the cluster, as written under
// [[host.node]].
type Role = config.Role

const (
	// RoleGate serves the S3 API in front of the cluster.
	RoleGate = config.RoleGate
	// RoleBlob stores erasure-coded object shards.
	RoleBlob = config.RoleBlob
	// RoleMeta participates in Raft consensus over global state.
	RoleMeta = config.RoleMeta
)

// HostConfig is one machine, as written under [[host]]: a data directory, an
// address, a TLS identity and the nodes it runs in one process.
type HostConfig = config.Host

// NodeConfig is a logical role pinned to a host, as written under
// [[host.node]].
type NodeConfig = config.Node

// The remaining tables of the configuration file.
type (
	RS         = config.RS
	Compaction = config.Compaction
	Bucket     = config.Bucket
	AuthEntry  = config.AuthEntry
	PolicyRule = config.PolicyRule
	IAMConfig  = config.IAMConfig
)

// Config is predastore's on-disk configuration.
type Config = config.Config

// LoadConfig reads and validates a TOML configuration file.
func LoadConfig(path string) (*Config, error) {
	return config.Load(path)
}

// HostBindAddr is where a host's cluster traffic listens, and NodeBindAddr
// where a gate serves S3. Both are re-exported because an embedder settling
// host-local fields has to know which plane it is settling: the S3 address
// belongs to the gate, and putting it on the host would move raft and blob
// traffic onto the public interface with it.
func HostBindAddr(h HostConfig) string { return config.HostBindAddr(h) }

// NodeBindAddr resolves a gate's S3 listen address against its host.
func NodeBindAddr(h HostConfig, n NodeConfig) string { return config.NodeBindAddr(h, n) }

// The queries below answer what the cluster is made of. They live here rather
// than in the config package because they are inventory questions about a
// parsed file, not part of parsing it.
//
// nodesByRole sorts by id. Raft bootstraps from the meta set and treats an
// identically ordered set as idempotent across replicas, so the order is part
// of the contract rather than a tidiness.

// hostOf is the [[host]] with this id, and whether the file names one.
func hostOf(c *Config, hostID HostID) (HostConfig, bool) {
	for _, h := range c.Hosts {
		if h.ID == hostID {
			return h, true
		}
	}
	return HostConfig{}, false
}

// nodesByRole is every node with the role, wherever it runs.
func nodesByRole(c *Config, role Role) []NodeConfig {
	var out []NodeConfig
	for _, h := range c.Hosts {
		for _, n := range h.Nodes {
			if n.Role == role {
				out = append(out, n)
			}
		}
	}
	slices.SortFunc(out, func(a, b NodeConfig) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// hasRemoteNodes reports whether any node runs off hostID, which is the only
// reason a node on it opens a network socket.
func hasRemoteNodes(c *Config, hostID HostID) bool {
	return slices.ContainsFunc(c.Hosts, func(h HostConfig) bool {
		return h.ID != hostID && len(h.Nodes) > 0
	})
}

// nodeIDs names a set of nodes by id, which is how every client addresses one.
func nodeIDs(nodes []NodeConfig) []NodeID {
	ids := make([]NodeID, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.ID)
	}
	return ids
}

// The conversions below turn the file into each subsystem's own settings. They
// live here rather than in the config package because gate.Config names a
// NodeID and so imports config; converting there would close the cycle.

func bucketConfigs(c *Config) []handlers.BucketConfig {
	buckets := make([]handlers.BucketConfig, len(c.Buckets))
	for i, b := range c.Buckets {
		buckets[i] = handlers.BucketConfig{
			Name:      b.Name,
			Region:    b.Region,
			Public:    b.Public,
			AccountID: b.AccountID,
		}
	}
	return buckets
}

func authEntries(c *Config) []auth.Entry {
	entries := make([]auth.Entry, len(c.Auth))
	for i, a := range c.Auth {
		rules := make([]auth.PolicyRule, len(a.Policy))
		for j, p := range a.Policy {
			rules[j] = auth.PolicyRule{Bucket: p.Bucket, Actions: p.Actions}
		}
		entries[i] = auth.Entry{
			AccessKeyID:     a.AccessKeyID,
			SecretAccessKey: a.SecretAccessKey,
			AccountID:       a.AccountID,
			Policy:          rules,
		}
	}
	return entries
}

func iamConfig(c *Config) *auth.IAMConfig {
	if c.IAM == nil {
		return nil
	}
	return &auth.IAMConfig{
		NATSUrl:          c.IAM.NATSUrl,
		NATSToken:        c.IAM.NATSToken,
		MasterKeyPath:    c.IAM.MasterKeyPath,
		AccessKeysBucket: c.IAM.AccessKeysBucket,
	}
}

// gateConfig is everything the S3 frontend runs on: the slice of the file it
// reads, plus the wiring its host supplies — where it listens, the TLS
// identity it serves under, and the cluster clients it works through.
func gateConfig(
	c *Config, host HostConfig, n NodeConfig, mc handlers.MetaClient, bc handlers.BlobClient,
) gate.Config {
	return gate.Config{
		Region:      c.Region,
		RS:          gate.RS{Data: c.RS.Data, Parity: c.RS.Parity},
		Buckets:     bucketConfigs(c),
		Auth:        authEntries(c),
		IAM:         iamConfig(c),
		RateLimit:   c.RateLimit,
		BlobNodeIDs: nodeIDs(nodesByRole(c, RoleBlob)),

		Addr:    config.NodeBindAddr(host, n),
		Port:    n.Port,
		TLSCert: host.TLSCert,
		TLSKey:  host.TLSKey,
		Meta:    mc,
		Blob:    bc,
	}
}
