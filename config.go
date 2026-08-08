package predastore

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gateway"
	"github.com/mulgadc/predastore/internal/gateway/auth"
	"github.com/mulgadc/predastore/internal/gateway/handlers"
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
// [[node]].
type Role = config.Role

const (
	// RoleGateway serves the S3 API in front of the cluster.
	RoleGateway = config.RoleGateway
	// RoleShardStorage stores erasure-coded object shards.
	RoleShardStorage = config.RoleShardStorage
	// RoleStateReplica participates in Raft consensus over global state.
	RoleStateReplica = config.RoleStateReplica
)

// HostConfig is one machine, as written under [[host]]: a data directory, an
// address and a TLS identity. Nodes pinned to it run in one process.
type HostConfig = config.Host

// NodeConfig is a logical role pinned to a host, as written under [[node]].
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

// basePath resolves BasePath to an absolute directory, since relative bucket
// and data paths are resolved against it long after the working directory
// stops being meaningful.
func basePath(c *Config) (string, error) {
	if filepath.IsAbs(c.BasePath) {
		return c.BasePath, nil
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve base path: %w", err)
	}
	return filepath.Join(dir, c.BasePath), nil
}

// The queries below answer what the cluster is made of. They live here rather
// than in the config package because they are inventory questions about a
// parsed file, not part of parsing it.
//
// Each returns nodes sorted by id. Raft bootstraps from the state-replica set
// and treats an identically ordered set as idempotent across replicas, so the
// order is part of the contract rather than a tidiness.

// hostOf is the [[host]] a node runs on.
func hostOf(c *Config, hostID HostID) (HostConfig, bool) {
	for _, h := range c.Hosts {
		if h.ID == hostID {
			return h, true
		}
	}
	return HostConfig{}, false
}

// localNodes are the nodes pinned to hostID: the ones this process runs.
func localNodes(c *Config, hostID HostID) []NodeConfig {
	return selectNodes(c, func(n NodeConfig) bool { return n.HostID == hostID })
}

// nodesByRole is every node with the role, wherever it runs.
func nodesByRole(c *Config, role Role) []NodeConfig {
	return selectNodes(c, func(n NodeConfig) bool { return n.Role == role })
}

// hasRemoteNodes reports whether any node runs off hostID, which is the only
// reason a node on it opens a network socket.
func hasRemoteNodes(c *Config, hostID HostID) bool {
	return slices.ContainsFunc(c.Nodes, func(n NodeConfig) bool { return n.HostID != hostID })
}

// dataDir is where a node keeps its state, derived from its own host's base
// directory and its node id. A relative path is resolved against the base
// path, so a config can be shared across machines and the launcher decides
// where state lands.
func dataDir(c *Config, nodeID NodeID, base string) string {
	for _, n := range c.Nodes {
		if n.ID != nodeID {
			continue
		}
		if h, ok := hostOf(c, n.HostID); ok {
			dir := filepath.Join(h.DataDir, nodeKey(nodeID))
			if !filepath.IsAbs(dir) {
				dir = filepath.Join(base, dir)
			}
			return dir
		}
	}
	return ""
}

// nodeKey names a node's directory under its host's data root.
func nodeKey(nodeID NodeID) string {
	return fmt.Sprintf("node-%d", nodeID)
}

// nodeIDs names a set of nodes by id, which is how every client addresses one.
func nodeIDs(nodes []NodeConfig) []NodeID {
	ids := make([]NodeID, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.ID)
	}
	return ids
}

func selectNodes(c *Config, keep func(NodeConfig) bool) []NodeConfig {
	var out []NodeConfig
	for _, n := range c.Nodes {
		if keep(n) {
			out = append(out, n)
		}
	}
	slices.SortFunc(out, func(a, b NodeConfig) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// The conversions below turn the file into each subsystem's own settings. They
// live here rather than in the config package because gateway.Config names a
// NodeID and so imports config; converting there would close the cycle.

// bucketConfigs converts the config-defined buckets, resolving relative
// pathnames against basePath and creating the directories they name. A
// directory that cannot be created is a warning, not a failure: only buckets
// that are actually served from disk need one.
func bucketConfigs(c *Config, basePath string) []handlers.BucketConfig {
	buckets := make([]handlers.BucketConfig, 0, len(c.Buckets))
	for _, b := range c.Buckets {
		out := handlers.BucketConfig{
			Name:      b.Name,
			Region:    b.Region,
			Type:      b.Type,
			Pathname:  b.Pathname,
			Public:    b.Public,
			AccountID: b.AccountID,
		}
		if out.Pathname != "" {
			if !filepath.IsAbs(out.Pathname) {
				out.Pathname = filepath.Join(basePath, out.Pathname)
			}
			if _, err := os.Stat(out.Pathname); errors.Is(err, os.ErrNotExist) {
				if mkErr := os.MkdirAll(out.Pathname, 0750); mkErr != nil {
					slog.Warn("Failed to create bucket directory", "path", out.Pathname, "error", mkErr)
				}
			}
		}
		buckets = append(buckets, out)
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

// gatewayConfig is the slice of the file the S3 frontend reads, with the
// process-level debug override already folded in.
func gatewayConfig(c *Config, basePath string, debug bool) *gateway.Config {
	return &gateway.Config{
		Region:         c.Region,
		RS:             gateway.RS{Data: c.RS.Data, Parity: c.RS.Parity},
		Buckets:        bucketConfigs(c, basePath),
		Auth:           authEntries(c),
		IAM:            iamConfig(c),
		Debug:          c.Debug || debug,
		DisableLogging: c.DisableLogging,
		RateLimit:      c.RateLimit,
		StorageNodeIDs: nodeIDs(nodesByRole(c, RoleShardStorage)),
	}
}
