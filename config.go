package predastore

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"

	"github.com/mulgadc/predastore/internal/gateway"
	"github.com/mulgadc/predastore/internal/gateway/auth"
	"github.com/mulgadc/predastore/internal/gateway/handlers"
	"github.com/mulgadc/predastore/internal/gateway/model"
	"github.com/mulgadc/predastore/internal/topology"
	"github.com/mulgadc/predastore/pkg/ratelimit"
	"github.com/pelletier/go-toml/v2"
)

// The TOML file is this package's contract with operators, so every type it
// names is declared or re-exported here. The topology tables are aliases
// rather than copies: the shape an operator writes under [[host]] and [[node]]
// is the shape placement is derived from, and a mirror would only be something
// to keep in sync. They are named for the file because Host is already this
// package's running process.

// NodeID and HostID identify the two levels of the cluster. They are distinct
// types so that a host id cannot be passed where a node id belongs.
type (
	NodeID = topology.NodeID
	HostID = topology.HostID
)

// Role is the function a node performs in the cluster, as written under
// [[node]].
type Role = topology.Role

const (
	// RoleShardStorage stores erasure-coded object shards.
	RoleShardStorage = topology.RoleShardStorage
	// RoleStateReplica participates in Raft consensus over global state.
	RoleStateReplica = topology.RoleStateReplica
)

// HostConfig is one predastore process, as written under [[host]]: the
// endpoint that owns a socket and a data directory. Nodes pinned to it run
// inside that process.
type HostConfig = topology.Host

// NodeConfig is a logical role pinned to a host, as written under [[node]].
type NodeConfig = topology.Node

// RS fixes the erasure code. The counts must match what the cluster was
// written with, so they are configuration rather than a per-request choice.
type RS struct {
	Data   int `toml:"data"`
	Parity int `toml:"parity"`
}

// Compaction tunes the shard store's background compactor. A zero interval
// falls back to the store's default; compaction itself is never off, because
// without it overwrite and delete churn never reclaims dead shards.
type Compaction struct {
	IntervalSeconds int `toml:"interval_seconds"`
}

// Bucket is a bucket declared in the config rather than created through the
// API.
type Bucket struct {
	Name   string `toml:"name"`
	Region string `toml:"region"`
	Type   string `toml:"type"`
	// Pathname is an on-disk directory backing the bucket; a relative path
	// resolves against BasePath.
	Pathname  string `toml:"pathname"`
	Public    bool   `toml:"public"`
	AccountID string `toml:"account_id"`
}

// AuthEntry is one config-defined service account, as it appears under
// [[auth]].
type AuthEntry struct {
	AccessKeyID     string       `toml:"access_key_id"`
	SecretAccessKey string       `toml:"secret_access_key"`
	AccountID       string       `toml:"account_id"`
	Policy          []PolicyRule `toml:"policy"`
}

// PolicyRule grants a config-defined account a set of actions on a bucket.
type PolicyRule struct {
	// Bucket is a bucket name or "*".
	Bucket string `toml:"bucket"`
	// Actions are S3 action names such as "s3:GetObject", or "s3:*".
	Actions []string `toml:"actions"`
}

// IAMConfig enables IAM authentication backed by NATS KV, layered over the
// config-defined accounts.
type IAMConfig struct {
	NATSUrl          string `toml:"nats_url"`
	NATSToken        string `toml:"nats_token"`
	MasterKeyPath    string `toml:"master_key_path"`
	AccessKeysBucket string `toml:"access_keys_bucket"`
}

// Config is predastore's on-disk configuration: the cluster topology, the
// erasure code, the config-defined buckets and the S3 credentials.
//
// The S3 listen address is not here. A `host` key would collide with the
// [[host]] topology table, which TOML rejects outright, so the gateway's
// address comes from Options instead.
type Config struct {
	Version string `toml:"version"`
	Region  string `toml:"region"`

	RS RS `toml:"rs"`

	// Hosts are processes owning a socket and a data directory; Nodes are
	// roles pinned to those hosts. Everything per-node derives from the host
	// base and the node id.
	Hosts []HostConfig `toml:"host"`
	Nodes []NodeConfig `toml:"node"`

	Compaction Compaction `toml:"compaction"`

	Buckets []Bucket `toml:"buckets"`

	// TODO: Move to IAM
	Auth                  []AuthEntry `toml:"auth"`
	AllowAnonymousListing bool        `toml:"allow_anonymous_listing"`
	AllowAnonymousAccess  bool        `toml:"allow_anonymous_access"`

	IAM *IAMConfig `toml:"iam"`

	Debug bool `toml:"debug"`
	// BasePath is the directory relative bucket and data paths resolve
	// against. Empty means the working directory.
	BasePath       string `toml:"base_path"`
	DisableLogging bool   `toml:"disable_logging"`

	RateLimit ratelimit.Config `toml:"ratelimit"`
}

// LoadConfig reads and validates a TOML configuration file. It touches no
// filesystem beyond the file itself: paths are resolved and directories
// created by New, so a caller may still override BasePath in between.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	cfg := &Config{}
	if err := toml.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	// Every config-defined service account must have an account_id so that
	// buckets it creates land with a real owner ID — otherwise the ownership
	// check would compare callerAccountID against "".
	for i, a := range cfg.Auth {
		if a.AccountID == "" {
			return nil, fmt.Errorf("auth entry %d (access_key_id=%q) missing account_id", i, a.AccessKeyID)
		}
	}

	// The topology, when present, must be internally consistent before
	// anything derives placement or addresses from it.
	if len(cfg.Hosts) > 0 || len(cfg.Nodes) > 0 {
		if err := topology.Validate(cfg.Hosts, cfg.Nodes); err != nil {
			return nil, err
		}
	}

	buckets := make([]Bucket, 0, len(cfg.Buckets))
	for _, b := range cfg.Buckets {
		// account_id is a hard requirement (the bucket-ownership check has
		// nothing to compare against without it) — validated before the name
		// so a malformed name cannot mask a missing owner.
		if b.AccountID == "" {
			return nil, fmt.Errorf("bucket %q missing account_id", b.Name)
		}
		// A bad name is dropped rather than fatal: the rest of the config is
		// still serviceable and the operator gets a warning.
		if err := model.IsValidBucketName(b.Name); err != nil {
			slog.Warn("Invalid bucket name", "bucket", b.Name, "error", err)
			continue
		}
		buckets = append(buckets, b)
	}
	cfg.Buckets = buckets

	return cfg, nil
}

// basePath resolves BasePath to an absolute directory, since relative bucket
// and data paths are resolved against it long after the working directory
// stops being meaningful.
func (c *Config) basePath() (string, error) {
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
// than on the resolver because they are questions about the configuration,
// which this package owns; the resolver only turns node ids into addresses.
//
// Each returns nodes sorted by id. Raft bootstraps from the state-replica set
// and treats an identically ordered set as idempotent across replicas, so the
// order is part of the contract rather than a tidiness.

// localHost is the [[host]] this process runs.
func (c *Config) localHost(hostID HostID) (HostConfig, bool) {
	for _, h := range c.Hosts {
		if h.ID == hostID {
			return h, true
		}
	}
	return HostConfig{}, false
}

// localNodes are the nodes pinned to hostID: the ones this process runs.
func (c *Config) localNodes(hostID HostID) []NodeConfig {
	return c.selectNodes(func(n NodeConfig) bool { return n.HostID == hostID })
}

// nodesByRole is every node with the role, wherever it runs.
func (c *Config) nodesByRole(role Role) []NodeConfig {
	return c.selectNodes(func(n NodeConfig) bool { return n.Role == role })
}

// dataDir is where a node keeps its state, derived from its own host's base
// directory and its node id. A relative path is resolved against BasePath by
// the caller, which is the only thing that knows it.
func (c *Config) dataDir(nodeID NodeID) string {
	for _, n := range c.Nodes {
		if n.ID != nodeID {
			continue
		}
		if h, ok := c.localHost(n.HostID); ok {
			return filepath.Join(h.DataDir, topology.NodeKey(nodeID))
		}
	}
	return ""
}

// storageNodeIDs are the shard-storage nodes the gateway places shards across.
func (c *Config) storageNodeIDs() []NodeID {
	nodes := c.nodesByRole(RoleShardStorage)
	ids := make([]NodeID, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.ID)
	}
	return ids
}

func (c *Config) selectNodes(keep func(NodeConfig) bool) []NodeConfig {
	var out []NodeConfig
	for _, n := range c.Nodes {
		if keep(n) {
			out = append(out, n)
		}
	}
	slices.SortFunc(out, func(a, b NodeConfig) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// bucketConfigs converts the config-defined buckets, resolving relative
// pathnames against basePath and creating the directories they name. A
// directory that cannot be created is a warning, not a failure: only buckets
// that are actually served from disk need one.
func (c *Config) bucketConfigs(basePath string) []handlers.BucketConfig {
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

func (c *Config) authEntries() []auth.Entry {
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

func (c *Config) iamConfig() *auth.IAMConfig {
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
func (c *Config) gatewayConfig(basePath string, debug bool) *gateway.Config {
	return &gateway.Config{
		Region:         c.Region,
		RS:             gateway.RS{Data: c.RS.Data, Parity: c.RS.Parity},
		Buckets:        c.bucketConfigs(basePath),
		Auth:           c.authEntries(),
		IAM:            c.iamConfig(),
		Debug:          c.Debug || debug,
		DisableLogging: c.DisableLogging,
		RateLimit:      c.RateLimit,
		StorageNodeIDs: c.storageNodeIDs(),
	}
}
