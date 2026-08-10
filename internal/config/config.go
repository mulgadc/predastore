// Package config parses and validates predastore's on-disk TOML file and
// holds the vocabulary the rest of the tree is written in: node and host ids,
// roles, and the [[host]] and [[host.node]] tables themselves.
//
// It is a leaf. It answers what the file says and whether it is coherent, and
// nothing else — placement, addressing and the conversions into each
// subsystem's own settings all live above it.
package config

import (
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/pkg/ratelimit"
	"github.com/pelletier/go-toml/v2"
)

// NodeID and HostID identify the two levels of the cluster. They are distinct
// types because almost every function taking one takes the other too and they
// are otherwise indistinguishable: passing a host id where a node id belongs
// is a mistake the compiler should catch rather than a lookup that quietly
// misses.
//
// They are uint64 rather than int because both are serialized — to TOML here,
// and node ids to JSON in the delete tombstones the compactor reads. A fixed
// width keeps that encoding the same on every platform, and an unsigned one
// makes a negative id unrepresentable rather than a validation step everything
// downstream has to trust.
type (
	NodeID uint64
	HostID uint64
)

// Role is the function a node performs within the cluster, as written under
// [[host.node]].
type Role string

const (
	// RoleGate serves the S3 API in front of the cluster.
	RoleGate Role = "gate"
	// RoleBlob stores erasure-coded object shards.
	RoleBlob Role = "blob"
	// RoleMeta participates in Raft consensus over global state.
	RoleMeta Role = "meta"
)

// Host is one s3d process, as written under [[host]]: the machine that owns a
// data directory and a TLS identity. Nodes pinned to it run inside that
// process as goroutines, each on its own port.
//
// The file is meant to be identical on every machine, so the fields only the
// machine itself reads are optional here and may instead come from its s3d
// flags. Empty means the file did not supply one.
type Host struct {
	ID HostID `toml:"id"`
	// BindAddr is the local listen address, without a port; 0.0.0.0 binds all
	// interfaces. It defaults to Addr.
	BindAddr string `toml:"bind_addr"`
	// Addr is the address other hosts dial, without a port, split from
	// BindAddr for NAT and multi-homed machines.
	Addr string `toml:"addr"`
	// DataDir is the on-disk root; nodes derive their subdirectories from
	// node id.
	DataDir string `toml:"data_dir"`
	// EncryptionKey is the path to the AES-256 key shards are encrypted with
	// at rest.
	EncryptionKey string `toml:"encryption_key"`
	// TLSCert and TLSKey identify the host to its peers and to S3 clients.
	TLSCert string `toml:"tls_cert"`
	TLSKey  string `toml:"tls_key"`
	// Nodes are the roles this host runs, as written under [[host.node]].
	Nodes []Node `toml:"node"`
}

// Node is a logical role pinned to the host it is written under. Nodes sharing
// a host are colocated and talk over the in-process pipe; nodes on different
// hosts talk over the network.
type Node struct {
	ID   NodeID `toml:"id"`
	Role Role   `toml:"role"`
	// Port is the port this node answers on, unique within its host. For a
	// gate it is the S3 port, and its rpc sockets bind ephemerally.
	Port int `toml:"port"`
}

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
	Name      string `toml:"name"`
	Region    string `toml:"region"`
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
type Config struct {
	Version string `toml:"version"`
	Region  string `toml:"region"`

	RS RS `toml:"rs"`

	// Hosts are the machines of the cluster, each owning a data directory, a
	// TLS identity and the nodes pinned to it. Everything per-node derives
	// from the host base and the node id.
	Hosts []Host `toml:"host"`

	Compaction Compaction `toml:"compaction"`

	Buckets []Bucket `toml:"bucket"`

	// TODO: Move to IAM
	Auth []AuthEntry `toml:"auth"`

	IAM *IAMConfig `toml:"iam"`

	RateLimit ratelimit.Config `toml:"ratelimit"`
}

// Load reads and validates a TOML configuration file. It touches no
// filesystem beyond the file itself: the directories the config names are
// created by whoever uses them.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	cfg := &Config{}
	if err := toml.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// A bad bucket name is dropped rather than fatal: the rest of the config
	// is still serviceable and the operator gets a warning.
	buckets := make([]Bucket, 0, len(cfg.Buckets))
	for _, b := range cfg.Buckets {
		if err := model.IsValidBucketName(b.Name); err != nil {
			slog.Warn("Invalid bucket name", "bucket", b.Name, "error", err)
			continue
		}
		buckets = append(buckets, b)
	}
	cfg.Buckets = buckets

	return cfg, nil
}

// Validate reports whether the configuration is internally coherent. It is
// everything the file decides for the whole cluster; the host-local fields are
// checked by whoever resolves them against the flags, on the local host alone.
func (c *Config) Validate() error {
	// Every config-defined service account must have an account_id so that
	// buckets it creates land with a real owner ID — otherwise the ownership
	// check would compare callerAccountID against "".
	for i, a := range c.Auth {
		if a.AccountID == "" {
			return fmt.Errorf("auth entry %d (access_key_id=%q) missing account_id", i, a.AccessKeyID)
		}
	}

	// account_id is a hard requirement for a bucket too, and is checked before
	// the name so a malformed name cannot mask a missing owner.
	for _, b := range c.Buckets {
		if b.AccountID == "" {
			return fmt.Errorf("bucket %q missing account_id", b.Name)
		}
	}

	// The topology is optional; callers gate on its presence, so an empty one
	// is only invalid once something has been written.
	if len(c.Hosts) > 0 {
		return c.validateTopology()
	}
	return nil
}

// validateTopology checks the cluster as a whole: ids unique, roles known, and
// every node reachable at a port no sibling on its host has already claimed.
func (c *Config) validateTopology() error {
	hostIDs := make(map[HostID]bool, len(c.Hosts))
	// Node ids are unique across the file, not within a host: the rpc resolver
	// keys one flat table by id. The host each is on names both sides of a
	// collision, which the nesting otherwise hides.
	nodeHosts := make(map[NodeID]HostID)

	for _, h := range c.Hosts {
		if h.ID == 0 {
			return fmt.Errorf("config: host id must be positive")
		}
		if hostIDs[h.ID] {
			return fmt.Errorf("config: duplicate host id %d", h.ID)
		}
		hostIDs[h.ID] = true
		if h.Addr == "" {
			return fmt.Errorf("config: host %d missing addr", h.ID)
		}
		// Ports belong to nodes, so a host address carrying one is naming
		// something the cluster no longer has a use for.
		if hasPort(h.BindAddr) {
			return fmt.Errorf("config: host %d bind_addr %q must not carry a port", h.ID, h.BindAddr)
		}
		if hasPort(h.Addr) {
			return fmt.Errorf("config: host %d addr %q must not carry a port", h.ID, h.Addr)
		}

		ports := make(map[int]NodeID, len(h.Nodes))
		var gate NodeID

		for _, n := range h.Nodes {
			if n.ID == 0 {
				return fmt.Errorf("config: node id must be positive")
			}
			if other, ok := nodeHosts[n.ID]; ok {
				return fmt.Errorf("config: duplicate node id %d on hosts %d and %d", n.ID, other, h.ID)
			}
			nodeHosts[n.ID] = h.ID
			switch n.Role {
			case RoleGate, RoleBlob, RoleMeta:
			default:
				return fmt.Errorf("config: node %d has unknown role %q", n.ID, n.Role)
			}
			if n.Port == 0 {
				return fmt.Errorf("config: node %d missing port", n.ID)
			}
			if other, ok := ports[n.Port]; ok {
				return fmt.Errorf("config: nodes %d and %d both use port %d on host %d", other, n.ID, n.Port, h.ID)
			}
			ports[n.Port] = n.ID
			if n.Role == RoleGate {
				// Two gates on one host would be two S3 endpoints answering for
				// the same machine, which nothing downstream can choose between.
				if gate != 0 {
					return fmt.Errorf("config: host %d has more than one gate (nodes %d and %d)", h.ID, gate, n.ID)
				}
				gate = n.ID
			}
		}
	}

	if len(nodeHosts) == 0 {
		return fmt.Errorf("config: no nodes defined")
	}
	return nil
}

// hasPort reports whether an address carries a port. A bare IPv6 literal has
// no port and fails to split, which is the answer wanted here.
func hasPort(addr string) bool {
	_, _, err := net.SplitHostPort(addr)
	return err == nil
}
