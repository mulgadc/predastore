// Package config parses and validates predastore's on-disk TOML file and
// holds the vocabulary the rest of the tree is written in: node and host ids,
// roles, and the [[host]] and [[host.node]] tables themselves.
//
// It is a leaf. It answers what the file says and whether it is coherent, and
// nothing else — placement, addressing and the conversions into each
// subsystem's own settings all live above it.
package config

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/mulgadc/bluebottle/pkg/ratelimit"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/pelletier/go-toml/v2"
)

// Version is the file format this build reads. There is no migration between
// versions: a format change rewrites the file.
const Version = 1

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
	// BindAddr is where this host's cluster traffic listens — raft and blob
	// over QUIC — without a port; 0.0.0.0 binds all interfaces. It defaults to
	// Addr, which keeps the cluster plane off any interface peers do not use.
	// The S3 API is public and binds separately, per gate node.
	BindAddr string `toml:"bind_addr"`
	// Addr is the address other hosts dial, without a port, split from
	// BindAddr for NAT and multi-homed machines.
	Addr string `toml:"addr"`
	// DataDir is the on-disk root for the nodes that do not name one of their
	// own; each of those derives a subdirectory from its node id.
	DataDir string `toml:"data_dir"`
	// EncryptionKey is the path to the AES-256 key shards are encrypted with
	// at rest.
	EncryptionKey string `toml:"encryption_key"`
	// TLSCert and TLSKey identify the host to its peers and to S3 clients.
	TLSCert string `toml:"tls_cert"`
	TLSKey  string `toml:"tls_key"`
	// AdminPort serves /healthz and /readyz on BindAddr. It is a property of
	// the process rather than a role, so it sits here rather than becoming a
	// fourth node role. Zero runs no admin listener.
	AdminPort int `toml:"admin_port"`
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
	// BindAddr is where a gate serves the S3 API, without a port. S3 is a
	// public service and the cluster plane is not, so a multi-homed host puts
	// this on the public interface and leaves the host's bind_addr on the
	// private one. Empty follows the host. Only a gate may set it.
	BindAddr string `toml:"bind_addr"`
	// DataDir is an absolute directory this node owns outright, so that blob
	// nodes on one host can sit on separate disks. Empty derives one under the
	// host's root instead. It has no flag: it is per-node, and a flag is not.
	DataDir string `toml:"data_dir"`
}

// NodeDataDir is where a node keeps its state: the directory it names, or one
// of its own under the data root of the host it runs on.
func NodeDataDir(h Host, n Node) string {
	return cmp.Or(n.DataDir, filepath.Join(h.DataDir, fmt.Sprintf("node-%d", n.ID)))
}

// HostBindAddr is where this host's cluster traffic listens: the address it
// names, or the one its peers dial. The fallback is what keeps raft and blob
// traffic off the interfaces nothing in the cluster uses — an empty bind
// address is a wildcard to the network stack, so leaving it out must not mean
// publishing consensus to every interface.
func HostBindAddr(h Host) string {
	return cmp.Or(h.BindAddr, h.Addr)
}

// NodeBindAddr is where a gate serves the S3 API: the address it names, or the
// one its host binds the cluster plane to. A host that says nothing binds one
// address for both, which is what a single-homed machine wants.
func NodeBindAddr(h Host, n Node) string {
	return cmp.Or(n.BindAddr, HostBindAddr(h))
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

// S3 tunes the gate's HTTP surface, as opposed to the S3 semantics above it.
type S3 struct {
	// EnableHTTP2 advertises h2 ahead of http/1.1 over ALPN. Unset means on:
	// the gate is a general-purpose S3 endpoint and h2 suits a client whose
	// cost is round trips rather than bodies.
	//
	// It is a pointer because absent and false differ here, and only a client
	// that offers h2 is given it — so this is a fleet-wide backstop, not the
	// way to keep one client off h2. A client that multiplexes large bodies
	// onto one connection should decline h2 itself, the way viperblock's
	// backend does.
	EnableHTTP2 *bool `toml:"enable_http2"`
}

// HTTP2Enabled resolves EnableHTTP2 against its default.
func (s S3) HTTP2Enabled() bool {
	return s.EnableHTTP2 == nil || *s.EnableHTTP2
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
	Version int    `toml:"version"`
	Region  string `toml:"region"`

	RS RS `toml:"rs"`

	// Hosts are the machines of the cluster, each owning a data directory, a
	// TLS identity and the nodes pinned to it. Everything per-node derives
	// from the host base and the node id.
	Hosts []Host `toml:"host"`

	Compaction Compaction `toml:"compaction"`

	S3 S3 `toml:"s3"`

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

	// Strict, because an unknown key is either a typo or a setting this build
	// dropped, and both read as configured until something misbehaves.
	dec := toml.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	cfg := &Config{}
	if err := dec.Decode(cfg); err != nil {
		if unknown, ok := errors.AsType[*toml.StrictMissingError](err); ok {
			return nil, fmt.Errorf("parse %s: unknown key %q", path, strings.Join(unknown.Errors[0].Key(), "."))
		}
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// A bad bucket name is dropped rather than fatal: the rest of the config
	// is still serviceable and the operator gets a warning.
	buckets := make([]Bucket, 0, len(cfg.Buckets))
	for _, b := range cfg.Buckets {
		if err := (model.Bucket{Name: b.Name}).Validate(); err != nil {
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
	// First, because a file from another format explains every other complaint
	// this function could make about it.
	if c.Version != Version {
		return fmt.Errorf("config version %d, s3d reads version %d", c.Version, Version)
	}

	// The region is what a request's credential scope is compared against, so
	// an empty one matches nothing a client can sign.
	if c.Region == "" {
		return fmt.Errorf("missing region")
	}

	// The erasure code has no default: a substituted one would place objects
	// the file never asked for, at a width the blob-node check below never saw.
	if c.RS.Data <= 0 || c.RS.Parity < 0 {
		return fmt.Errorf("rs data must be positive and parity must not be negative")
	}
	// Zero parity is redundancy delegated to whatever sits under the blob node,
	// which only holds while the object is one shard on one node. Striping it
	// wider without parity makes losing any node lose every object.
	if c.RS.Parity == 0 && c.RS.Data != 1 {
		return fmt.Errorf("rs parity 0 requires rs data 1, got data %d", c.RS.Data)
	}

	if c.IAM != nil && isRelative(c.IAM.MasterKeyPath) {
		return fmt.Errorf("iam master_key_path %q must be absolute", c.IAM.MasterKeyPath)
	}

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
	blobs := 0

	for _, h := range c.Hosts {
		if h.ID == 0 {
			return fmt.Errorf("host id must be positive")
		}
		if hostIDs[h.ID] {
			return fmt.Errorf("duplicate host id %d", h.ID)
		}
		hostIDs[h.ID] = true
		if h.Addr == "" {
			return fmt.Errorf("host %d missing addr", h.ID)
		}
		// Ports belong to nodes, so a host address carrying one is naming
		// something the cluster no longer has a use for.
		if hasPort(h.BindAddr) {
			return fmt.Errorf("host %d bind_addr %q must not carry a port", h.ID, h.BindAddr)
		}
		if hasPort(h.Addr) {
			return fmt.Errorf("host %d addr %q must not carry a port", h.ID, h.Addr)
		}
		// Peers dial addr, so a wildcard or a group address names nothing they
		// can reach. bind_addr is the one that may be a wildcard.
		if ip := net.ParseIP(h.Addr); ip != nil && (ip.IsUnspecified() || ip.IsMulticast()) {
			return fmt.Errorf("host %d addr %q is not an address a peer can dial", h.ID, h.Addr)
		}
		for _, p := range [][2]string{
			{"data_dir", h.DataDir},
			{"encryption_key", h.EncryptionKey},
			{"tls_cert", h.TLSCert},
			{"tls_key", h.TLSKey},
		} {
			if isRelative(p[1]) {
				return fmt.Errorf("host %d %s %q must be absolute", h.ID, p[0], p[1])
			}
		}

		ports := make(map[int]NodeID, len(h.Nodes))
		// Derived directories are unique by node id, so a collision means an
		// explicit data_dir. One that names another node's derived directory is
		// only visible here when the file supplied the host root; when it comes
		// from --data-dir, this runs again after the flags are merged.
		dirs := make(map[string]NodeID, len(h.Nodes))
		var gate NodeID

		for _, n := range h.Nodes {
			if n.ID == 0 {
				return fmt.Errorf("node id must be positive")
			}
			if other, ok := nodeHosts[n.ID]; ok {
				return fmt.Errorf("duplicate node id %d on hosts %d and %d", n.ID, other, h.ID)
			}
			nodeHosts[n.ID] = h.ID
			switch n.Role {
			case RoleGate, RoleBlob, RoleMeta:
			default:
				return fmt.Errorf("node %d has unknown role %q", n.ID, n.Role)
			}
			if n.Port == 0 {
				return fmt.Errorf("node %d missing port", n.ID)
			}
			if other, ok := ports[n.Port]; ok {
				return fmt.Errorf("nodes %d and %d both use port %d on host %d", other, n.ID, n.Port, h.ID)
			}
			ports[n.Port] = n.ID
			// Only the S3 API is served on an address of its own; every other
			// node reaches its peers over the host's cluster plane, so a
			// bind_addr on one names a listener it does not have.
			if n.Role != RoleGate && n.BindAddr != "" {
				return fmt.Errorf("node %d has role %q and must not set bind_addr", n.ID, n.Role)
			}
			if hasPort(n.BindAddr) {
				return fmt.Errorf("node %d bind_addr %q must not carry a port", n.ID, n.BindAddr)
			}
			if n.Role == RoleGate {
				// A gate keeps nothing on disk, so a data_dir on one is a
				// misunderstanding rather than something to ignore.
				if n.DataDir != "" {
					return fmt.Errorf("gate node %d must not set data_dir", n.ID)
				}
				// Two gates on one host would be two S3 endpoints answering for
				// the same machine, which nothing downstream can choose between.
				if gate != 0 {
					return fmt.Errorf("host %d has more than one gate (nodes %d and %d)", h.ID, gate, n.ID)
				}
				gate = n.ID
				continue
			}
			if n.Role == RoleBlob {
				blobs++
			}
			if isRelative(n.DataDir) {
				return fmt.Errorf("node %d data_dir %q must be absolute", n.ID, n.DataDir)
			}
			dir := NodeDataDir(h, n)
			if other, ok := dirs[dir]; ok {
				return fmt.Errorf("nodes %d and %d both use data dir %s on host %d", other, n.ID, dir, h.ID)
			}
			dirs[dir] = n.ID
		}

		// The admin listener shares the host's cluster plane with the nodes, so
		// it is one more port on the same machine and collides the same way.
		if h.AdminPort != 0 {
			if h.AdminPort < 1 || h.AdminPort > 65535 {
				return fmt.Errorf("host %d admin_port %d is not a port", h.ID, h.AdminPort)
			}
			if other, ok := ports[h.AdminPort]; ok {
				return fmt.Errorf("host %d admin_port %d is already used by node %d", h.ID, h.AdminPort, other)
			}
		}
	}

	if len(nodeHosts) == 0 {
		return fmt.Errorf("no nodes defined")
	}
	// Placement spreads a stripe over distinct blob nodes, so a cluster with
	// fewer of them than the stripe is wide fails every write.
	if shards := c.RS.Data + c.RS.Parity; shards > blobs {
		return fmt.Errorf("rs data+parity is %d but the cluster has %d blob nodes", shards, blobs)
	}
	return nil
}

// hasPort reports whether an address carries a port. A bare IPv6 literal has
// no port and fails to split, which is the answer wanted here.
func hasPort(addr string) bool {
	_, _, err := net.SplitHostPort(addr)
	return err == nil
}

// isRelative reports whether a path is present and relative. Nothing anchors a
// relative one: s3d is started from wherever the operator happens to be. Empty
// means the file supplied none, which the flags may still.
func isRelative(path string) bool {
	return path != "" && !filepath.IsAbs(path)
}
