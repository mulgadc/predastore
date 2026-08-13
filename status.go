package predastore

import (
	"context"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
)

// Status is one meta replica's raft state: its own node id, its raft.State,
// the leader it currently observes (empty when the cluster has none), and
// the raft term and log positions behind that view. It is a type alias
// because this is exactly the response OpMetaStatus answers with, and a
// consumer outside this module already decodes its JSON shape.
type Status = meta.MetaStatus

// MetaNodesOnHost returns the meta node ids pinned to host, in id order. A
// host running no meta node, or an id naming no host, returns nil: neither
// is an error, since a host is free to run no consensus replica of its own.
func MetaNodesOnHost(cfg *Config, host HostID) []NodeID {
	h, ok := hostOf(cfg, host)
	if !ok {
		return nil
	}
	var ids []NodeID
	for _, n := range h.Nodes {
		if n.Role == RoleMeta {
			ids = append(ids, n.ID)
		}
	}
	return ids
}

// NodeStatus dials the meta replica named by node directly, over the
// network, and returns its raft status. It queries that one replica only:
// unlike a data read it never falls back to another replica or follows a
// leader redirect, because the caller is asking what this specific process
// observes, which is exactly what a health probe needs.
//
// The caller need not be a member of the cluster or hold any identity of
// its own — a monitoring daemon reading the same configuration file
// predastore runs from is the intended shape. Building the quic transport a
// dial-only connection needs still requires loading a TLS keypair (the type
// serves listening too), so NodeStatus reuses the target's own host
// identity for it; that keypair is never presented on a dial-only
// connection; TLS trust is not the OS store but a pool holding exactly the
// certificate the configuration names for that host, which is the
// verification a caller with nothing but the config file can do without an
// externally supplied CA.
func NodeStatus(ctx context.Context, cfg *Config, node NodeID) (Status, error) {
	host, ok := hostOfNode(cfg, node)
	if !ok {
		return Status{}, fmt.Errorf("node %d is not in the configuration", node)
	}

	pool, err := trustedPool(host.TLSCert)
	if err != nil {
		return Status{}, fmt.Errorf("load host %d TLS identity: %w", host.ID, err)
	}

	quic, err := transport.NewQUICTransport(HostBindAddr(host), 0, host.TLSCert, host.TLSKey, transport.WithRootCAs(pool))
	if err != nil {
		return Status{}, fmt.Errorf("create status transport: %w", err)
	}
	defer quic.Close()

	res, err := rpc.NewRemoteResolver(cfg, quic)
	if err != nil {
		return Status{}, fmt.Errorf("build status resolver: %w", err)
	}

	connPool := rpc.NewConnPool(node, res)
	defer connPool.Close()

	cli, err := meta.NewClient(meta.ClientConfig{
		Client:   rpc.NewClient(connPool),
		Replicas: []NodeID{node},
	})
	if err != nil {
		return Status{}, fmt.Errorf("create status client: %w", err)
	}

	return cli.Status(ctx, node)
}

// trustedPool reads a PEM certificate file and returns a pool trusting
// exactly it. A status probe verifies the one identity its own
// configuration names for the target host, rather than any certificate a
// public CA happened to issue.
func trustedPool(certPath string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(certPath)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates found in %s", certPath)
	}
	return pool, nil
}

// hostOfNode returns the host a node id is pinned to.
func hostOfNode(c *Config, id NodeID) (HostConfig, bool) {
	for _, h := range c.Hosts {
		for _, n := range h.Nodes {
			if n.ID == id {
				return h, true
			}
		}
	}
	return HostConfig{}, false
}
