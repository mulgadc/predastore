package predastore

import (
	"context"
	"crypto/x509"
	"fmt"

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
// The caller supplies rootCAs: the cluster's certificate authority, or any
// pool naming exactly the certificates it should accept. cfg's per-host
// tls_cert/tls_key name files on that host's own filesystem — a different
// file under the same path on every host — so a caller reaching a node on
// another machine cannot read them and must not guess at them. rootCAs is
// how it verifies every node's identity instead. No local TLS identity of
// the caller's own is required: nothing on this wire ever asks a dialer to
// present one back, so NodeStatus asks for none either.
func NodeStatus(ctx context.Context, cfg *Config, node NodeID, rootCAs *x509.CertPool) (Status, error) {
	if _, ok := hostOfNode(cfg, node); !ok {
		return Status{}, fmt.Errorf("node %d is not in the configuration", node)
	}

	// The transport binds its own outbound socket; it is not the target's
	// address, so there is no host-specific bind address to pick.
	quic, err := transport.NewQUICDialTransport("", 0, transport.WithRootCAs(rootCAs))
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
