// Command tombstones lists the delete tombstones a running cluster still
// holds, for a shell harness that has to see the reaper reclaim one rather
// than infer it from disk use. It reads both prefixes the reaper sweeps.
//
//	go run ./scripts/bench/tombstones -config c.toml -ca server.pem
//
// One line per row, then a count:
//
//	prefix=deleted: bucket=b key=k epoch=7
//	rows=1
//
// The exit status is 1 when the scan could not be answered, so "no rows" and
// "could not ask" are never the same output.
package main

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mulgadc/predastore"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
)

// scanner is the slice of the meta client a listing needs.
type scanner interface {
	ScanFrom(ctx context.Context, prefix, after string, limit int) ([]meta.Item, error)
}

var _ scanner = (*meta.Client)(nil)

// tombstone is one decoded row, under the prefix it was found at.
type tombstone struct {
	Prefix string
	Info   handlers.DeletedObjectInfo
}

func (t tombstone) String() string {
	return fmt.Sprintf("prefix=%s bucket=%s key=%s epoch=%d",
		t.Prefix, t.Info.Bucket, t.Info.Key, t.Info.WriteEpoch)
}

// listTombstones pages through every tombstone prefix the reaper reads. A row
// that does not decode is reported by its key, because a row the reaper skips
// is still a row the cluster holds.
func listTombstones(ctx context.Context, s scanner, pageSize int, undecodable io.Writer) ([]tombstone, error) {
	var out []tombstone
	for _, rowPrefix := range []string{handlers.DeletedObjectPrefix, handlers.DeletedEpochPrefix} {
		prefix := handlers.TableKey(model.TableObjects, rowPrefix)
		cursor := ""
		for {
			items, err := s.ScanFrom(ctx, prefix, cursor, pageSize)
			if err != nil {
				return nil, fmt.Errorf("scan %s: %w", rowPrefix, err)
			}
			for _, item := range items {
				cursor = item.Key
				var info handlers.DeletedObjectInfo
				if err := gob.NewDecoder(bytes.NewReader(item.Value)).Decode(&info); err != nil {
					_, _ = fmt.Fprintf(undecodable, "undecodable key=%q err=%q\n", item.Key, err)
					continue
				}
				out = append(out, tombstone{Prefix: rowPrefix, Info: info})
			}
			if len(items) < pageSize {
				break
			}
		}
	}

	return out, nil
}

// metaReplicas is every meta node the profile declares.
func metaReplicas(cfg *predastore.Config) []config.NodeID {
	var ids []config.NodeID
	for _, h := range cfg.Hosts {
		for _, n := range h.Nodes {
			if n.Role == config.RoleMeta {
				ids = append(ids, n.ID)
			}
		}
	}

	return ids
}

func run(ctx context.Context, cfg *predastore.Config, roots *x509.CertPool) error {
	replicas := metaReplicas(cfg)
	if len(replicas) == 0 {
		return fmt.Errorf("config has no meta node")
	}

	quic, err := transport.NewQUICDialTransport("", 0, transport.WithRootCAs(roots))
	if err != nil {
		return fmt.Errorf("create transport: %w", err)
	}
	defer quic.Close()

	res, err := rpc.NewRemoteResolver(cfg, quic)
	if err != nil {
		return fmt.Errorf("build resolver: %w", err)
	}
	pool := rpc.NewConnPool(replicas[0], res)
	defer pool.Close()

	cli, err := meta.NewClient(meta.ClientConfig{Client: rpc.NewClient(pool), Replicas: replicas})
	if err != nil {
		return fmt.Errorf("create meta client: %w", err)
	}

	rows, err := listTombstones(ctx, cli, 256, os.Stderr)
	if err != nil {
		return err
	}
	for _, r := range rows {
		fmt.Println(r)
	}
	fmt.Printf("rows=%d\n", len(rows))

	return nil
}

func main() {
	configPath := flag.String("config", "", "cluster config file")
	caPath := flag.String("ca", "", "PEM holding the cluster's trust anchor")
	timeout := flag.Duration("timeout", 20*time.Second, "deadline for the whole listing")
	flag.Parse()

	if *configPath == "" || *caPath == "" {
		fmt.Fprintln(os.Stderr, "usage: tombstones -config <file> -ca <file>")
		os.Exit(2)
	}

	cfg, err := predastore.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(2)
	}

	pem, err := os.ReadFile(*caPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read ca: %v\n", err)
		os.Exit(2)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		fmt.Fprintf(os.Stderr, "no certificate in %s\n", *caPath)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	err = run(ctx, cfg, roots)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
