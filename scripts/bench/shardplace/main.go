// Command shardplace prints where one object's shards land, so a harness can
// aim a fault at a named shard instead of at a host it hopes is involved.
// Placement is derived from the object's name, not its contents, so it is
// knowable before the object is written and stays the same across overwrites.
//
//	go run ./scripts/bench/shardplace -config c.toml -bucket b -key k
//
// One line per shard, in shard-index order:
//
//	shard=0 role=data node=2 host=1
//
// The host is what a signal is delivered to: a host is one s3d process and its
// blob node runs inside it, so freezing the host freezes that shard alone.
package main

import (
	"cmp"
	"flag"
	"fmt"
	"os"
	"slices"

	"github.com/mulgadc/predastore"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
)

func main() {
	configPath := flag.String("config", "", "cluster config file")
	bucket := flag.String("bucket", "", "bucket holding the object")
	key := flag.String("key", "", "object key")
	flag.Parse()

	if *configPath == "" || *bucket == "" || *key == "" {
		fmt.Fprintln(os.Stderr, "usage: shardplace -config <file> -bucket <name> -key <name>")
		os.Exit(2)
	}

	cfg, err := predastore.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(2)
	}

	// Sorted by node id to match the ring the gate builds, which takes its
	// members from the config in that order. A different order here would
	// name the wrong host for every shard.
	type blobNode struct {
		node config.NodeID
		host config.HostID
	}
	var blobs []blobNode
	for _, h := range cfg.Hosts {
		for _, n := range h.Nodes {
			if n.Role == config.RoleBlob {
				blobs = append(blobs, blobNode{node: n.ID, host: h.ID})
			}
		}
	}
	slices.SortFunc(blobs, func(a, b blobNode) int { return cmp.Compare(a.node, b.node) })
	if len(blobs) == 0 {
		fmt.Fprintln(os.Stderr, "config has no blob node")
		os.Exit(2)
	}

	ids := make([]config.NodeID, 0, len(blobs))
	hostOf := make(map[config.NodeID]config.HostID, len(blobs))
	for _, b := range blobs {
		ids = append(ids, b.node)
		hostOf[b.node] = b.host
	}

	total := cfg.RS.Data + cfg.RS.Parity
	nodes, err := placement.NewRing(ids).Nodes(model.ObjectHash(*bucket, *key), total)
	if err != nil {
		fmt.Fprintf(os.Stderr, "place %s/%s: %v\n", *bucket, *key, err)
		os.Exit(1)
	}

	for i, node := range nodes {
		role := "data"
		if i >= cfg.RS.Data {
			role = "parity"
		}
		fmt.Printf("shard=%d role=%s node=%d host=%d\n", i, role, node, hostOf[node])
	}
}
