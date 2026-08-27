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

// shardPlacement is one shard of one object: which node holds it, which host
// runs that node, and whether it carries data or parity.
type shardPlacement struct {
	Index int
	Role  string
	Node  config.NodeID
	Host  config.HostID
}

func (s shardPlacement) String() string {
	return fmt.Sprintf("shard=%d role=%s node=%d host=%d", s.Index, s.Role, s.Node, s.Host)
}

// blobRing is the placement ring the gate builds, rebuilt here from the same
// file. Node ids ascending is what makes it the same ring: the gate takes its
// members from the config in that order, and any other order would name the
// wrong host for every shard.
func blobRing(cfg *predastore.Config) ([]config.NodeID, map[config.NodeID]config.HostID) {
	ids := make([]config.NodeID, 0, len(cfg.Hosts))
	hostOf := make(map[config.NodeID]config.HostID, len(cfg.Hosts))
	for _, h := range cfg.Hosts {
		for _, n := range h.Nodes {
			if n.Role == config.RoleBlob {
				ids = append(ids, n.ID)
				hostOf[n.ID] = h.ID
			}
		}
	}
	slices.Sort(ids)
	return ids, hostOf
}

// resolveShards answers where bucket/key's shards live, in the order the gate
// writes them: data shards first, then parity.
func resolveShards(cfg *predastore.Config, bucket, key string) ([]shardPlacement, error) {
	ids, hostOf := blobRing(cfg)
	if len(ids) == 0 {
		return nil, fmt.Errorf("config has no blob node")
	}

	total := cfg.RS.Data + cfg.RS.Parity
	nodes, err := placement.NewRing(ids).Nodes(model.ObjectHash(bucket, key), total)
	if err != nil {
		return nil, fmt.Errorf("place %s/%s: %w", bucket, key, err)
	}

	out := make([]shardPlacement, 0, len(nodes))
	for i, node := range nodes {
		role := "data"
		if i >= cfg.RS.Data {
			role = "parity"
		}
		out = append(out, shardPlacement{Index: i, Role: role, Node: node, Host: hostOf[node]})
	}
	slices.SortFunc(out, func(a, b shardPlacement) int { return cmp.Compare(a.Index, b.Index) })
	return out, nil
}

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

	shards, err := resolveShards(cfg, *bucket, *key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	for _, s := range shards {
		fmt.Println(s)
	}
}
