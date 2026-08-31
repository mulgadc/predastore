// The torn-overwrite scenario stops one host and concludes from what survives,
// so a probe that names the wrong host would not fail — it would quietly prove
// nothing. These tests hold it to the ring the gate builds.

package main

import (
	"math/rand/v2"
	"testing"

	"github.com/mulgadc/predastore"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadProfile(t *testing.T) *predastore.Config {
	t.Helper()
	cfg, err := predastore.LoadConfig("../../../config/4host.toml")
	require.NoError(t, err)
	return cfg
}

// The gate takes the ring's members from the config's blob nodes in ascending
// id order. Members in any other order is a different ring, and every shard
// would resolve to a host that does not hold it.
func TestBlobRingIsEveryBlobNodeInAscendingOrder(t *testing.T) {
	cfg := loadProfile(t)
	ids, hostOf := blobRing(cfg)

	var want []config.NodeID
	for _, h := range cfg.Hosts {
		for _, n := range h.Nodes {
			if n.Role == config.RoleBlob {
				want = append(want, n.ID)
				assert.Equal(t, h.ID, hostOf[n.ID], "node %d is on host %d", n.ID, h.ID)
			}
		}
	}
	require.NotEmpty(t, want)
	assert.ElementsMatch(t, want, ids)
	assert.IsIncreasing(t, ids, "ring members must be ascending to match the gate's")
	assert.Len(t, hostOf, len(ids))
}

// Placement follows the object hash, so the answer must not depend on the
// order the file happens to declare its hosts in. If it did, the probe would
// disagree with the gate on any config whose hosts are not already sorted.
func TestResolveShardsIgnoresHostDeclarationOrder(t *testing.T) {
	cfg := loadProfile(t)
	want, err := resolveShards(cfg, "bucket", "state.json")
	require.NoError(t, err)

	shuffled := loadProfile(t)
	rand.New(rand.NewPCG(1, 2)).Shuffle(len(shuffled.Hosts), func(i, j int) {
		shuffled.Hosts[i], shuffled.Hosts[j] = shuffled.Hosts[j], shuffled.Hosts[i]
	})
	got, err := resolveShards(shuffled, "bucket", "state.json")
	require.NoError(t, err)

	assert.Equal(t, want, got)
}

// The property the scenario rests on: the nodes named here are the nodes the
// gate would write to, in the order it writes them, split into data, parity and
// the handoff holder at the configured boundaries.
func TestResolveShardsMatchesTheGatesRing(t *testing.T) {
	cfg := loadProfile(t)
	ids, hostOf := blobRing(cfg)
	total := cfg.RS.Data + cfg.RS.Parity

	for _, key := range []string{"state.json", "victim.bin", "a", "deeply/nested/key"} {
		t.Run(key, func(t *testing.T) {
			nodes, err := placement.NewRing(ids).Nodes(
				model.ObjectHash("bucket", key), min(total+1, len(ids)))
			require.NoError(t, err)

			shards, err := resolveShards(cfg, "bucket", key)
			require.NoError(t, err)
			require.Len(t, shards, len(nodes))

			for i, s := range shards {
				assert.Equal(t, i, s.Index)
				assert.Equal(t, nodes[i], s.Node)
				assert.Equal(t, hostOf[nodes[i]], s.Host)
				switch {
				case i >= total:
					assert.Equal(t, "handoff", s.Role)
				case i >= cfg.RS.Data:
					assert.Equal(t, "parity", s.Role)
				default:
					assert.Equal(t, "data", s.Role)
				}
			}
		})
	}
}

// The handoff holder is a node the stripe does not use, which is what makes it
// somewhere to put a shard rather than a second copy on a node already holding
// one of this object's.
func TestHandoffHolderIsOutsideTheStripe(t *testing.T) {
	cfg := loadProfile(t)
	ids, _ := blobRing(cfg)
	total := cfg.RS.Data + cfg.RS.Parity
	require.Greater(t, len(ids), total, "this profile has no node to spare")

	shards, err := resolveShards(cfg, "bucket", "state.json")
	require.NoError(t, err)
	require.Len(t, shards, total+1)

	holder := shards[total]
	assert.Equal(t, "handoff", holder.Role)
	for _, s := range shards[:total] {
		assert.NotEqual(t, holder.Node, s.Node, "the holder already carries shard %d", s.Index)
	}
}

// The harness parses this with awk, so the field names and their order are the
// interface rather than a detail of the printing.
func TestShardPlacementPrintsParseableFields(t *testing.T) {
	s := shardPlacement{Index: 2, Role: "parity", Node: 9, Host: 3}
	assert.Equal(t, "shard=2 role=parity node=9 host=3", s.String())
}

func TestResolveShardsRejectsAConfigWithNoBlobNode(t *testing.T) {
	cfg := loadProfile(t)
	for i := range cfg.Hosts {
		cfg.Hosts[i].Nodes = nil
	}
	_, err := resolveShards(cfg, "bucket", "state.json")
	assert.Error(t, err)
}
