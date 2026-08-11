// Package placement decides which blob nodes hold an object's shards. The
// ring is a concrete consistent-hash implementation, not a pluggable strategy:
// every gate in a cluster must derive the same placement from the same
// object hash, so there is nothing to swap at runtime.
package placement

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
	"github.com/mulgadc/predastore/internal/config"
)

// Ring tuning. The load factor bounds how unevenly partitions may be spread
// across members.
const (
	partitionCount            = 5
	replicationFactor         = 100
	load              float64 = 1.25
)

// memberPrefix fronts every ring member name. Member names carry the node id
// so placement resolves straight to the id the storage client addresses.
const memberPrefix = "node-"

// hasher implements consistent.Hasher using xxhash.
type hasher struct{}

func (hasher) Sum64(data []byte) uint64 { return xxhash.Sum64(data) }

// member implements consistent.Member.
type member string

func (m member) String() string { return string(m) }

// Ring maps object hashes onto the cluster's blob nodes.
type Ring struct {
	ring *consistent.Consistent
}

// NewRing builds the placement ring over the given blob node ids.
func NewRing(nodeIDs []config.NodeID) *Ring {
	ring := consistent.New(nil, consistent.Config{
		PartitionCount:    partitionCount,
		ReplicationFactor: replicationFactor,
		Load:              load,
		Hasher:            hasher{},
	})
	for _, id := range nodeIDs {
		ring.Add(member(memberPrefix + strconv.FormatUint(uint64(id), 10)))
	}
	return &Ring{ring: ring}
}

// Nodes returns the ids of the count nodes an object's shards belong on, in
// shard order: the caller writes shard i to Nodes()[i].
func (r *Ring) Nodes(objectHash [32]byte, count int) ([]config.NodeID, error) {
	members, err := r.ring.GetClosestN(objectHash[:], count)
	if err != nil {
		return nil, err
	}
	ids := make([]config.NodeID, len(members))
	for i, m := range members {
		ids[i], err = nodeID(m.String())
		if err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// nodeID recovers the node id from a ring member name. Zero is rejected
// because the topology validates ids as positive, so a member named node-0
// means the ring was built from something that is not a cluster.
func nodeID(name string) (config.NodeID, error) {
	v, err := strconv.ParseUint(strings.Replace(name, memberPrefix, "", 1), 10, 64)
	if err != nil {
		return 0, err
	}
	if v == 0 {
		return 0, fmt.Errorf("node id %d is not positive", v)
	}
	return config.NodeID(v), nil
}
