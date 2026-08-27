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

// Ring tuning. GetClosestN anchors on the partition's owner and then takes the
// next count-1 members in one globally fixed order, so partitionCount decides
// how evenly the anchors land and nothing else does. At 5 partitions over 4
// members one node held a shard of every object; 271 puts every node within a
// couple of percent of its fair share.
//
// load bounds how many partitions a member may own. Slack in that bound buys
// stability across membership changes, which this cluster does not have, and
// costs balance, which it does: 1.0 is the tightest bound the library accepts
// and measures best at every cluster size. Never set it below 1.0 — the
// distributor panics when no member has room.
const (
	partitionCount            = 271
	replicationFactor         = 100
	load              float64 = 1.0
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
