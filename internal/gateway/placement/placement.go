// Package placement decides which storage nodes hold an object's shards. The
// ring is a concrete consistent-hash implementation, not a pluggable strategy:
// every gateway in a cluster must derive the same placement from the same
// object hash, so there is nothing to swap at runtime.
package placement

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
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

// Ring maps object hashes onto the cluster's shard-storage nodes.
type Ring struct {
	ring *consistent.Consistent
}

// NewRing builds the placement ring over the given storage node ids.
func NewRing(nodeIDs []int) *Ring {
	ring := consistent.New(nil, consistent.Config{
		PartitionCount:    partitionCount,
		ReplicationFactor: replicationFactor,
		Load:              load,
		Hasher:            hasher{},
	})
	for _, id := range nodeIDs {
		ring.Add(member(memberPrefix + strconv.Itoa(id)))
	}
	return &Ring{ring: ring}
}

// Nodes returns the ids of the count nodes an object's shards belong on, in
// shard order: the caller writes shard i to Nodes()[i].
func (r *Ring) Nodes(objectHash [32]byte, count int) ([]uint32, error) {
	members, err := r.ring.GetClosestN(objectHash[:], count)
	if err != nil {
		return nil, err
	}
	ids := make([]uint32, len(members))
	for i, m := range members {
		ids[i], err = nodeID(m.String())
		if err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// nodeID recovers the node id from a ring member name. It errors on a numeric
// component that is negative or beyond uint32 rather than wrapping it into a
// valid-looking address.
func nodeID(name string) (uint32, error) {
	v, err := strconv.Atoi(strings.Replace(name, memberPrefix, "", 1))
	if err != nil {
		return 0, err
	}
	if v < 0 || v > math.MaxUint32 {
		return 0, fmt.Errorf("node id %d out of uint32 range", v)
	}
	return uint32(v), nil
}
