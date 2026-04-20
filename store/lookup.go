package store

import "fmt"

// Lookup fetches the Shard metadata for (objectHash, shardIndex).
func (store *Store) Lookup(objectHash [32]byte, shardIndex int) (*Shard, error) {
	data, err := store.index.Get(makeShardKey(objectHash, shardIndex))
	if err != nil {
		return nil, fmt.Errorf("store: Lookup: %w", err)
	}
	return decodeShard(data)
}
