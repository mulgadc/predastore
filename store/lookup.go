package store

// Lookup fetches the Shard metadata for (objectHash, shardIndex).
func (store *Store) Lookup(objectHash [32]byte, shardIndex int) (*Shard, error) {
	panic("store: Lookup not implemented")
}
