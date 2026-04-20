package store

// Delete removes the index entry for (objectHash, shardIndex).
// Does not reclaim disk space; that is compaction's job.
func (store *Store) Delete(objectHash [32]byte, shardIndex int) error {
	panic("store: Delete not implemented")
}
