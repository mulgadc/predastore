package store

import "fmt"

// Delete removes the index entry for (objectHash, shardIndex).
// Does not reclaim disk space; that is compaction's job.
func (store *Store) Delete(objectHash [32]byte, shardIndex int) error {
	if err := store.index.Delete(makeShardKey(objectHash, shardIndex)); err != nil {
		return fmt.Errorf("store: Delete: %w", err)
	}
	return nil
}
