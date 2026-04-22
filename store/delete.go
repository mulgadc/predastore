package store

import "fmt"

// Delete removes the index entry for the given key. It does not
// reclaim segment disk space; that is compaction's job.
func (store *Store) Delete(key string) error {
	if err := store.index.Delete([]byte(key)); err != nil {
		return fmt.Errorf("store: Delete: %w", err)
	}
	return nil
}
