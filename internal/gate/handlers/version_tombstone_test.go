package handlers

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedVersion stores one version of a key the way a write to a versioned
// bucket does: its own shard set under its own version hash.
func seedVersion(t *testing.T, f writeFixture, bucket, key, versionID string, body []byte) [32]byte {
	t.Helper()
	ctx := context.Background()
	versionHash := model.VersionHash(bucket, key, versionID)

	place, _, err := f.write(ctx, versionHash, bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)

	record, err := EncodePlacement(place)
	require.NoError(t, err)
	require.NoError(t, metaPut(ctx, f.mc, model.TableObjects, string(versionHash[:]), record))
	return versionHash
}

// tombstones returns every reclaim record in state under both prefixes the
// reaper scans, keyed by stored key.
func tombstones(t *testing.T, mc MetaClient) map[string]DeletedObjectInfo {
	t.Helper()
	found := make(map[string]DeletedObjectInfo)
	for _, prefix := range []string{DeletedEpochPrefix, DeletedObjectPrefix} {
		items, err := metaScan(context.Background(), mc, model.TableObjects, prefix, 0)
		require.NoError(t, err)
		for _, item := range items {
			var info DeletedObjectInfo
			require.NoError(t, gob.NewDecoder(bytes.NewReader(item.Value)).Decode(&info))
			found[item.Key] = info
		}
	}
	return found
}

// Deleting every version of a key in succession is what emptying a versioned
// bucket does, and it is the access pattern that collides: one reclaim record
// per key would leave the first version's shards unaccounted for while the
// bucket is allowed to disappear.
func TestTwoVersionsOfOneKeyKeepSeparateReclaimRecords(t *testing.T) {
	t.Parallel()

	const key = "a.txt"
	f := newWriteFixture(4, 2)
	first := seedVersion(t, f, deleteTestBucket, key, "V1", []byte("first"))
	second := seedVersion(t, f, deleteTestBucket, key, "V2", []byte("second"))
	require.NotEqual(t, first, second, "two versions must not share a shard set")

	// Every shard delete fails, so each version's delete leaves a record of
	// where its shards still are.
	failing := &failingBlob{fakeBlob: f.bc}
	ctx := context.Background()
	require.NoError(t, deleteStoredVersion(ctx, f.mc, failing, deleteTestBucket, key, first))
	require.NoError(t, deleteStoredVersion(ctx, f.mc, failing, deleteTestBucket, key, second))

	found := tombstones(t, f.mc)
	require.Len(t, found, 2, "records: %v", found)

	byHash := make(map[[32]byte]DeletedObjectInfo, len(found))
	for _, info := range found {
		byHash[info.ObjectHash] = info
	}
	require.Contains(t, byHash, first, "the first version's shards are unaccounted for")
	require.Contains(t, byHash, second)

	for hash, info := range byHash {
		assert.Equal(t, deleteTestBucket, info.Bucket)
		assert.Equal(t, key, info.Key)
		assert.NotEmpty(t, info.DataShardNodes, "record for %x names no shard nodes", hash)
		assert.NotZero(t, info.WriteEpoch, "record for %x cannot tell a recreate from its own delete", hash)
	}
}

// A version delete writes its record under the epoch prefix, at the key that
// names its version hash and generation.
func TestVersionReclaimRecordUsesTheEpochPrefix(t *testing.T) {
	t.Parallel()

	const key = "nested/dir/a.txt"
	f := newWriteFixture(4, 2)
	versionHash := seedVersion(t, f, deleteTestBucket, key, "V1", []byte("body"))

	require.NoError(t, deleteStoredVersion(context.Background(), f.mc,
		&failingBlob{fakeBlob: f.bc}, deleteTestBucket, key, versionHash))

	found := tombstones(t, f.mc)
	require.Len(t, found, 1)
	for stored, info := range found {
		assert.True(t, strings.HasPrefix(stored, DeletedEpochPrefix),
			"%q is outside the epoch prefix", stored)
		assert.Equal(t, DeletedObjectKey(deleteTestBucket, key, versionHash, info.WriteEpoch), stored)
		assert.NotZero(t, info.WriteEpoch)
	}
}

// A record written in the old shape, before the version was in the key, still
// decodes and is still found: the tombstones most likely to need reclaiming
// are the ones written before the deploy.
func TestLegacyReclaimRecordIsStillFound(t *testing.T) {
	t.Parallel()

	const key = "legacy.txt"
	f := newWriteFixture(4, 2)
	objectHash := model.ObjectHash(deleteTestBucket, key)

	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(DeletedObjectInfo{
		Bucket:         deleteTestBucket,
		Key:            key,
		ObjectHash:     objectHash,
		DeletedAt:      1,
		WriteEpoch:     7,
		DataShardNodes: []config.NodeID{1, 2},
	}))
	require.NoError(t, metaPut(context.Background(), f.mc, model.TableObjects,
		DeletedObjectPrefix+deleteTestBucket+"/"+key, buf.Bytes()))

	found := tombstones(t, f.mc)
	require.Len(t, found, 1)
	info, ok := found[DeletedObjectPrefix+deleteTestBucket+"/"+key]
	require.True(t, ok, "the old key shape is no longer readable: %v", found)
	assert.Equal(t, objectHash, info.ObjectHash)
	assert.Equal(t, uint64(7), info.WriteEpoch)
}

// writeLegacyRecord stores a reclaim record in the layout used before the
// object hash was in the key.
func writeLegacyRecord(t *testing.T, mc MetaClient, bucket, key string) {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(DeletedObjectInfo{
		Bucket: bucket, Key: key, ObjectHash: model.ObjectHash(bucket, key),
		DeletedAt: 1, WriteEpoch: 7, DataShardNodes: []config.NodeID{1, 2},
	}))
	require.NoError(t, metaPut(context.Background(), mc, model.TableObjects,
		DeletedObjectPrefix+bucket+"/"+key, buf.Bytes()))
}

// Writing a version's record must not overwrite a legacy record for the same
// key, which is the collision the two layouts coexisting has to survive.
func TestVersionRecordDoesNotOverwriteALegacyOne(t *testing.T) {
	t.Parallel()

	const key = "a.txt"
	f := newWriteFixture(4, 2)
	writeLegacyRecord(t, f.mc, deleteTestBucket, key)

	versionHash := seedVersion(t, f, deleteTestBucket, key, "V1", []byte("body"))
	require.NoError(t, deleteStoredVersion(context.Background(), f.mc,
		&failingBlob{fakeBlob: f.bc}, deleteTestBucket, key, versionHash))

	found := tombstones(t, f.mc)
	require.Len(t, found, 2, "the version's record replaced the legacy one: %v", found)
}

// The other half of the collision, and the one that nearly shipped: a legacy
// record for an object key that itself ends in a hash must not be reachable by
// a new record for the key that hash belongs to.
//
// Laying the key out as bucket/key#hash made these two the same row, so the
// new write destroyed the only record of the legacy object's shards.
func TestVersionRecordDoesNotOverwriteALegacyRecordOfAHashSuffixedKey(t *testing.T) {
	t.Parallel()

	const key = "a.txt"
	f := newWriteFixture(4, 2)
	versionHash := seedVersion(t, f, deleteTestBucket, key, "V1", []byte("body"))

	// An object whose own key ends in the hash the new record would append.
	collidingKey := key + "#" + hex.EncodeToString(versionHash[:])
	writeLegacyRecord(t, f.mc, deleteTestBucket, collidingKey)

	require.NoError(t, deleteStoredVersion(context.Background(), f.mc,
		&failingBlob{fakeBlob: f.bc}, deleteTestBucket, key, versionHash))

	found := tombstones(t, f.mc)
	require.Len(t, found, 2, "a legacy record was overwritten: %v", found)

	legacy, ok := found[DeletedObjectPrefix+deleteTestBucket+"/"+collidingKey]
	require.True(t, ok, "the legacy record for %q is gone: %v", collidingKey, found)
	assert.Equal(t, collidingKey, legacy.Key)
	assert.Equal(t, model.ObjectHash(deleteTestBucket, collidingKey), legacy.ObjectHash)
}
