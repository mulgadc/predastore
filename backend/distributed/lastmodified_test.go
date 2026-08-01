package distributed

import (
	"bytes"
	"context"
	"encoding/gob"
	"testing"
	"time"

	"github.com/mulgadc/predastore/backend"
	s3db "github.com/mulgadc/predastore/s3db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixedClock returns a clock function that always reports t, so tests can
// assert on specific instants instead of racing time.Now().
func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

// legacyObjectToShardNodes mirrors the shape of ObjectToShardNodes before
// LastModified existed. It proves gob decodes old records into the zero
// time rather than erroring, since gob matches fields by name.
type legacyObjectToShardNodes struct {
	Object           [32]byte
	Size             int64
	DataShardNodes   []uint32
	ParityShardNodes []uint32
}

func TestFreshObjectLastModifiedAgreesAcrossHeadGetList(t *testing.T) {
	be := setupTestBackend(t)
	ctx := context.Background()

	writeTime := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	be.SetClock(fixedClock(writeTime))
	putTestObject(t, be, "test-bucket", "fresh.txt", 4096)

	headResp, err := be.HeadObject(ctx, "test-bucket", "fresh.txt")
	require.NoError(t, err)
	assert.True(t, headResp.LastModified.Equal(writeTime),
		"HeadObject LastModified = %v, want %v", headResp.LastModified, writeTime)

	getResp, err := be.GetObject(ctx, &backend.GetObjectRequest{
		Bucket:     "test-bucket",
		Key:        "fresh.txt",
		RangeStart: -1,
		RangeEnd:   -1,
	})
	require.NoError(t, err)
	getResp.Body.Close()
	assert.True(t, getResp.LastModified.Equal(writeTime),
		"GetObject LastModified = %v, want %v", getResp.LastModified, writeTime)

	listResp, err := be.ListObjects(ctx, &backend.ListObjectsRequest{Bucket: "test-bucket", Prefix: "fresh.txt"})
	require.NoError(t, err)
	require.Len(t, listResp.Contents, 1)
	assert.True(t, listResp.Contents[0].LastModified.Equal(writeTime),
		"ListObjects LastModified = %v, want %v", listResp.Contents[0].LastModified, writeTime)

	// The three read paths must agree exactly, not just each be individually plausible.
	assert.True(t, headResp.LastModified.Equal(getResp.LastModified),
		"HeadObject and GetObject disagree: %v vs %v", headResp.LastModified, getResp.LastModified)
	assert.True(t, headResp.LastModified.Equal(listResp.Contents[0].LastModified),
		"HeadObject and ListObjects disagree: %v vs %v", headResp.LastModified, listResp.Contents[0].LastModified)
}

func TestOverwriteAdvancesLastModified(t *testing.T) {
	be := setupTestBackend(t)
	ctx := context.Background()

	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	be.SetClock(fixedClock(t1))
	putTestObject(t, be, "test-bucket", "overwrite.txt", 1024)

	head1, err := be.HeadObject(ctx, "test-bucket", "overwrite.txt")
	require.NoError(t, err)
	require.True(t, head1.LastModified.Equal(t1))

	t2 := t1.Add(24 * time.Hour)
	be.SetClock(fixedClock(t2))
	putTestObject(t, be, "test-bucket", "overwrite.txt", 2048)

	head2, err := be.HeadObject(ctx, "test-bucket", "overwrite.txt")
	require.NoError(t, err)
	assert.True(t, head2.LastModified.Equal(t2),
		"overwrite did not advance LastModified: got %v, want %v", head2.LastModified, t2)
	assert.False(t, head2.LastModified.Equal(t1), "overwrite must not keep the original timestamp")
}

func TestListObjectsStableAcrossCalls(t *testing.T) {
	be := setupTestBackend(t)
	ctx := context.Background()

	writeTime := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	be.SetClock(fixedClock(writeTime))
	putTestObject(t, be, "test-bucket", "stable.txt", 512)

	first, err := be.ListObjects(ctx, &backend.ListObjectsRequest{Bucket: "test-bucket", Prefix: "stable.txt"})
	require.NoError(t, err)
	require.Len(t, first.Contents, 1)

	second, err := be.ListObjects(ctx, &backend.ListObjectsRequest{Bucket: "test-bucket", Prefix: "stable.txt"})
	require.NoError(t, err)
	require.Len(t, second.Contents, 1)

	assert.True(t, first.Contents[0].LastModified.Equal(second.Contents[0].LastModified),
		"ListObjects LastModified changed between two calls on an unmodified object: %v vs %v",
		first.Contents[0].LastModified, second.Contents[0].LastModified)
	assert.True(t, first.Contents[0].LastModified.Equal(writeTime))
}

// TestObjectToShardNodesDecodesLegacyGobWithZeroLastModified proves the
// gob-compatibility contract directly: a record encoded before LastModified
// existed decodes cleanly into the current struct with a zero timestamp.
func TestObjectToShardNodesDecodesLegacyGobWithZeroLastModified(t *testing.T) {
	legacy := legacyObjectToShardNodes{
		Object:           [32]byte{1, 2, 3},
		Size:             42,
		DataShardNodes:   []uint32{0, 1, 2},
		ParityShardNodes: []uint32{3, 4},
	}

	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(legacy))

	var decoded ObjectToShardNodes
	require.NoError(t, gob.NewDecoder(&buf).Decode(&decoded))

	assert.True(t, decoded.LastModified.IsZero(),
		"legacy record without LastModified must decode to the zero time, got %v", decoded.LastModified)
	assert.Equal(t, legacy.Size, decoded.Size)
}

// TestPreExistingObjectReportsZeroLastModifiedUntilRewritten asserts the
// chosen backward-compatibility policy: an object whose metadata predates
// the LastModified field reports the zero time on every read path until it
// is next written (PutObject/PutObjectFromPath/CompleteMultipartUpload all
// stamp LastModified on every call, including overwrites). No QUIC nodes
// are needed since HeadObject/ListObjects never read shard data.
func TestPreExistingObjectReportsZeroLastModifiedUntilRewritten(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &Config{
		BadgerDir:      tmpDir,
		PartitionCount: 5,
		Buckets: []BucketConfig{
			{Name: "test-bucket", Region: "us-east-1"},
		},
	}
	b, err := New(cfg)
	require.NoError(t, err)
	defer b.Close()

	be, ok := b.(*Backend)
	require.True(t, ok)

	ctx := context.Background()
	const key = "legacy.txt"

	objectHash := s3db.GenObjectHash("test-bucket", key)
	hashRingShards, err := be.HashRing().GetClosestN(objectHash[:], be.RsDataShard()+be.RsParityShard())
	require.NoError(t, err)

	legacy := legacyObjectToShardNodes{
		Object:           objectHash,
		Size:             777,
		DataShardNodes:   make([]uint32, be.RsDataShard()),
		ParityShardNodes: make([]uint32, be.RsParityShard()),
	}
	for i := 0; i < be.RsDataShard(); i++ {
		legacy.DataShardNodes[i], err = NodeToUint32(hashRingShards[i].String())
		require.NoError(t, err)
	}
	for i := 0; i < be.RsParityShard(); i++ {
		legacy.ParityShardNodes[i], err = NodeToUint32(hashRingShards[be.RsDataShard()+i].String())
		require.NoError(t, err)
	}

	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(legacy))
	require.NoError(t, be.GlobalState().Set(TableObjects, objectHash[:], buf.Bytes()))

	arnKey := []byte(arnObjectPrefix + "test-bucket/" + key)
	require.NoError(t, be.GlobalState().Set(TableObjects, arnKey, objectHash[:]))

	head, err := be.HeadObject(ctx, "test-bucket", key)
	require.NoError(t, err)
	assert.True(t, head.LastModified.IsZero(),
		"pre-existing object without a stored timestamp must report the zero time, got %v", head.LastModified)

	listResp, err := be.ListObjects(ctx, &backend.ListObjectsRequest{Bucket: "test-bucket", Prefix: key})
	require.NoError(t, err)
	require.Len(t, listResp.Contents, 1)
	assert.True(t, listResp.Contents[0].LastModified.IsZero(),
		"ListObjects for a pre-existing object must report the zero time, not a fabricated one, got %v",
		listResp.Contents[0].LastModified)
}
