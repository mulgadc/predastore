package blob_test

import (
	"context"
	"strings"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Two writers of one shard position, the earlier one publishing its placement
// record after the later one has already committed. This is the ordering that
// used to strand an object: the record named a generation no node held, every
// read failed on the epoch, and repair could not mend it because repair rebuilds
// at the epoch the record names.
//
// Neither writer is failed and both generations are readable. Which one the
// object ends up as is decided by the record, and the record only moves
// forward, so the later writer wins — but the earlier writer was told its write
// landed, and for as long as a record can still name it, it did.
func TestAnEarlierWriterIsNotStrandedByALaterOne(t *testing.T) {
	c := startBlobNode(t)
	earlier := []byte("earlier PUT")
	later := []byte("later PUT")

	// Prepared rows are keyed by epoch, so the second prepare takes nothing
	// from the first: each writer owns its own in-flight extent.
	put(t, c, 1, earlier)
	put(t, c, 2, later)

	require.NoError(t, commit(t, c, 2))

	// Losing the race is not an error. The writer is told its write was
	// superseded, not that it failed.
	require.NoError(t, commit(t, c, 1))

	got, err := get(t, c, 2)
	require.NoError(t, err)
	assert.Equal(t, later, got)

	// The generation a record may still name is served from the retained
	// namespace rather than refused.
	got, err = get(t, c, 1)
	require.NoError(t, err, "a record naming the superseded generation must still resolve")
	assert.Equal(t, earlier, got)
}

// A prepare arriving after a higher epoch has already committed is accepted:
// it writes its own row and cannot displace anything. Refusing it here is what
// made a lost race look like an unreachable node to the write path, which
// failed the whole PUT with a 500 for what is an ordinary concurrent write.
func TestALowerEpochPrepareIsAcceptedAndLosesAtCommit(t *testing.T) {
	c := startBlobNode(t)

	put(t, c, 2, []byte("higher timestamp"))
	require.NoError(t, commit(t, c, 2))

	lower := "lower timestamp"
	_, err := c.Put(context.Background(), epochServerNode, blob.PutRequest{
		Key: epochKey(), Index: epochTestShardIndex, Size: int64(len(lower)), Epoch: 1,
	}, strings.NewReader(lower))
	require.NoError(t, err, "a lower epoch prepares its own row and displaces nothing")

	require.NoError(t, commit(t, c, 1))

	// Forward-only: the commit published nothing and the live generation is
	// still the higher epoch.
	got, err := get(t, c, 2)
	require.NoError(t, err)
	assert.Equal(t, []byte("higher timestamp"), got)
}
