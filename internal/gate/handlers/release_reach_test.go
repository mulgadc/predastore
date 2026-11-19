package handlers

import (
	"bytes"
	"context"
	"testing"

	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A release has to reach every node that holds the generation, and the handoff
// holder is not in the placement record: it is derived from the ring. Missing
// it leaves a retained generation nothing will ever release, and the sweep no
// longer ages one out.
func TestAReleaseReachesTheHandoffHolder(t *testing.T) {
	t.Parallel()

	f := newHandoffFixture(2, 1, 1)
	objectHash := model.ObjectHash("b", "k")
	owners, err := f.ring.Nodes(objectHash, 4)
	require.NoError(t, err)
	owner, holder := owners[1], owners[3]

	// The owner of shard 1 is down, so that shard lands on the spare.
	f.bc.mu.Lock()
	f.bc.down[owner] = true
	f.bc.mu.Unlock()

	ctx := context.Background()
	first, _, err := f.write(ctx, objectHash, randomBytes(t, 1<<16))
	require.NoError(t, err)
	require.Equal(t, holder, mustHolder(t, f, objectHash, 1), "the refused shard has to be on the spare node")

	second, _, err := f.write(ctx, objectHash, randomBytes(t, 1<<16))
	require.NoError(t, err)

	record, err := EncodePlacement(first)
	require.NoError(t, err)
	releaseSuperseded(ctx, f.bc, objectHash, record, second.WriteEpoch, handoffNode(f.ring, f.cfg, objectHash))

	assert.Contains(t, f.bc.releasedEpochs(holder, objectHash, 1), first.WriteEpoch,
		"the handoff holder was never told the generation it holds is finished with")

	// And the recorded owners are still released, which is the path that
	// already worked.
	assert.Contains(t, f.bc.releasedEpochs(first.AllNodes()[0], objectHash, 0), first.WriteEpoch)
}

// Re-uploading a part number replaces the part, so the generation it displaces
// has to be released. Publishing the part's record without learning what it
// displaced released nothing, and nothing else reclaims a part generation.
//
// This covers the sequential re-upload. Two uploads of one part number racing
// each other is the reason the commit now runs before the swap, and this does
// not cover that: the node here cannot be paused between the two.
func TestAPartReUploadReleasesTheGenerationItReplaces(t *testing.T) {
	t.Parallel()

	f := newHandoffFixture(2, 1, 1)
	ctx := context.Background()
	const uploadID, partNumber = "upload-1", 1

	first := randomBytes(t, 1<<16)
	_, _, err := storePart(ctx, f.mc, f.bc, f.ring, f.cfg,
		"b", "k", uploadID, partNumber, bytes.NewReader(first), int64(len(first)), nil)
	require.NoError(t, err)

	objectHash := model.ObjectHash("b", partObjectKey("k", uploadID, partNumber))
	firstEpoch := mustPartEpoch(t, f, uploadID, partNumber)

	second := randomBytes(t, 1<<16)
	meta, _, err := storePart(ctx, f.mc, f.bc, f.ring, f.cfg,
		"b", "k", uploadID, partNumber, bytes.NewReader(second), int64(len(second)), nil)
	require.NoError(t, err)
	wantETag := model.NewPartETagHasher()
	_, err = wantETag.Write(second)
	require.NoError(t, err)
	assert.Equal(t, model.PartETagFrom(wantETag), meta.ETag, "the re-upload has to be the part that wins")

	owners, err := f.ring.Nodes(objectHash, f.cfg.TotalShards())
	require.NoError(t, err)
	assert.Contains(t, f.bc.releasedEpochs(owners[0], objectHash, 0), firstEpoch,
		"the replaced part generation was never released, so nothing will reclaim it")
}

// mustHolder reports which node holds a committed shard.
func mustHolder(t *testing.T, f handoffFixture, key [32]byte, index uint32) config.NodeID {
	t.Helper()
	for _, node := range f.nodes {
		if _, ok := f.bc.held(node, key, index); ok {
			return node
		}
	}
	t.Fatalf("no node holds shard %d", index)

	return 0
}

// mustPartEpoch reads the epoch of the generation a part's record names.
func mustPartEpoch(t *testing.T, f handoffFixture, uploadID string, partNumber int) uint64 {
	t.Helper()
	raw, err := metaGet(context.Background(), f.mc, model.TableObjects, partShardKey(uploadID, partNumber))
	require.NoError(t, err)
	place, err := DecodePlacement(raw)
	require.NoError(t, err)

	return place.WriteEpoch
}
