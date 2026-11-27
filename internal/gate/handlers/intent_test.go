package handlers

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testIntent() Intent {
	return Intent{
		State:   IntentPending,
		Bucket:  "bkt",
		Key:     "lock",
		Nodes:   []config.NodeID{1, 2, 3},
		Handoff: 4,
		Release: &IntentRelease{Epoch: 7, Nodes: []config.NodeID{1, 2, 3}},
		Created: time.Unix(1700000000, 0).UTC(),
	}
}

// A stored key names its hash and epoch, and no row of another table, nor a
// truncated or zero-epoch intent key, reads as one.
func TestIntentKeyParsesOnlyIntentRows(t *testing.T) {
	id := IntentID{Hash: model.ObjectHash("bkt", "lock"), Epoch: 0x0102030405060708}

	got, ok := ParseIntentKey(id.Key())
	require.True(t, ok)
	assert.Equal(t, id, got)

	for _, stored := range []string{
		TableKey(model.TableObjects, string(id.Hash[:])),
		id.Key()[:len(id.Key())-1],
		IntentID{Hash: id.Hash}.Key(),
	} {
		_, ok := ParseIntentKey(stored)
		assert.False(t, ok, "%q parsed as an intent key", stored)
	}
}

// Two writers of one key at different epochs never share a row.
func TestIntentKeysOfOneObjectDifferByEpoch(t *testing.T) {
	hash := model.ObjectHash("bkt", "lock")
	assert.NotEqual(t, IntentID{hash, 1}.Key(), IntentID{hash, 2}.Key())
}

// A row of an unknown format or state is refused rather than read as an
// intent the reaper would act on.
func TestDecodeIntentRefusesUnknownRows(t *testing.T) {
	row, err := EncodeIntent(testIntent())
	require.NoError(t, err)
	got, err := DecodeIntent(row)
	require.NoError(t, err)
	assert.Equal(t, testIntent(), got)

	for name, data := range map[string]string{
		"format": `{"v":2,"state":"pending","nodes":[1]}`,
		"state":  `{"v":1,"state":"done","nodes":[1]}`,
		"nodes":  `{"v":1,"state":"pending"}`,
		"json":   `not json`,
	} {
		_, err := DecodeIntent([]byte(data))
		assert.Error(t, err, name)
	}
}

// intentBlob records what settling sent, and answers each position with the
// error its operation's map names. The handoff node holds nothing, as when no
// shard was handed off.
type intentBlob struct {
	mu                               sync.Mutex
	commits, aborts, releases        []shardPos
	commitErr, abortErr, releaseErr map[shardPos]error
}

func (b *intentBlob) answer(errs map[shardPos]error, p shardPos) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return errs[p]
}

func (b *intentBlob) record(list *[]shardPos, node config.NodeID, index uint32) shardPos {
	p := shardPos{node, int(index)}
	b.mu.Lock()
	*list = append(*list, p)
	b.mu.Unlock()
	return p
}

func (b *intentBlob) Commit(_ context.Context, node config.NodeID, req blob.CommitRequest) (bool, error) {
	p := b.record(&b.commits, node, req.Index)
	if err, ok := b.commitErr[p]; ok {
		return false, err
	}
	if node == 4 {
		return false, blob.ErrNotPrepared
	}
	return false, nil
}

func (b *intentBlob) Abort(_ context.Context, node config.NodeID, req blob.CommitRequest) error {
	return b.answer(b.abortErr, b.record(&b.aborts, node, req.Index))
}

func (b *intentBlob) Release(_ context.Context, node config.NodeID, req blob.ReleaseRequest) error {
	return b.answer(b.releaseErr, b.record(&b.releases, node, req.Index))
}

func sortedPos(ps []shardPos) []shardPos {
	out := slices.Clone(ps)
	slices.SortFunc(out, func(a, b shardPos) int {
		if a.index != b.index {
			return a.index - b.index
		}
		return int(a.node) - int(b.node)
	})
	return out
}

// Every action reaches the owner and the handoff node at every index.
var allCandidates = []shardPos{{1, 0}, {4, 0}, {2, 1}, {4, 1}, {3, 2}, {4, 2}}

// A commit settles once every candidate answers, not-prepared included, and
// only then releases the displaced generation, handoff node included.
func TestCommitIntentReleasesOnlyAfterEveryCandidateAnswered(t *testing.T) {
	id := IntentID{Hash: model.ObjectHash("bkt", "lock"), Epoch: 9}
	bc := &intentBlob{commitErr: map[shardPos]error{{2, 1}: blob.ErrOvertakenUnproven}}

	got := CommitIntent(t.Context(), bc, id, testIntent())

	assert.Equal(t, IntentCommit{Durable: 2, Complete: true}, got)
	assert.Equal(t, sortedPos(allCandidates), sortedPos(bc.commits))
	assert.Equal(t, sortedPos(allCandidates), sortedPos(bc.releases))
}

// An unreachable candidate keeps the intent and sends no release, because
// the displaced generation there may still be live.
func TestCommitIntentWithAnUnreachableCandidateIsIncomplete(t *testing.T) {
	id := IntentID{Hash: model.ObjectHash("bkt", "lock"), Epoch: 9}
	bc := &intentBlob{commitErr: map[shardPos]error{{3, 2}: errors.New("unreachable")}}

	got := CommitIntent(t.Context(), bc, id, testIntent())

	assert.False(t, got.Complete)
	assert.Equal(t, 2, got.Durable)
	assert.Empty(t, bc.releases)
}

// A failed release keeps the intent.
func TestCommitIntentWithAFailedReleaseIsIncomplete(t *testing.T) {
	id := IntentID{Hash: model.ObjectHash("bkt", "lock"), Epoch: 9}
	bc := &intentBlob{releaseErr: map[shardPos]error{{4, 2}: errors.New("unreachable")}}
	in := testIntent()

	assert.False(t, CommitIntent(t.Context(), bc, id, in).Complete)
}

// An abort that finds a writer still open, or a node that does not answer,
// keeps the intent; every candidate is still sent the abort.
func TestAbortIntentConfirmsOnlyWhenNothingIsOpen(t *testing.T) {
	id := IntentID{Hash: model.ObjectHash("bkt", "lock"), Epoch: 9}

	bc := &intentBlob{}
	assert.True(t, AbortIntent(t.Context(), bc, id, testIntent()))
	assert.Equal(t, sortedPos(allCandidates), sortedPos(bc.aborts))

	bc = &intentBlob{abortErr: map[shardPos]error{{2, 1}: blob.ErrWriterOpen}}
	assert.False(t, AbortIntent(t.Context(), bc, id, testIntent()))
	assert.Len(t, bc.aborts, len(allCandidates))
}

// A fence moves a pending intent to aborted once; after it, or after the
// write's own commit, a fence reports what won and changes nothing.
func TestFenceIntentDecidesAPendingIntentOnce(t *testing.T) {
	ctx := t.Context()
	mc := newFakeMeta()
	id := IntentID{Hash: model.ObjectHash("bkt", "lock"), Epoch: 9}
	pending, err := EncodeIntent(testIntent())
	require.NoError(t, err)
	require.NoError(t, mc.Put(ctx, id.Key(), pending))

	result, in, err := FenceIntent(ctx, mc, id, pending)
	require.NoError(t, err)
	assert.Equal(t, FenceApplied, result)
	assert.Equal(t, IntentAborted, in.State)

	result, _, err = FenceIntent(ctx, mc, id, pending)
	require.NoError(t, err)
	assert.Equal(t, FenceAborted, result)

	committed, err := EncodeIntent(testIntent().withState(IntentCommitted))
	require.NoError(t, err)
	require.NoError(t, mc.Put(ctx, id.Key(), committed))
	result, in, err = FenceIntent(ctx, mc, id, pending)
	require.NoError(t, err)
	assert.Equal(t, FenceCommitted, result)
	assert.Equal(t, IntentCommitted, in.State)
	stored, err := mc.Get(ctx, id.Key())
	require.NoError(t, err)
	assert.Equal(t, committed, stored)

	require.NoError(t, mc.Delete(ctx, id.Key()))
	result, _, err = FenceIntent(ctx, mc, id, pending)
	require.NoError(t, err)
	assert.Equal(t, FenceGone, result)
}

// A refused or unknown fence decides nothing.
func TestFenceIntentWithoutAnAnswerIsUnknown(t *testing.T) {
	id := IntentID{Hash: model.ObjectHash("bkt", "lock"), Epoch: 9}
	pending, err := EncodeIntent(testIntent())
	require.NoError(t, err)
	for _, outcome := range []meta.TxnOutcome{meta.TxnRefused, meta.TxnUnknown} {
		result, _, err := FenceIntent(t.Context(), stuckTxn{outcome}, id, pending)
		assert.Equal(t, FenceUnknown, result)
		assert.Error(t, err)
	}
}

type stuckTxn struct{ outcome meta.TxnOutcome }

func (s stuckTxn) Txn(context.Context, []meta.TxnCheck, []meta.TxnWrite) (meta.TxnOutcome, []meta.TxnValue, error) {
	return s.outcome, nil, errors.New("no answer")
}
