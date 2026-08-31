package repair

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/klauspost/reedsolomon"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
)

// repairShard restores one shard to the generation its record names.
//
// The cheap case first: a node that prepared the shard and never had the commit
// driven home holds the right bytes already, invisible. Publishing them is one
// round trip and no reconstruction, and it is the same forward recovery a read
// performs, so asking costs nothing when the answer is no.
func (s *Service) repairShard(ctx context.Context, t task) error {
	// Superseded means the node has moved past this generation and published
	// nothing, so nothing was repaired and the other routes still have to run.
	superseded, err := s.cfg.Blob.Commit(ctx, t.node, blob.CommitRequest{
		Key: t.hash, Index: uint32(t.index), Epoch: t.place.WriteEpoch, //nolint:gosec // G115: bounded by the shard count.
	})
	switch {
	case err == nil && !superseded:
		slog.DebugContext(ctx, "Repaired a shard by publishing what its node had prepared",
			"node", t.node, "index", t.index)

		return nil
	case err != nil && !errors.Is(err, blob.ErrNotPrepared):
		return fmt.Errorf("publish prepared shard: %w", err)
	}

	if err := s.pullFromHandoff(ctx, t); err == nil {
		return nil
	} else if !errors.Is(err, errNoHandoff) {
		return err
	}

	return s.rebuildShard(ctx, t)
}

// errNoHandoff reports that the handoff holder has nothing for this position,
// which is the ordinary case and not a failure.
var errNoHandoff = errors.New("no shard on the handoff holder")

// pullFromHandoff moves a handed-off shard back to the node that owns it.
//
// A write whose owner refused puts the shard one step along the ring, and that
// node is derived rather than recorded, so it can be asked without a hint
// having been stored anywhere. Streaming those bytes back is one transfer and
// no decode, against k transfers and a decode to rebuild the same shard, so it
// is worth asking even though the usual answer is no.
func (s *Service) pullFromHandoff(ctx context.Context, t task) error {
	holder := s.handoffNode(t.hash)
	if holder == 0 || holder == t.node {
		return errNoHandoff
	}

	body, err := s.cfg.Blob.Get(ctx, holder, blob.GetRequest{
		Key: t.hash, Index: uint32(t.index), //nolint:gosec // G115: bounded by the shard count.
		RangeStart: -1, RangeEnd: -1, Epoch: t.place.WriteEpoch,
	})
	if err != nil {
		return errNoHandoff
	}
	defer body.Close()

	shardSize := (t.place.Size + int64(s.cfg.DataShards) - 1) / int64(s.cfg.DataShards)
	if _, err := s.cfg.Blob.Put(ctx, t.node, blob.PutRequest{
		Key: t.hash, Index: uint32(t.index), //nolint:gosec // G115: bounded by the shard count.
		Size: shardSize, Epoch: t.place.WriteEpoch,
	}, body); err != nil {
		return fmt.Errorf("return handed-off shard to its owner: %w", err)
	}
	if err := s.publish(ctx, t); err != nil {
		return err
	}

	// Only once the owner holds it. A holder emptied before that would turn a
	// failed return into the loss the handoff existed to prevent.
	if _, err := s.cfg.Blob.Delete(ctx, holder, blob.DeleteRequest{
		Key: t.hash, Index: uint32(t.index), //nolint:gosec // G115: bounded by the shard count.
	}); err != nil {
		slog.WarnContext(ctx, "Handed-off shard returned but not released on its holder",
			"holder", holder, "owner", t.node, "index", t.index, "err", err)
	}
	slog.InfoContext(ctx, "Handed-off shard returned to its owner",
		"holder", holder, "owner", t.node, "index", t.index)

	return nil
}

// handoffNode derives where a write would have put a shard its owner refused:
// one step off the end of the stripe on the ring, the same position the write
// path and the read path compute. Zero means the cluster has no node to spare.
func (s *Service) handoffNode(hash [32]byte) config.NodeID {
	total := s.cfg.DataShards + s.cfg.ParityShards
	nodes, err := s.cfg.Ring.Nodes(hash, total+1)
	if err != nil || len(nodes) <= total {
		return 0
	}

	return nodes[total]
}

// rebuildShard reconstructs the shard from its peers and writes it back.
//
// Any DataShards of the stripe rebuild any one of them, so there is no
// difference between restoring a data shard and restoring a parity shard: both
// are one Reconstruct with a single output slot open, and the arithmetic is the
// erasure coder's rather than written out here.
func (s *Service) rebuildShard(ctx context.Context, t task) error {
	total := s.cfg.DataShards + s.cfg.ParityShards
	shardSize := (t.place.Size + int64(s.cfg.DataShards) - 1) / int64(s.cfg.DataShards)

	sources, closeSources, err := s.fetchPeers(ctx, t, total)
	if err != nil {
		return err
	}
	defer closeSources()

	enc, err := reedsolomon.NewStream(s.cfg.DataShards, s.cfg.ParityShards)
	if err != nil {
		return err
	}

	// The rebuilt shard is streamed straight into the put rather than buffered:
	// a bounded pool each holding a shard of a large object is a memory profile
	// that fails exactly when the cluster is already degraded.
	pr, pw := io.Pipe()
	fill := make([]io.Writer, total)
	fill[t.index] = pw

	var wg sync.WaitGroup
	wg.Go(func() {
		// CloseWithError is what unblocks the put if reconstruction fails, and
		// the plain close is what gives it its EOF.
		pw.CloseWithError(enc.Reconstruct(sources, fill))
	})

	_, putErr := s.cfg.Blob.Put(ctx, t.node, blob.PutRequest{
		Key:   t.hash,
		Index: uint32(t.index), //nolint:gosec // G115: bounded by the shard count.
		Size:  shardSize,
		Epoch: t.place.WriteEpoch,
	}, pr)
	// Whatever happened, the reconstructor must not be left writing into a pipe
	// nobody reads: io.Pipe does not observe ctx and would block forever.
	pr.CloseWithError(putErr)
	wg.Wait()

	if putErr != nil {
		return fmt.Errorf("write rebuilt shard: %w", putErr)
	}

	return s.publish(ctx, t)
}

// publish commits the rebuilt shard, but only after confirming the record still
// names the generation it was built for.
//
// A write that landed while the rebuild was in flight has already published a
// newer generation on this node, and committing over it would demote a fresh
// shard to a stale one. The check does not close that window completely — the
// record can move between this read and the commit — but the residual case is
// a shard the next read discards on epoch mismatch and the next pass rebuilds,
// which is a wasted repair rather than a wrong object.
func (s *Service) publish(ctx context.Context, t task) error {
	current, err := s.currentEpoch(ctx, t.hash)
	if err != nil {
		return errors.Join(fmt.Errorf("re-read placement record: %w", err), s.discard(ctx, t))
	}
	if current != t.place.WriteEpoch {
		slog.InfoContext(ctx, "Object was rewritten during its repair; discarding the rebuilt shard",
			"node", t.node, "index", t.index,
			"built_for", fmt.Sprintf("%016x", t.place.WriteEpoch),
			"record_now", fmt.Sprintf("%016x", current))

		return s.discard(ctx, t)
	}

	// A rebuild overtaken by a live write is not a failure: the newer generation
	// is the one the cluster wants, and it arrived without this sweep's help.
	superseded, err := s.cfg.Blob.Commit(ctx, t.node, blob.CommitRequest{
		Key: t.hash, Index: uint32(t.index), Epoch: t.place.WriteEpoch, //nolint:gosec // G115: bounded by the shard count.
	})
	if err != nil {
		return fmt.Errorf("commit rebuilt shard: %w", err)
	}
	if superseded {
		slog.DebugContext(ctx, "A live write overtook a rebuilt shard",
			"node", t.node, "index", t.index)
	}

	return nil
}

// discard releases a rebuilt shard that must not be published.
func (s *Service) discard(ctx context.Context, t task) error {
	if err := s.cfg.Blob.Abort(ctx, t.node, blob.CommitRequest{
		Key: t.hash, Index: uint32(t.index), Epoch: t.place.WriteEpoch, //nolint:gosec // G115: bounded by the shard count.
	}); err != nil {
		return fmt.Errorf("discard rebuilt shard: %w", err)
	}

	return nil
}

// currentEpoch reads the epoch the object's record names right now.
func (s *Service) currentEpoch(ctx context.Context, hash [32]byte) (uint64, error) {
	raw, err := s.cfg.Meta.Get(ctx, handlers.TableKey(model.TableObjects, string(hash[:])))
	if err != nil {
		return 0, err
	}
	place, err := handlers.DecodePlacement(raw)
	if err != nil {
		return 0, err
	}

	return place.WriteEpoch, nil
}

// fetchPeers opens streams to enough peers to rebuild with, and returns a
// reader slice positioned by shard index with nil everywhere it has none.
//
// Every stream demands the record's epoch, so a peer holding another generation
// refuses rather than contributing. Reconstructing from a mixture of
// generations is precisely the corruption this whole design exists to remove,
// and repair is the one place that would do it deliberately if nobody said
// otherwise.
func (s *Service) fetchPeers(
	ctx context.Context, t task, total int,
) ([]io.Reader, func(), error) {
	readers := make([]io.Reader, total)
	var opened []io.Closer
	closeAll := func() {
		for _, c := range opened {
			_ = c.Close()
		}
	}

	have := 0
	for index, node := range t.place.AllNodes() {
		if index == t.index || have >= s.cfg.DataShards {
			continue
		}
		r, err := s.cfg.Blob.Get(ctx, node, blob.GetRequest{
			Key:        t.hash,
			Index:      uint32(index),
			RangeStart: -1,
			RangeEnd:   -1,
			Epoch:      t.place.WriteEpoch,
		})
		if err != nil {
			slog.DebugContext(ctx, "Peer shard unusable for a repair",
				"node", node, "index", index, "err", err)

			continue
		}
		readers[index] = r
		opened = append(opened, r)
		have++
	}

	if have < s.cfg.DataShards {
		closeAll()

		return nil, nil, fmt.Errorf("%w: %d of %d peers hold epoch %016x",
			errTooFewPeers, have, s.cfg.DataShards, t.place.WriteEpoch)
	}

	return readers, closeAll, nil
}

// errTooFewPeers reports that not enough peers hold the record's generation for
// anything to be rebuilt from them. It is the honest outcome: the object is
// already below what the parity covers, and inventing a shard from a mixture
// would replace a detectable loss with an undetectable one.
var errTooFewPeers = errors.New("too few peers at the record epoch to rebuild")
