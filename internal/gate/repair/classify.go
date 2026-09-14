package repair

import (
	"errors"
	"fmt"

	"github.com/mulgadc/predastore/internal/blob"
)

// ErrPassDeferred reports a pass stopped because no peer of an owed shard
// answered. Nothing it would have concluded could be trusted, so nothing failed.
var ErrPassDeferred = errors.New("repair pass deferred: no peer of an owed shard answered")

// failReason names why a shard could not be repaired.
type failReason string

const (
	reasonPeerUnreachable failReason = "peer_unreachable"
	reasonPeerOtherEpoch  failReason = "peer_other_epoch"
	reasonPeerMissing     failReason = "peer_missing"
	reasonOther           failReason = "other"
)

// peerShortfall is a rebuild that found too few peers at the record's epoch,
// with the reason each of the others could not contribute.
type peerShortfall struct {
	have, need int
	epoch      uint64

	unreachable, otherEpoch, missing int
}

// count files one peer's refusal under its cause. Anything that is not an
// answer about the shard is treated as not having reached the peer.
func (p *peerShortfall) count(err error) {
	switch {
	case errors.Is(err, blob.ErrEpochMismatch):
		p.otherEpoch++
	case errors.Is(err, blob.ErrNotFound):
		p.missing++
	default:
		p.unreachable++
	}
}

func (p *peerShortfall) Error() string {
	return fmt.Sprintf("%s: %d of %d peers hold epoch %016x (%d unreachable, %d at another epoch, %d missing the shard)",
		p.reason(), p.have, p.need, p.epoch, p.unreachable, p.otherEpoch, p.missing)
}

func (p *peerShortfall) Unwrap() error { return errTooFewPeers }

// reason names the shortfall by its most transient cause: a shard with a peer
// that did not answer has not been shown to be unrebuildable.
func (p *peerShortfall) reason() failReason {
	switch {
	case p.unreachable > 0:
		return reasonPeerUnreachable
	case p.otherEpoch > 0:
		return reasonPeerOtherEpoch
	default:
		return reasonPeerMissing
	}
}

// classify names the cause of a failed repair.
func classify(err error) failReason {
	var short *peerShortfall
	if errors.As(err, &short) {
		return short.reason()
	}

	return reasonOther
}

// noPeerReachable reports a rebuild where no peer answered at all. That is a
// fact about this node's view of the cluster, not about the shard.
func noPeerReachable(err error) bool {
	var short *peerShortfall

	return errors.As(err, &short) &&
		short.have == 0 && short.otherEpoch == 0 && short.missing == 0 && short.unreachable > 0
}

// countFailure records a failed shard under its cause and returns the cause.
func (s *Service) countFailure(err error) failReason {
	s.failed.Add(1)
	reason := classify(err)
	switch reason {
	case reasonPeerUnreachable:
		s.failedPeerUnreachable.Add(1)
	case reasonPeerOtherEpoch:
		s.failedPeerOtherEpoch.Add(1)
	case reasonPeerMissing:
		s.failedPeerMissing.Add(1)
	default:
		s.failedOther.Add(1)
	}

	return reason
}
