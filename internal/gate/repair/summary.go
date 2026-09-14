package repair

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Pass outcomes, as the summary line and the admin endpoint report them.
const (
	OutcomeComplete  = "complete"
	OutcomeDeferred  = "deferred"
	OutcomeFailed    = "failed"
	OutcomeCancelled = "cancelled"
	OutcomeNotReady  = "not_ready"
)

// PassSummary is what one pass did. It is logged at the end of every pass and
// served on the admin port, so it carries counts and never addresses or keys.
type PassSummary struct {
	Outcome    string    `json:"outcome"`
	Reason     string    `json:"reason,omitempty"`
	Finished   time.Time `json:"finished"`
	DurationMs int64     `json:"duration_ms"`

	Scanned int64 `json:"scanned"`
	Owned   int64 `json:"owned"`
	Owed    int64 `json:"owed"`
	// Remaining is what the pass left owed.
	Remaining int64 `json:"remaining"`

	RepairedCommit  int64 `json:"repaired_commit"`
	RepairedStandby int64 `json:"repaired_standby"`
	RepairedRebuild int64 `json:"repaired_rebuild"`
	Superseded      int64 `json:"superseded"`

	FailedPeerUnreachable int64 `json:"failed_peer_unreachable"`
	FailedPeerOtherEpoch  int64 `json:"failed_peer_other_epoch"`
	FailedPeerMissing     int64 `json:"failed_peer_missing"`
	FailedOther           int64 `json:"failed_other"`

	// OwedStreak counts consecutive passes, this one included, that found
	// shards owed.
	OwedStreak int64 `json:"owed_streak"`
}

// owedStreakWarning is how many consecutive passes may find shards owed before
// it is worth a warning. One is ordinary after a degraded write or a restart.
const owedStreakWarning = 2

// repairRoute is how an owed shard was restored.
type repairRoute int

const (
	routeCommit repairRoute = iota + 1
	routeStandby
	routeRebuild
)

func (s *Service) countRoute(route repairRoute) {
	switch route {
	case routeCommit:
		s.repairedCommit.Add(1)
	case routeStandby:
		s.repairedStandby.Add(1)
	case routeRebuild:
		s.repairedRebuild.Add(1)
	}
}

// summarise records and logs what the pass that began at start did, as the
// difference between the counters then and now.
func (s *Service) summarise(ctx context.Context, before Stats, start time.Time, err error) {
	after := s.Stats()
	sum := PassSummary{
		Outcome:    outcomeOf(err),
		Finished:   time.Now(),
		DurationMs: time.Since(start).Milliseconds(),

		Scanned:   after.Scanned - before.Scanned,
		Owned:     after.Owned - before.Owned,
		Owed:      after.Owed - before.Owed,
		Remaining: after.Pending,

		RepairedCommit:  after.RepairedCommit - before.RepairedCommit,
		RepairedStandby: after.RepairedStandby - before.RepairedStandby,
		RepairedRebuild: after.RepairedRebuild - before.RepairedRebuild,
		Superseded:      after.Superseded - before.Superseded,

		FailedPeerUnreachable: after.FailedPeerUnreachable - before.FailedPeerUnreachable,
		FailedPeerOtherEpoch:  after.FailedPeerOtherEpoch - before.FailedPeerOtherEpoch,
		FailedPeerMissing:     after.FailedPeerMissing - before.FailedPeerMissing,
		FailedOther:           after.FailedOther - before.FailedOther,
	}
	switch sum.Outcome {
	case OutcomeDeferred:
		sum.Reason = "no peer of an owed shard answered"
	case OutcomeFailed:
		sum.Reason = "scan of placement records failed"
	}
	if sum.Owed > 0 {
		sum.OwedStreak = s.owedStreak.Add(1)
	} else {
		s.owedStreak.Store(0)
	}
	s.lastPass.Store(&sum)

	slog.InfoContext(ctx, "Repair pass summary",
		"outcome", sum.Outcome,
		"scanned", sum.Scanned,
		"owned", sum.Owned,
		"owed", sum.Owed,
		"repaired_commit", sum.RepairedCommit,
		"repaired_standby", sum.RepairedStandby,
		"repaired_rebuild", sum.RepairedRebuild,
		"superseded", sum.Superseded,
		"failed_peer_unreachable", sum.FailedPeerUnreachable,
		"failed_peer_other_epoch", sum.FailedPeerOtherEpoch,
		"failed_peer_missing", sum.FailedPeerMissing,
		"failed_other", sum.FailedOther,
		"remaining", sum.Remaining,
		"duration_ms", sum.DurationMs)

	if sum.OwedStreak >= owedStreakWarning {
		slog.WarnContext(ctx, "Repair found shards owed on consecutive passes",
			"consecutive_passes", sum.OwedStreak, "owed", sum.Owed, "remaining", sum.Remaining)
	}
}

func outcomeOf(err error) string {
	switch {
	case err == nil:
		return OutcomeComplete
	case errors.Is(err, ErrPassDeferred):
		return OutcomeDeferred
	case errors.Is(err, context.Canceled):
		return OutcomeCancelled
	default:
		return OutcomeFailed
	}
}
