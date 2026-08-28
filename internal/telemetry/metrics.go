// Package telemetry holds predastore's own OpenTelemetry instruments: meta
// raft consensus state, multipart upload counters and shard read errors. The
// OTel bootstrap itself lives in bluebottle/pkg/otelsetup, which cmd/s3d calls
// directly, and per-request HTTP metrics come from that package's middleware.
package telemetry

import (
	"context"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// meterName identifies the predastore meter, matching the package import path
// convention used by bluebottle/pkg/otelsetup.
const meterName = "github.com/mulgadc/predastore/internal/telemetry"

var (
	instrumentsOnce sync.Once
	meter           metric.Meter

	raftState        metric.Int64ObservableGauge
	raftTerm         metric.Int64ObservableGauge
	raftCommitIndex  metric.Int64ObservableGauge
	raftAppliedIndex metric.Int64ObservableGauge
	raftAppliedLag   metric.Int64ObservableGauge
	raftLeaderKnown  metric.Int64ObservableGauge

	multipartUploads     metric.Int64Counter
	multipartSessions    metric.Int64UpDownCounter
	multipartPartCount   metric.Int64Counter
	multipartPartBytes   metric.Int64Counter
	multipartPartFetches metric.Int64Counter

	shardErrors  metric.Int64Counter
	shardHedges  metric.Int64Counter
	shardTTFB    metric.Int64Histogram
	shardBytes   metric.Int64Counter
	shardStalls  metric.Int64Counter
	shardLongest metric.Int64Histogram
	shardRate    metric.Int64Histogram
)

// instruments lazily creates the shared instruments. The global meter
// delegates to the real provider once Init installs one; before that (or when
// export is disabled) every recorded call is a cheap no-op.
func instruments() {
	instrumentsOnce.Do(func() {
		meter = otel.Meter(meterName)
		var err error

		raftState, err = meter.Int64ObservableGauge("predastore.meta.raft.state",
			metric.WithDescription("Constant 1 carrying the replica's own raft.State() as an attribute."),
			metric.WithUnit("{replica}"))
		if err != nil {
			otel.Handle(err)
		}
		raftTerm, err = meter.Int64ObservableGauge("predastore.meta.raft.term",
			metric.WithDescription("Current raft term. A term climbing while no leader is observed is an election storm."),
			metric.WithUnit("{term}"))
		if err != nil {
			otel.Handle(err)
		}
		raftCommitIndex, err = meter.Int64ObservableGauge("predastore.meta.raft.commit_index",
			metric.WithDescription("Last raft log index committed by this replica."),
			metric.WithUnit("{index}"))
		if err != nil {
			otel.Handle(err)
		}
		raftAppliedIndex, err = meter.Int64ObservableGauge("predastore.meta.raft.applied_index",
			metric.WithDescription("Last raft log index applied to this replica's FSM."),
			metric.WithUnit("{index}"))
		if err != nil {
			otel.Handle(err)
		}
		raftAppliedLag, err = meter.Int64ObservableGauge("predastore.meta.raft.applied_lag",
			metric.WithDescription("Committed minus applied index. A lag that does not drain is a stalled FSM."),
			metric.WithUnit("{index}"))
		if err != nil {
			otel.Handle(err)
		}
		raftLeaderKnown, err = meter.Int64ObservableGauge("predastore.meta.raft.leader_known",
			metric.WithDescription("1 when this replica observes a leader, 0 when it does not. Zero on every replica beyond an election timeout is a livelock."),
			metric.WithUnit("{replica}"))
		if err != nil {
			otel.Handle(err)
		}

		// No metric name may also be another's prefix: Elasticsearch would
		// have to map the same field as both a leaf and an object, and the
		// second one to arrive is rejected. So "part" is a namespace only
		// (count/bytes/fetches siblings), and the open-session gauge is a
		// sibling of "uploads" rather than a child of it.
		multipartUploads, err = meter.Int64Counter("predastore.multipart.uploads",
			metric.WithDescription("Multipart uploads by outcome: created, completed, aborted or rejected."),
			metric.WithUnit("{upload}"))
		if err != nil {
			otel.Handle(err)
		}
		multipartSessions, err = meter.Int64UpDownCounter("predastore.multipart.sessions",
			metric.WithDescription("Multipart uploads created but not yet completed or aborted. A floor that rises across runs is leaked upload state."),
			metric.WithUnit("{upload}"))
		if err != nil {
			otel.Handle(err)
		}
		multipartPartCount, err = meter.Int64Counter("predastore.multipart.part.count",
			metric.WithDescription("Parts stored for multipart uploads."),
			metric.WithUnit("{part}"))
		if err != nil {
			otel.Handle(err)
		}
		multipartPartBytes, err = meter.Int64Counter("predastore.multipart.part.bytes",
			metric.WithDescription("Bytes stored as multipart upload parts."),
			metric.WithUnit("By"))
		if err != nil {
			otel.Handle(err)
		}
		multipartPartFetches, err = meter.Int64Counter("predastore.multipart.part.fetches",
			metric.WithDescription("Part read-backs during completion, by outcome and failure reason."),
			metric.WithUnit("{fetch}"))
		if err != nil {
			otel.Handle(err)
		}

		shardErrors, err = meter.Int64Counter("predastore.shard.errors",
			metric.WithDescription("Shard operations that failed, by op and reason. Reads are tolerated by parity, so a rate here separates routine reconstruction from a node losing data."),
			metric.WithUnit("{error}"))
		if err != nil {
			otel.Handle(err)
		}
		shardHedges, err = meter.Int64Counter("predastore.shard.hedges",
			metric.WithDescription("Shard reads given up on in favour of parity, by reason. Without the reason there is no telling a cluster hedging usefully from one hedging on every request."),
			metric.WithUnit("{hedge}"))
		if err != nil {
			otel.Handle(err)
		}
		shardTTFB, err = meter.Int64Histogram("predastore.shard.ttfb",
			metric.WithDescription("Time from opening a shard stream to its first byte. A node that accepts a connection and then says nothing shows up here and nowhere else."),
			metric.WithUnit("ms"))
		if err != nil {
			otel.Handle(err)
		}
		shardBytes, err = meter.Int64Counter("predastore.shard.read.bytes",
			metric.WithDescription("Bytes delivered by shard reads, by node."),
			metric.WithUnit("By"))
		if err != nil {
			otel.Handle(err)
		}
		shardStalls, err = meter.Int64Counter("predastore.shard.stalls",
			metric.WithDescription("Gaps in a shard stream long enough to count as a stall. A shard that keeps delivering is never hedged, so this is what separates slow from stopped."),
			metric.WithUnit("{stall}"))
		if err != nil {
			otel.Handle(err)
		}
		shardLongest, err = meter.Int64Histogram("predastore.shard.stall.longest",
			metric.WithDescription("The longest gap in a single shard stream."),
			metric.WithUnit("ms"))
		if err != nil {
			otel.Handle(err)
		}
		shardRate, err = meter.Int64Histogram("predastore.shard.throughput",
			metric.WithDescription("Delivered rate of a shard read over the time it spent reading. The floor a slow shard is judged against, in place of an absolute duration that is a throughput floor in disguise."),
			metric.WithUnit("KiBy/s"))
		if err != nil {
			otel.Handle(err)
		}
	})
}

// RaftSnapshot is one meta replica's consensus state at collection time.
//
// Term, CommitIndex and AppliedIndex are the raw raft.Stats() strings, because
// that is how raft renders them. Parsing happens here so an unparseable value
// can be skipped rather than reported as a real zero, which is a legitimate
// index.
type RaftSnapshot struct {
	NodeID       string
	State        string
	LeaderKnown  bool
	Term         string
	CommitIndex  string
	AppliedIndex string
}

// RegisterRaftGauges observes one meta replica's consensus state on every
// collection. snapshot is called once per collection and its result feeds
// every gauge, so the indexes are always mutually consistent and applied_lag
// is a real difference rather than two readings subtracted.
//
// The returned function unregisters the callback; the caller invokes it when
// the replica shuts down. Registering more than one replica in a process is
// supported: the instruments are shared and the node attribute separates them.
func RegisterRaftGauges(snapshot func() RaftSnapshot) (func() error, error) {
	instruments()
	if meter == nil {
		return func() error { return nil }, nil
	}

	reg, err := meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			observeRaft(o, snapshot())
			return nil
		},
		raftState, raftTerm, raftCommitIndex, raftAppliedIndex, raftAppliedLag, raftLeaderKnown,
	)
	if err != nil {
		return nil, err
	}
	return reg.Unregister, nil
}

// observeRaft emits one snapshot across the raft gauges.
func observeRaft(o metric.Observer, s RaftSnapshot) {
	node := metric.WithAttributeSet(attribute.NewSet(attribute.String("node", s.NodeID)))

	o.ObserveInt64(raftState, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String("node", s.NodeID),
		attribute.String("raft.state", s.State),
	)))
	o.ObserveInt64(raftLeaderKnown, boolToInt64(s.LeaderKnown), node)

	if term, ok := parseIndex(s.Term); ok {
		o.ObserveInt64(raftTerm, term, node)
	}
	commit, commitOK := parseIndex(s.CommitIndex)
	if commitOK {
		o.ObserveInt64(raftCommitIndex, commit, node)
	}
	applied, appliedOK := parseIndex(s.AppliedIndex)
	if appliedOK {
		o.ObserveInt64(raftAppliedIndex, applied, node)
	}
	if commitOK && appliedOK {
		o.ObserveInt64(raftAppliedLag, max(commit-applied, 0), node)
	}
}

// parseIndex reads one raft.Stats() numeric field. ok is false when the value
// is absent or not a number, which the caller reports as no observation at all
// rather than as zero.
func parseIndex(v string) (int64, bool) {
	if v == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// Multipart upload outcomes. rejected is a completion refused because the
// parts named do not match what was stored — the client-visible 400, which is
// otherwise indistinguishable from a client that never completed at all.
const (
	UploadCreated   = "created"
	UploadCompleted = "completed"
	UploadAborted   = "aborted"
	UploadRejected  = "rejected"
)

// RecordMultipartUpload counts one upload reaching outcome, and keeps the
// open-session count in step: created opens one, completed and aborted each
// close one. rejected leaves the session open, because it is still there.
func RecordMultipartUpload(ctx context.Context, outcome string) {
	instruments()
	opt := metric.WithAttributeSet(attribute.NewSet(attribute.String("outcome", outcome)))
	if multipartUploads != nil {
		multipartUploads.Add(ctx, 1, opt)
	}
	if multipartSessions == nil {
		return
	}
	switch outcome {
	case UploadCreated:
		multipartSessions.Add(ctx, 1)
	case UploadCompleted, UploadAborted:
		multipartSessions.Add(ctx, -1)
	}
}

// RecordMultipartPart counts one part stored, and the bytes it carried.
func RecordMultipartPart(ctx context.Context, size int64) {
	instruments()
	if multipartPartCount != nil {
		multipartPartCount.Add(ctx, 1)
	}
	if multipartPartBytes != nil && size > 0 {
		multipartPartBytes.Add(ctx, size)
	}
}

// Part read-back failure reasons, recorded when an upload is completed.
const (
	// FetchReasonMetaMissing is a part whose shard placement is not in the
	// meta store: the part index is gone, not the data.
	FetchReasonMetaMissing = "meta_missing"
	// FetchReasonPlacementDecode is placement metadata that will not decode.
	FetchReasonPlacementDecode = "placement_decode"
	// FetchReasonShardRead is too few surviving shards to reconstruct the
	// part — the data itself is unreadable.
	FetchReasonShardRead = "shard_read"
)

// RecordMultipartPartFetch counts one part read-back during completion.
// reason is empty on success and one of the FetchReason constants otherwise.
func RecordMultipartPartFetch(ctx context.Context, reason string) {
	instruments()
	if multipartPartFetches == nil {
		return
	}
	attrs := []attribute.KeyValue{attribute.String("outcome", "success")}
	if reason != "" {
		attrs = []attribute.KeyValue{
			attribute.String("outcome", "error"),
			attribute.String("reason", reason),
		}
	}
	multipartPartFetches.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attrs...)))
}

// Shard failure reasons. Bounded on purpose: the underlying error text names
// nodes and keys and would make this counter unbounded.
const (
	// ShardReasonNotFound is a node that answered and does not hold the shard.
	ShardReasonNotFound = "not_found"
	// ShardReasonTransport is any other failure reaching or reading the node.
	ShardReasonTransport = "transport"
	// ShardReasonStaleEpoch is a node holding the shard under a different
	// write epoch than the placement record names: up, answering, and wrong.
	ShardReasonStaleEpoch = "stale_epoch"
)

// RecordShardError counts one failed shard operation. op is "read"; reason is
// one of the ShardReason constants.
func RecordShardError(ctx context.Context, op, reason string) {
	instruments()
	if shardErrors == nil {
		return
	}
	shardErrors.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String("op", op),
		attribute.String("reason", reason),
	)))
}

// Hedge reasons, as a bounded label set. A shard that keeps delivering is
// never hedged, so there is deliberately no reason for "slow".
const (
	// HedgeHardFail is a refused, closed, missing or wrong-epoch shard: it is
	// not coming, so parity starts immediately rather than after a delay.
	HedgeHardFail = "hard_fail"
	// HedgeNoTTFB is a stream that opened and then sent nothing within the
	// budget, while a peer had already delivered.
	HedgeNoTTFB = "no_ttfb"
	// HedgeStall is a stream that was delivering and then stopped.
	HedgeStall = "stall"
)

// ShardRead is what one shard delivered over the course of one object read.
type ShardRead struct {
	Node string
	// TTFB is zero when the shard never delivered a byte.
	TTFB    time.Duration
	Bytes   int64
	Active  time.Duration
	Stalls  int64
	Longest time.Duration
}

// RecordShardHedge counts one shard given up on, by reason.
func RecordShardHedge(ctx context.Context, reason string) {
	instruments()
	if shardHedges == nil {
		return
	}
	shardHedges.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String("reason", reason),
	)))
}

// RecordShardRead records what one shard delivered. Rate is reported over the
// time spent reading rather than wall time, so a shard waiting its turn behind
// a stripe is not counted as slow.
func RecordShardRead(ctx context.Context, r ShardRead) {
	instruments()
	if shardBytes == nil {
		return
	}
	node := metric.WithAttributeSet(attribute.NewSet(attribute.String("node", r.Node)))
	shardBytes.Add(ctx, r.Bytes, node)
	if r.TTFB > 0 {
		shardTTFB.Record(ctx, r.TTFB.Milliseconds(), node)
	}
	if r.Stalls > 0 {
		shardStalls.Add(ctx, r.Stalls, node)
		shardLongest.Record(ctx, r.Longest.Milliseconds(), node)
	}
	if r.Active > 0 && r.Bytes > 0 {
		shardRate.Record(ctx, r.Bytes*int64(time.Second)/int64(r.Active)/1024, node)
	}
}
