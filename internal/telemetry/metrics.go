// Package telemetry holds predastore's own OpenTelemetry instruments: meta
// raft consensus state, multipart upload counters, shard and object outcomes,
// and the gate's in-flight memory. The OTel bootstrap itself lives in
// bluebottle/pkg/otelsetup, which cmd/s3d calls directly, and per-request HTTP
// metrics come from that package's middleware.
//
// Instruments recorded per shard read their attribute set from a pre-built
// cache rather than building one per call, because attribute.NewSet sorts and
// deduplicates and so allocates. Anything hotter than per-shard does not
// belong here at all: it belongs in an atomic counter on the owning struct,
// published through an observable callback.
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

// Metric names, declared together so the prefix invariant is checkable in one
// place. No name may be a prefix of another: Elasticsearch would have to map
// the same field as both a leaf and an object, and the second one to arrive is
// rejected. So "part" and "inflight" are namespaces only, and the open-session
// gauge is a sibling of "uploads" rather than a child of it.
const (
	metricRaftState        = "predastore.meta.raft.state"
	metricRaftTerm         = "predastore.meta.raft.term"
	metricRaftCommitIndex  = "predastore.meta.raft.commit_index"
	metricRaftAppliedIndex = "predastore.meta.raft.applied_index"
	metricRaftAppliedLag   = "predastore.meta.raft.applied_lag"
	metricRaftLeaderKnown  = "predastore.meta.raft.leader_known"

	metricMultipartUploads     = "predastore.multipart.uploads"
	metricMultipartSessions    = "predastore.multipart.sessions"
	metricMultipartPartCount   = "predastore.multipart.part.count"
	metricMultipartPartBytes   = "predastore.multipart.part.bytes"
	metricMultipartPartFetches = "predastore.multipart.part.fetches"

	metricShardErrors   = "predastore.shard.errors"
	metricShardOps      = "predastore.shard.ops"
	metricShardDuration = "predastore.shard.duration"
	metricShardHedges   = "predastore.shard.hedges"
	metricShardTTFB     = "predastore.shard.ttfb"
	metricShardBytes    = "predastore.shard.read.bytes"
	metricShardStalls   = "predastore.shard.stalls"
	metricShardLongest  = "predastore.shard.stall.longest"
	metricShardRate     = "predastore.shard.throughput"

	metricObjectReads  = "predastore.object.reads"
	metricObjectWrites = "predastore.object.writes"

	metricGateInflightBytes    = "predastore.gate.inflight.bytes"
	metricGateInflightRequests = "predastore.gate.inflight.requests"

	metricBlobFreeFrac   = "predastore.blob.free_frac"
	metricBlobFreeBytes  = "predastore.blob.free_bytes"
	metricBlobTotalBytes = "predastore.blob.total_bytes"
	metricBlobPressure   = "predastore.blob.pressure"
	metricBlobSegments   = "predastore.blob.segments"
	metricBlobSegNum     = "predastore.blob.seg_num"
	metricBlobValueNum   = "predastore.blob.value_num"
	metricBlobFragNum    = "predastore.blob.frag_num"
	metricBlobLiveBytes  = "predastore.blob.live_bytes"
	metricBlobDeadBytes  = "predastore.blob.dead_bytes"
	metricBlobLiveFrac   = "predastore.blob.live_frac"

	metricBlobCompactionCycles       = "predastore.blob.compaction.cycles"
	metricBlobCompactionSegments     = "predastore.blob.compaction.segments"
	metricBlobCompactionBytes        = "predastore.blob.compaction.bytes"
	metricBlobCompactionLastDuration = "predastore.blob.compaction.last_duration_seconds"

	metricBlobIntegrityFailures = "predastore.blob.integrity.failures"

	metricObjectPhaseDuration = "predastore.object.phase.duration"

	metricMetaClientOps       = "predastore.meta.client.ops"
	metricMetaClientDuration  = "predastore.meta.client.duration"
	metricMetaClientRedirects = "predastore.meta.client.redirects"

	metricMetaFSMLSMBytes   = "predastore.meta.fsm.lsm_bytes"
	metricMetaFSMVLogBytes  = "predastore.meta.fsm.vlog_bytes"
	metricMetaSnapshotIndex = "predastore.meta.snapshot.index"
	metricMetaLogTrailing   = "predastore.meta.log.trailing"

	metricRPCConnections   = "predastore.rpc.connections"
	metricRPCEvictions     = "predastore.rpc.evictions"
	metricRPCStreamsOpen   = "predastore.rpc.streams.open"
	metricRPCStreamOpenDur = "predastore.rpc.stream.open.duration"

	metricRepairPending  = "predastore.repair.pending"
	metricRepairRepaired = "predastore.repair.repaired"
	metricRepairFailed   = "predastore.repair.failed"
	metricRepairPasses   = "predastore.repair.passes"
)

// metricNames is every name this package registers. Only the tests read it:
// keeping it beside the constants is what makes a newly added instrument fail
// the coverage check rather than quietly escape the prefix invariant.
var metricNames = []string{
	metricRaftState, metricRaftTerm, metricRaftCommitIndex,
	metricRaftAppliedIndex, metricRaftAppliedLag, metricRaftLeaderKnown,
	metricMultipartUploads, metricMultipartSessions, metricMultipartPartCount,
	metricMultipartPartBytes, metricMultipartPartFetches,
	metricShardErrors, metricShardOps, metricShardDuration,
	metricShardHedges, metricShardTTFB, metricShardBytes,
	metricShardStalls, metricShardLongest, metricShardRate,
	metricObjectReads, metricObjectWrites,
	metricGateInflightBytes, metricGateInflightRequests,
	metricBlobFreeFrac, metricBlobFreeBytes, metricBlobTotalBytes, metricBlobPressure,
	metricBlobSegments, metricBlobSegNum, metricBlobValueNum, metricBlobFragNum,
	metricBlobLiveBytes, metricBlobDeadBytes, metricBlobLiveFrac,
	metricBlobCompactionCycles, metricBlobCompactionSegments, metricBlobCompactionBytes,
	metricBlobCompactionLastDuration, metricBlobIntegrityFailures,
	metricObjectPhaseDuration,
	metricMetaClientOps, metricMetaClientDuration, metricMetaClientRedirects,
	metricMetaFSMLSMBytes, metricMetaFSMVLogBytes, metricMetaSnapshotIndex, metricMetaLogTrailing,
	metricRPCConnections, metricRPCEvictions, metricRPCStreamsOpen, metricRPCStreamOpenDur,
	metricRepairPending, metricRepairRepaired, metricRepairFailed, metricRepairPasses,
}

// secondsBuckets bounds the duration histograms. The SDK default boundaries
// run 0..10000 and only make sense for milliseconds: every second-valued
// sample lands in the first bucket and every percentile reads back as 2.5.
var secondsBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

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

	shardOps      metric.Int64Counter
	shardDuration metric.Float64Histogram

	objectReads  metric.Int64Counter
	objectWrites metric.Int64Counter

	gateInflightBytes    metric.Int64UpDownCounter
	gateInflightRequests metric.Int64UpDownCounter

	blobFreeFrac   metric.Float64ObservableGauge
	blobFreeBytes  metric.Int64ObservableGauge
	blobTotalBytes metric.Int64ObservableGauge
	blobPressure   metric.Int64ObservableGauge
	blobSegments   metric.Int64ObservableGauge
	blobSegNum     metric.Int64ObservableGauge
	blobValueNum   metric.Int64ObservableGauge
	blobFragNum    metric.Int64ObservableGauge
	blobLiveBytes  metric.Int64ObservableGauge
	blobDeadBytes  metric.Int64ObservableGauge
	blobLiveFrac   metric.Float64ObservableGauge

	blobCompactionCycles       metric.Int64ObservableCounter
	blobCompactionSegments     metric.Int64ObservableCounter
	blobCompactionBytes        metric.Int64ObservableCounter
	blobCompactionLastDuration metric.Float64ObservableGauge

	blobIntegrityFailures metric.Int64ObservableCounter

	objectPhaseDuration metric.Float64Histogram

	metaClientOps       metric.Int64Counter
	metaClientDuration  metric.Float64Histogram
	metaClientRedirects metric.Int64Counter

	metaFSMLSMBytes   metric.Int64ObservableGauge
	metaFSMVLogBytes  metric.Int64ObservableGauge
	metaSnapshotIndex metric.Int64ObservableGauge
	metaLogTrailing   metric.Int64ObservableGauge

	rpcConnections   metric.Int64ObservableGauge
	rpcEvictions     metric.Int64Counter
	rpcStreamsOpen   metric.Int64UpDownCounter
	rpcStreamOpenDur metric.Float64Histogram

	repairPending  metric.Int64ObservableGauge
	repairRepaired metric.Int64ObservableGauge
	repairFailed   metric.Int64ObservableGauge
	repairPasses   metric.Int64ObservableGauge
)

// instruments lazily creates the shared instruments. The global meter
// delegates to the real provider once Init installs one; before that (or when
// export is disabled) every recorded call is a cheap no-op.
func instruments() {
	instrumentsOnce.Do(func() {
		meter = otel.Meter(meterName)

		raftState = int64Gauge(metricRaftState,
			"Constant 1 carrying the replica's own raft.State() as an attribute.", "{replica}")
		raftTerm = int64Gauge(metricRaftTerm,
			"Current raft term. A term climbing while no leader is observed is an election storm.", "{term}")
		raftCommitIndex = int64Gauge(metricRaftCommitIndex,
			"Last raft log index committed by this replica.", "{index}")
		raftAppliedIndex = int64Gauge(metricRaftAppliedIndex,
			"Last raft log index applied to this replica's FSM.", "{index}")
		raftAppliedLag = int64Gauge(metricRaftAppliedLag,
			"Committed minus applied index. A lag that does not drain is a stalled FSM.", "{index}")
		raftLeaderKnown = int64Gauge(metricRaftLeaderKnown,
			"1 when this replica observes a leader, 0 when it does not. Zero on every replica beyond an election timeout is a livelock.", "{replica}")

		multipartUploads = int64Counter(metricMultipartUploads,
			"Multipart uploads by outcome: created, completed, aborted or rejected.", "{upload}")
		multipartSessions = int64UpDownCounter(metricMultipartSessions,
			"Multipart uploads created but not yet completed or aborted. A floor that rises across runs is leaked upload state.", "{upload}")
		multipartPartCount = int64Counter(metricMultipartPartCount,
			"Parts stored for multipart uploads.", "{part}")
		multipartPartBytes = int64Counter(metricMultipartPartBytes,
			"Bytes stored as multipart upload parts.", "By")
		multipartPartFetches = int64Counter(metricMultipartPartFetches,
			"Part read-backs during completion, by outcome and failure reason.", "{fetch}")

		shardErrors = int64Counter(metricShardErrors,
			"Shard operations that failed, by op, node and reason. Reads are tolerated by parity, so a rate here separates routine reconstruction from a node losing data.", "{error}")
		shardOps = int64Counter(metricShardOps,
			"Shard operations attempted, by op, node and outcome. The denominator the error counter is read against.", "{operation}")
		shardDuration = secondsHistogram(metricShardDuration,
			"Duration of one shard operation against one node. A per-node tail is what separates a single sick node from a slow cluster.")

		shardHedges = int64Counter(metricShardHedges,
			"Shard reads given up on in favour of parity, by reason. Without the reason there is no telling a cluster hedging usefully from one hedging on every request.", "{hedge}")
		shardTTFB = int64Histogram(metricShardTTFB,
			"Time from opening a shard stream to its first byte. A node that accepts a connection and then says nothing shows up here and nowhere else.", "ms")
		shardBytes = int64Counter(metricShardBytes,
			"Bytes delivered by shard reads, by node.", "By")
		shardStalls = int64Counter(metricShardStalls,
			"Gaps in a shard stream long enough to count as a stall. A shard that keeps delivering is never hedged, so this is what separates slow from stopped.", "{stall}")
		shardLongest = int64Histogram(metricShardLongest,
			"The longest gap in a single shard stream.", "ms")
		shardRate = int64Histogram(metricShardRate,
			"Delivered rate of a shard read over the time it spent reading. The floor a slow shard is judged against, in place of an absolute duration that is a throughput floor in disguise.", "KiBy/s")

		objectReads = int64Counter(metricObjectReads,
			"Object reads served, by path. A reconstructed read consumed parity to answer, and is the only in-band evidence that a blob node is losing data.", "{read}")
		objectWrites = int64Counter(metricObjectWrites,
			"Object writes by outcome and failure reason.", "{write}")

		gateInflightBytes = int64UpDownCounter(metricGateInflightBytes,
			"Object payload bytes currently resident in gate memory. Declared object size rather than a heap measurement: concurrency multiplying object size is what exhausts a gate.", "By")
		gateInflightRequests = int64UpDownCounter(metricGateInflightRequests,
			"Object requests currently holding a payload buffer in gate memory.", "{request}")

		blobFreeFrac = float64Gauge(metricBlobFreeFrac,
			"Free fraction of the filesystem backing this node's store, as of the last measurement the write path took.", "1")
		blobFreeBytes = int64Gauge(metricBlobFreeBytes,
			"Bytes available to the store on its filesystem. A fraction cannot say how long the space lasts; this can.", "By")
		blobTotalBytes = int64Gauge(metricBlobTotalBytes,
			"Size of the filesystem backing this node's store.", "By")
		blobPressure = int64Gauge(metricBlobPressure,
			"Constant 1 carrying the watermark band the store is in as an attribute: ok, nearfull or full.", "{node}")
		blobSegments = int64Gauge(metricBlobSegments,
			"Segment files currently on disk. Climbing while bytes do not is compaction failing to keep up.", "{segment}")
		blobSegNum = int64Gauge(metricBlobSegNum,
			"Monotonic segment counter. Its rate is how fast the store rolls segments.", "{segment}")
		blobValueNum = int64Gauge(metricBlobValueNum,
			"Monotonic value counter. Its rate is the node's write rate, counted with nothing added to the write path.", "{value}")
		blobFragNum = int64Gauge(metricBlobFragNum,
			"Monotonic fragment counter. Its rate against value_num is the average value size in fragments.", "{fragment}")
		blobLiveBytes = int64Gauge(metricBlobLiveBytes,
			"On-disk bytes holding live data, as of the last compaction scan.", "By")
		blobDeadBytes = int64Gauge(metricBlobDeadBytes,
			"On-disk bytes superseded or deleted and not yet reclaimed, as of the last compaction scan.", "By")
		blobLiveFrac = float64Gauge(metricBlobLiveFrac,
			"Live share of on-disk bytes. A falling value with a flat reclaim rate is space compaction is not getting back.", "1")

		blobCompactionCycles = int64ObservableCounter(metricBlobCompactionCycles,
			"Compaction cycles run, by outcome. No cycles at all on a busy node is a compactor that is not running.", "{cycle}")
		blobCompactionSegments = int64ObservableCounter(metricBlobCompactionSegments,
			"Segments compaction has scanned and dropped.", "{segment}")
		blobCompactionBytes = int64ObservableCounter(metricBlobCompactionBytes,
			"Bytes compaction has relocated and reclaimed. Reclaimed is the only measure of space actually returned.", "By")
		blobCompactionLastDuration = float64Gauge(metricBlobCompactionLastDuration,
			"Duration of the last completed compaction cycle. Approaching the cycle interval means compaction never rests.", "s")

		blobIntegrityFailures = int64ObservableCounter(metricBlobIntegrityFailures,
			"Fragments that failed their GCM tag. Any non-zero value is corruption: bytes on disk no longer authenticate.", "{fragment}")

		objectPhaseDuration = secondsHistogram(metricObjectPhaseDuration,
			"Time one phase of an object request took, by phase and op. Where a slow PUT or GET actually goes.")

		metaClientOps = int64Counter(metricMetaClientOps,
			"Meta store operations attempted by this gate, by op and outcome.", "{operation}")
		metaClientDuration = secondsHistogram(metricMetaClientDuration,
			"Time one meta operation took, including every replica it had to try. Two of these sit on every PUT.")
		metaClientRedirects = int64Counter(metricMetaClientRedirects,
			"Write attempts that did not land on a leader, by reason. A sustained rate is an election that is not settling.", "{redirect}")

		metaFSMLSMBytes = int64Gauge(metricMetaFSMLSMBytes,
			"On-disk size of this replica's LSM tree, where badger keeps metadata values under its default threshold. The single raft group's ceiling, visible before it is reached.", "By")
		metaFSMVLogBytes = int64Gauge(metricMetaFSMVLogBytes,
			"On-disk size of this replica's badger value log file. Badger preallocates this file, so it steps between fixed sizes rather than growing with the state machine.", "By")
		metaSnapshotIndex = int64Gauge(metricMetaSnapshotIndex,
			"Raft log index of this replica's last snapshot.", "{index}")
		metaLogTrailing = int64Gauge(metricMetaLogTrailing,
			"Log entries written since the last snapshot. A number that only climbs is snapshotting that has stopped.", "{entry}")

		rpcConnections = int64Gauge(metricRPCConnections,
			"Connections this node's pool currently holds, one per peer it is in contact with.", "{connection}")
		rpcEvictions = int64Counter(metricRPCEvictions,
			"Connections dropped from the pool, by peer and reason. A peer evicted repeatedly is the one at fault.", "{eviction}")
		rpcStreamsOpen = int64UpDownCounter(metricRPCStreamsOpen,
			"Streams this node has open to a peer. Bounded by the transport's per-connection cap, which is otherwise invisible.", "{stream}")
		rpcStreamOpenDur = secondsHistogram(metricRPCStreamOpenDur,
			"Time spent acquiring a stream to a peer, before a byte is written. QUIC blocks this wait when the peer's stream credit is spent, so it is the only thing that separates a slow peer from a connection that has run out of streams to talk over.")

		repairPending = int64Gauge(metricRepairPending,
			"Shard positions the last completed sweep could not rebuild. Above zero means stripes are short a holder right now.", "{shard}")
		repairRepaired = int64Gauge(metricRepairRepaired,
			"Shards rebuilt since start. Flat while pending is high means the sweep cannot place them, not that there is nothing to do.", "{shard}")
		repairFailed = int64Gauge(metricRepairFailed,
			"Rebuild attempts that failed since start.", "{shard}")
		repairPasses = int64Gauge(metricRepairPasses,
			"Sweeps completed since start. Not climbing means the sweep is stalled or was never started.", "{pass}")
	})
}

// Every instrument is built through one of the helpers below, so a construction
// failure is reported the same way wherever it happens. The meter is set by
// instruments() before any of them is called.
func int64Gauge(name, description, unit string) metric.Int64ObservableGauge {
	g, err := meter.Int64ObservableGauge(name,
		metric.WithDescription(description), metric.WithUnit(unit))
	if err != nil {
		otel.Handle(err)
	}
	return g
}

func float64Gauge(name, description, unit string) metric.Float64ObservableGauge {
	g, err := meter.Float64ObservableGauge(name,
		metric.WithDescription(description), metric.WithUnit(unit))
	if err != nil {
		otel.Handle(err)
	}
	return g
}

func int64ObservableCounter(name, description, unit string) metric.Int64ObservableCounter {
	c, err := meter.Int64ObservableCounter(name,
		metric.WithDescription(description), metric.WithUnit(unit))
	if err != nil {
		otel.Handle(err)
	}
	return c
}

func int64Counter(name, description, unit string) metric.Int64Counter {
	c, err := meter.Int64Counter(name,
		metric.WithDescription(description), metric.WithUnit(unit))
	if err != nil {
		otel.Handle(err)
	}
	return c
}

func int64Histogram(name, description, unit string) metric.Int64Histogram {
	h, err := meter.Int64Histogram(name,
		metric.WithDescription(description), metric.WithUnit(unit))
	if err != nil {
		otel.Handle(err)
	}
	return h
}

func int64UpDownCounter(name, description, unit string) metric.Int64UpDownCounter {
	c, err := meter.Int64UpDownCounter(name,
		metric.WithDescription(description), metric.WithUnit(unit))
	if err != nil {
		otel.Handle(err)
	}
	return c
}

// secondsHistogram fixes the unit and the bucket boundaries together, so a new
// duration histogram cannot be declared in seconds and left on the SDK's
// millisecond-scale defaults.
func secondsHistogram(name, description string) metric.Float64Histogram {
	h, err := meter.Float64Histogram(name,
		metric.WithDescription(description), metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(secondsBuckets...))
	if err != nil {
		otel.Handle(err)
	}
	return h
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

// StoreSnapshot is one blob node's store at collection time. It mirrors what
// the engine maintains rather than anything it computes on demand: the whole
// point of the shape is that answering costs a mutex and no I/O.
//
// Measured and Scanned distinguish "not yet known" from a real zero. A store
// that has never been written to has taken no free-space measurement, and one
// running without compaction has never scanned for dead bytes; reporting either
// as zero would read as a full disk or a perfectly clean store.
type StoreSnapshot struct {
	NodeID string

	Measured   bool
	FreeFrac   float64
	FreeBytes  uint64
	TotalBytes uint64
	Pressure   string

	SegNum       uint64
	ValueNum     uint64
	FragNum      uint64
	LiveSegments int64

	Scanned   bool
	LiveBytes int64
	DeadBytes int64

	CompactionCycles       int64
	CompactionCyclesFailed int64
	SegmentsScanned        int64
	SegmentsDropped        int64
	BytesRelocated         int64
	BytesReclaimed         int64
	LastCycleSeconds       float64

	IntegrityFailures uint64
}

// RegisterStoreGauges observes one blob node's store on every collection,
// mirroring RegisterRaftGauges: snapshot is called once per collection and its
// result feeds every instrument, so the figures are mutually consistent.
//
// The returned function unregisters the callback; the caller invokes it when
// the node shuts down. Several nodes in one process are supported — the
// instruments are shared and the node attribute separates them.
func RegisterStoreGauges(snapshot func() StoreSnapshot) (func() error, error) {
	instruments()
	if meter == nil {
		return func() error { return nil }, nil
	}

	reg, err := meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			observeStore(o, snapshot())
			return nil
		},
		blobFreeFrac, blobFreeBytes, blobTotalBytes, blobPressure,
		blobSegments, blobSegNum, blobValueNum, blobFragNum,
		blobLiveBytes, blobDeadBytes, blobLiveFrac,
		blobCompactionCycles, blobCompactionSegments, blobCompactionBytes,
		blobCompactionLastDuration, blobIntegrityFailures,
	)
	if err != nil {
		return nil, err
	}
	return reg.Unregister, nil
}

// observeStore emits one snapshot across the blob store instruments.
func observeStore(o metric.Observer, s StoreSnapshot) {
	node := metric.WithAttributeSet(attribute.NewSet(attribute.String(nodeAttrKey, s.NodeID)))

	o.ObserveInt64(blobSegNum, int64(s.SegNum), node)     //nolint:gosec // monotonic counters; a store would have to write 9.2e18 values to wrap.
	o.ObserveInt64(blobValueNum, int64(s.ValueNum), node) //nolint:gosec // as above.
	o.ObserveInt64(blobFragNum, int64(s.FragNum), node)   //nolint:gosec // as above.
	o.ObserveInt64(blobSegments, s.LiveSegments, node)

	if s.Measured {
		o.ObserveFloat64(blobFreeFrac, s.FreeFrac, node)
		o.ObserveInt64(blobFreeBytes, int64(s.FreeBytes), node)   //nolint:gosec // filesystem sizes are far below the int64 ceiling.
		o.ObserveInt64(blobTotalBytes, int64(s.TotalBytes), node) //nolint:gosec // as above.
		o.ObserveInt64(blobPressure, 1, metric.WithAttributeSet(attribute.NewSet(
			attribute.String(nodeAttrKey, s.NodeID),
			attribute.String("blob.pressure", s.Pressure),
		)))
	}

	if s.Scanned {
		o.ObserveInt64(blobLiveBytes, s.LiveBytes, node)
		o.ObserveInt64(blobDeadBytes, s.DeadBytes, node)
		if total := s.LiveBytes + s.DeadBytes; total > 0 {
			o.ObserveFloat64(blobLiveFrac, float64(s.LiveBytes)/float64(total), node)
		}
	}

	o.ObserveInt64(blobCompactionCycles, s.CompactionCycles-s.CompactionCyclesFailed,
		outcomeAttrs(s.NodeID, OutcomeSuccess))
	o.ObserveInt64(blobCompactionCycles, s.CompactionCyclesFailed,
		outcomeAttrs(s.NodeID, OutcomeError))

	o.ObserveInt64(blobCompactionSegments, s.SegmentsScanned, kindAttrs(s.NodeID, CompactionKindScanned))
	o.ObserveInt64(blobCompactionSegments, s.SegmentsDropped, kindAttrs(s.NodeID, CompactionKindDropped))
	o.ObserveInt64(blobCompactionBytes, s.BytesRelocated, kindAttrs(s.NodeID, CompactionKindRelocated))
	o.ObserveInt64(blobCompactionBytes, s.BytesReclaimed, kindAttrs(s.NodeID, CompactionKindReclaimed))
	o.ObserveFloat64(blobCompactionLastDuration, s.LastCycleSeconds, node)

	o.ObserveInt64(blobIntegrityFailures, int64(s.IntegrityFailures), node) //nolint:gosec // a count of corrupt fragments cannot realistically reach the int64 ceiling.
}

// Compaction accounting kinds. Scanned counts candidates a cycle selected and
// dropped counts those it actually drained, so the gap between them is segments
// compaction gave up on. Reclaimed is space returned to the filesystem;
// relocated is live data moved to get at it.
const (
	CompactionKindScanned   = "scanned"
	CompactionKindDropped   = "dropped"
	CompactionKindRelocated = "relocated"
	CompactionKindReclaimed = "reclaimed"
)

// outcomeAttrs and kindAttrs build the per-collection attribute sets for the
// compaction counters. Built per observation rather than cached: a callback
// runs once per collection interval, where an allocation is free.
func outcomeAttrs(node, outcome string) metric.ObserveOption {
	return metric.WithAttributeSet(attribute.NewSet(
		attribute.String(nodeAttrKey, node),
		attribute.String("outcome", outcome),
	))
}

func kindAttrs(node, kind string) metric.ObserveOption {
	return metric.WithAttributeSet(attribute.NewSet(
		attribute.String(nodeAttrKey, node),
		attribute.String("kind", kind),
	))
}

// observeRaft emits one snapshot across the raft gauges.
func observeRaft(o metric.Observer, s RaftSnapshot) {
	node := metric.WithAttributeSet(attribute.NewSet(attribute.String(nodeAttrKey, s.NodeID)))

	o.ObserveInt64(raftState, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String(nodeAttrKey, s.NodeID),
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

// nodeAttrKey names the cluster member an observation belongs to. The value is
// always a string, matching the raft gauges: one field cannot be a keyword in
// one metric and a number in another without the sink rejecting the second.
const nodeAttrKey = "node"

// Shard operations. Bounded, and the only values the op attribute may take.
const (
	ShardOpRead   = "read"
	ShardOpWrite  = "write"
	ShardOpDelete = "delete"
)

// Operation outcomes.
const (
	OutcomeSuccess = "success"
	OutcomeError   = "error"
)

// Shard failure reasons. Bounded on purpose: the underlying error text names
// nodes and keys and would make this counter unbounded.
const (
	// ShardReasonNotFound is a node that answered and does not hold the shard.
	ShardReasonNotFound = "not_found"
	// ShardReasonStoreFull is a node that refused a write for capacity.
	ShardReasonStoreFull = "store_full"
	// ShardReasonTransport is any other failure reaching or reading the node.
	ShardReasonTransport = "transport"
	// ShardReasonStaleEpoch is a node holding the shard under a different
	// write epoch than the placement record names: up, answering, and wrong.
	ShardReasonStaleEpoch = "stale_epoch"
)

// Object read paths. A reconstructed read consumed parity to answer it, so the
// ratio of the two is the cluster's degraded-read rate.
const (
	ReadPathDirect        = "direct"
	ReadPathReconstructed = "reconstructed"
)

// Object write outcomes. Degraded is reserved for a write that succeeded with
// fewer than every shard placed, which the write path cannot yet do; naming it
// now means the dashboard does not change when it can.
const (
	WriteOutcomeSuccess  = "success"
	WriteOutcomeDegraded = "degraded"
	WriteOutcomeFailed   = "failed"
)

// Object write failure reasons.
const (
	// WriteReasonStoreFull is a node that refused a shard for capacity.
	WriteReasonStoreFull = "store_full"
	// WriteReasonShardWrite is any other failure placing a shard.
	WriteReasonShardWrite = "shard_write"
	// WriteReasonMeta is shards that landed but whose placement could not be
	// committed. The bytes are on disk and nothing references them.
	WriteReasonMeta = "meta"
)

// Gate operations, naming which side of the API holds a payload buffer.
const (
	GateOpPut = "put"
	GateOpGet = "get"
)

// shardAttrKey identifies one pre-built shard attribute set. Every field is
// bounded — three ops, a handful of outcomes or reasons, and one id per
// cluster member — so the cache is bounded by the cluster rather than by
// traffic. qualifier is the outcome or reason; empty builds the set the
// duration histogram wants, which carries op and node only.
type shardAttrKey struct {
	op        string
	qualifier string
	node      uint64
}

// attrOpts is one pre-built attribute set in the two shapes the instruments
// take it. The slices are cached rather than the bare option because Add and
// Record are variadic: passing a fresh option would allocate the argument
// slice on every call even when the set behind it was free.
type attrOpts struct {
	add    []metric.AddOption
	record []metric.RecordOption
}

// newAttrOpts builds both shapes from one attribute set.
func newAttrOpts(attrs ...attribute.KeyValue) attrOpts {
	opt := metric.WithAttributeSet(attribute.NewSet(attrs...))
	return attrOpts{
		add:    []metric.AddOption{opt},
		record: []metric.RecordOption{opt},
	}
}

var (
	shardAttrMu   sync.RWMutex
	shardOpAttrs  = map[shardAttrKey]attrOpts{}
	shardErrAttrs = map[shardAttrKey]attrOpts{}
)

// cachedShardAttrs returns the pre-built options for one shard observation,
// building them on first use. The read path is a map lookup under a read lock
// and allocates nothing, which is the point: attribute.NewSet sorts and
// deduplicates, so building a set per shard operation would allocate on every
// one of them.
func cachedShardAttrs(cache map[shardAttrKey]attrOpts, qualifierKey, op, qualifier string, node uint64) attrOpts {
	k := shardAttrKey{op: op, qualifier: qualifier, node: node}

	shardAttrMu.RLock()
	opts, ok := cache[k]
	shardAttrMu.RUnlock()
	if ok {
		return opts
	}

	attrs := []attribute.KeyValue{
		attribute.String("op", op),
		attribute.String(nodeAttrKey, strconv.FormatUint(node, 10)),
	}
	if qualifier != "" {
		attrs = append(attrs, attribute.String(qualifierKey, qualifier))
	}
	opts = newAttrOpts(attrs...)

	shardAttrMu.Lock()
	cache[k] = opts
	shardAttrMu.Unlock()
	return opts
}

// RecordShardOp counts one shard operation against one node and records how
// long it took. op is a ShardOp constant, outcome an Outcome constant, and
// seconds the elapsed time. Called once per shard per object, so both
// attribute sets come from the cache rather than being built here.
//
// The duration carries op and node but not outcome: a slow failure is part of
// the node's latency, not a separate series to go looking for.
func RecordShardOp(ctx context.Context, op, outcome string, node uint64, seconds float64) {
	instruments()
	if shardOps != nil {
		shardOps.Add(ctx, 1, cachedShardAttrs(shardOpAttrs, "outcome", op, outcome, node).add...)
	}
	if shardDuration != nil {
		shardDuration.Record(ctx, seconds, cachedShardAttrs(shardOpAttrs, "outcome", op, "", node).record...)
	}
}

// RecordShardError counts one failed shard operation. op is a ShardOp
// constant, reason a ShardReason constant, and node the peer that failed.
func RecordShardError(ctx context.Context, op, reason string, node uint64) {
	instruments()
	if shardErrors == nil {
		return
	}
	shardErrors.Add(ctx, 1, cachedShardAttrs(shardErrAttrs, "reason", op, reason, node).add...)
}

// Object request phases. Bounded, and the only values the phase attribute may
// take. A PUT and a GET share the names where they share the work.
const (
	// PhaseBucketCheck is the meta read that resolves the bucket.
	PhaseBucketCheck = "bucket_check"
	// PhaseShardFanout is splitting or joining the object across every blob
	// node holding a shard of it.
	PhaseShardFanout = "shard_fanout"
	// PhaseMetaPlacement is the placement record: written on PUT, read on GET.
	PhaseMetaPlacement = "meta_placement"
	// PhaseMetaListing is the second write a PUT makes, the listing key that
	// points at the object hash.
	PhaseMetaListing = "meta_listing"
	// PhaseReconstruct is rebuilding an object from parity.
	PhaseReconstruct = "reconstruct"
)

// RecordObjectPhase records how long one phase of an object request took. op is
// a GateOp constant and phase a Phase constant. Called a handful of times per
// request, never per shard, so the attribute set comes from the cache.
func RecordObjectPhase(ctx context.Context, op, phase string, seconds float64) {
	instruments()
	if objectPhaseDuration == nil {
		return
	}
	objectPhaseDuration.Record(ctx, seconds, objectAttrs("op", op, "phase", phase).record...)
}

// Meta client operations, naming the wire op rather than the method: Exists is
// a Get and ListKeys is a Scan, and counting them separately would report two
// operations where one crossed the network.
const (
	MetaOpGet    = "get"
	MetaOpScan   = "scan"
	MetaOpPut    = "put"
	MetaOpDelete = "delete"
	MetaOpStatus = "status"
)

// MetaOutcomeNotFound is a read every replica answered, none of which held the
// key. It is not an error: it is the answer, and counting it as a failure would
// make a HEAD of a missing object look like a broken meta plane.
const MetaOutcomeNotFound = "not_found"

// RecordMetaClientOp counts one meta operation and how long it took, including
// every replica it had to try. Two of these sit on every PUT.
func RecordMetaClientOp(ctx context.Context, op, outcome string, seconds float64) {
	instruments()
	if metaClientOps != nil {
		metaClientOps.Add(ctx, 1, objectAttrs("op", op, "outcome", outcome).add...)
	}
	if metaClientDuration != nil {
		metaClientDuration.Record(ctx, seconds, objectAttrs("op", op, "", "").record...)
	}
}

// Meta write redirect reasons.
const (
	// RedirectNotLeader is a replica that refused the write and named the
	// leader, so the next attempt goes straight there.
	RedirectNotLeader = "not_leader"
	// RedirectNoLeader is a replica that refused and could name no leader,
	// which means an election is still settling.
	RedirectNoLeader = "no_leader"
	// RedirectRetryExhausted is a write that ran out of attempts without ever
	// finding a leader. The write failed.
	RedirectRetryExhausted = "retry_exhausted"
)

// RecordMetaRedirect counts one write attempt that did not land on a leader.
func RecordMetaRedirect(ctx context.Context, reason string) {
	instruments()
	if metaClientRedirects == nil {
		return
	}
	metaClientRedirects.Add(ctx, 1, objectAttrs("reason", reason, "", "").add...)
}

// Connection eviction reasons.
const (
	// EvictionStall is a connection that opened streams and answered nothing
	// on a run of them. It was alive at the transport and useless above it.
	EvictionStall = "stall"
	// EvictionError is a connection that could not carry a stream at all.
	EvictionError = "error"
	// EvictionClosed is a connection dropped by an explicit eviction, which
	// includes a peer that closed it.
	EvictionClosed = "closed"
)

// RecordRPCEviction counts one connection dropped from the pool. node is the
// peer that was on the other end. An eviction is rare — per peer, not per
// request — so this formats its node id rather than reading a cache.
func RecordRPCEviction(ctx context.Context, node uint64, reason string) {
	instruments()
	if rpcEvictions == nil {
		return
	}
	rpcEvictions.Add(ctx, 1, objectAttrs(nodeAttrKey, strconv.FormatUint(node, 10), "reason", reason).add...)
}

// nodeOnlyAttrs caches the attribute set carrying nothing but a node id, for
// the stream gauge: one entry per peer, built once.
var (
	nodeAttrMu    sync.RWMutex
	nodeAttrCache = map[uint64]attrOpts{}
)

func nodeAttrs(node uint64) attrOpts {
	nodeAttrMu.RLock()
	opts, ok := nodeAttrCache[node]
	nodeAttrMu.RUnlock()
	if ok {
		return opts
	}

	opts = newAttrOpts(attribute.String(nodeAttrKey, strconv.FormatUint(node, 10)))

	nodeAttrMu.Lock()
	nodeAttrCache[node] = opts
	nodeAttrMu.Unlock()
	return opts
}

// EnterRPCStream counts one stream opened to a peer and returns the function
// that counts it closed. Returning the release rather than exposing a pair is
// deliberate: an unbalanced gauge is worse than no gauge, and a stream that is
// never released is a leak this is meant to show.
func EnterRPCStream(ctx context.Context, node uint64) func() {
	instruments()
	if rpcStreamsOpen == nil {
		return func() {}
	}
	opts := nodeAttrs(node)
	rpcStreamsOpen.Add(ctx, 1, opts.add...)
	return func() { rpcStreamsOpen.Add(context.WithoutCancel(ctx), -1, opts.add...) }
}

// RecordRPCStreamOpen records how long acquiring a stream to a peer took.
// Separate from the stream's own duration on purpose: this wait happens before
// the request exists, so it is the peer's stream credit and never its service
// time. The open-stream gauge cannot show it -- a caller blocked here has not
// entered the gauge yet.
func RecordRPCStreamOpen(ctx context.Context, node uint64, elapsed time.Duration) {
	instruments()
	if rpcStreamOpenDur == nil {
		return
	}
	rpcStreamOpenDur.Record(ctx, elapsed.Seconds(), nodeAttrs(node).record...)
}

// MetaSnapshot is one meta replica's storage state at collection time,
// alongside the consensus state RaftSnapshot carries. Every field comes from
// figures raft and badger already hold, so a collection reads no files.
type MetaSnapshot struct {
	NodeID string

	// FSMLSMBytes is the LSM tree's on-disk size, or zero when it could not be
	// read, reported as no observation rather than as an empty state machine.
	FSMLSMBytes int64
	// FSMVLogBytes is the value log's on-disk size, or zero when it could not
	// be read. Badger preallocates this file, so it steps rather than grows.
	FSMVLogBytes int64

	SnapshotIndex int64
	LastLogIndex  int64
}

// RegisterMetaGauges observes one replica's storage state on every collection.
// Separate from RegisterRaftGauges because the two answer different questions:
// one is whether consensus is healthy, this is whether the single raft group is
// running out of room.
func RegisterMetaGauges(snapshot func() MetaSnapshot) (func() error, error) {
	instruments()
	if meter == nil {
		return func() error { return nil }, nil
	}

	reg, err := meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			s := snapshot()
			node := metric.WithAttributeSet(attribute.NewSet(attribute.String(nodeAttrKey, s.NodeID)))
			if s.FSMLSMBytes > 0 {
				o.ObserveInt64(metaFSMLSMBytes, s.FSMLSMBytes, node)
			}
			if s.FSMVLogBytes > 0 {
				o.ObserveInt64(metaFSMVLogBytes, s.FSMVLogBytes, node)
			}
			o.ObserveInt64(metaSnapshotIndex, s.SnapshotIndex, node)
			// Trailing entries are what a snapshot would truncate. Clamped
			// because the two indexes are read from separate raft fields and a
			// negative gap is a torn read, not a real state.
			o.ObserveInt64(metaLogTrailing, max(s.LastLogIndex-s.SnapshotIndex, 0), node)
			return nil
		},
		metaFSMLSMBytes, metaFSMVLogBytes, metaSnapshotIndex, metaLogTrailing,
	)
	if err != nil {
		return nil, err
	}
	return reg.Unregister, nil
}

// RepairSnapshot is what a repair service reports about its sweeps. Pending is
// the one to watch: it counts shard positions the last completed pass could
// not rebuild, so a cluster sitting above zero is short redundancy now.
type RepairSnapshot struct {
	Pending  int64
	Repaired int64
	Failed   int64
	Passes   int64
}

// RegisterRepairGauges observes the repair sweep. Repair runs unasked, so its
// progress has to be visible without an operator having opted into anything.
func RegisterRepairGauges(snapshot func() RepairSnapshot) (func() error, error) {
	instruments()
	if meter == nil {
		return func() error { return nil }, nil
	}

	reg, err := meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			s := snapshot()
			o.ObserveInt64(repairPending, s.Pending)
			o.ObserveInt64(repairRepaired, s.Repaired)
			o.ObserveInt64(repairFailed, s.Failed)
			o.ObserveInt64(repairPasses, s.Passes)

			return nil
		},
		repairPending, repairRepaired, repairFailed, repairPasses,
	)
	if err != nil {
		return nil, err
	}

	return reg.Unregister, nil
}

// RegisterPoolGauges observes how many connections one node's pool holds.
func RegisterPoolGauges(node uint64, held func() int64) (func() error, error) {
	instruments()
	if meter == nil {
		return func() error { return nil }, nil
	}

	attrs := metric.WithAttributeSet(attribute.NewSet(
		attribute.String(nodeAttrKey, strconv.FormatUint(node, 10)),
	))
	reg, err := meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(rpcConnections, held(), attrs)
			return nil
		},
		rpcConnections,
	)
	if err != nil {
		return nil, err
	}
	return reg.Unregister, nil
}

// objectAttrKey identifies one pre-built object-level attribute set. These
// carry no node, so there are only a handful of them.
type objectAttrKey struct {
	key1, val1, key2, val2 string
}

var (
	objectAttrMu    sync.RWMutex
	objectAttrCache = map[objectAttrKey]attrOpts{}
)

// objectAttrs returns the pre-built options for a bounded pair of attributes.
// An empty second value omits it, which is what a read path or an unqualified
// success wants.
func objectAttrs(key1, val1, key2, val2 string) attrOpts {
	k := objectAttrKey{key1: key1, val1: val1, key2: key2, val2: val2}

	objectAttrMu.RLock()
	opts, ok := objectAttrCache[k]
	objectAttrMu.RUnlock()
	if ok {
		return opts
	}

	attrs := []attribute.KeyValue{attribute.String(key1, val1)}
	if val2 != "" {
		attrs = append(attrs, attribute.String(key2, val2))
	}
	opts = newAttrOpts(attrs...)

	objectAttrMu.Lock()
	objectAttrCache[k] = opts
	objectAttrMu.Unlock()
	return opts
}

// RecordObjectRead counts one object read served. path is ReadPathDirect when
// the data shards alone answered it, and ReadPathReconstructed when parity was
// consumed to rebuild it.
func RecordObjectRead(ctx context.Context, path string) {
	instruments()
	if objectReads == nil {
		return
	}
	objectReads.Add(ctx, 1, objectAttrs("path", path, "", "").add...)
}

// RecordObjectWrite counts one object write. outcome is a WriteOutcome
// constant and reason a WriteReason constant, empty on success.
func RecordObjectWrite(ctx context.Context, outcome, reason string) {
	instruments()
	if objectWrites == nil {
		return
	}
	objectWrites.Add(ctx, 1, objectAttrs("outcome", outcome, "reason", reason).add...)
}

// EnterGateInflight records one request holding size bytes of object payload
// in gate memory, and returns the function that releases it. Returning the
// release rather than exposing a signed delta is what makes the pair
// impossible to unbalance: callers defer it at the point they acquire.
//
// It measures declared object bytes rather than the heap. The point is that
// concurrency multiplies object size, which is what exhausts a gate.
func EnterGateInflight(ctx context.Context, op string, size int64) func() {
	instruments()
	if gateInflightBytes == nil && gateInflightRequests == nil {
		return func() {}
	}

	opts := objectAttrs("op", op, "", "")
	if gateInflightBytes != nil {
		gateInflightBytes.Add(ctx, size, opts.add...)
	}
	if gateInflightRequests != nil {
		gateInflightRequests.Add(ctx, 1, opts.add...)
	}

	return func() {
		if gateInflightBytes != nil {
			gateInflightBytes.Add(ctx, -size, opts.add...)
		}
		if gateInflightRequests != nil {
			gateInflightRequests.Add(ctx, -1, opts.add...)
		}
	}
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
