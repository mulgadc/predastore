package telemetry

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// withManualReader installs a real MeterProvider backed by a ManualReader for
// the duration of the test and restores the previous global provider on
// cleanup. It also resets instrumentsOnce so the test's Record calls create
// fresh instruments bound to the manual reader — otherwise the package-level
// Once fired by an earlier test would keep pointing at whatever provider was
// live when it first ran.
func withManualReader(t testing.TB) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	instrumentsOnce = sync.Once{}
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		instrumentsOnce = sync.Once{}
	})
	return reader
}

// collect gathers one snapshot keyed by metric name.
func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Aggregation {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	out := map[string]metricdata.Aggregation{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m.Data
		}
	}
	return out
}

// sumFor returns the value of the sum datapoint whose attributes match every
// key/value in want, and fails when no datapoint matches.
func sumFor(t *testing.T, data metricdata.Aggregation, want map[string]string) int64 {
	t.Helper()
	sum, ok := data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("aggregation is %T, want Sum[int64]", data)
	}
	for _, dp := range sum.DataPoints {
		if attrsMatch(dp.Attributes, want) {
			return dp.Value
		}
	}
	t.Fatalf("no datapoint with attributes %v", want)
	return 0
}

// gaugeFor returns the value of the gauge datapoint matching want. found is
// false when the gauge emitted no such datapoint, which is a real outcome:
// an unparseable raft index is deliberately not observed at all.
func gaugeFor(t *testing.T, data metricdata.Aggregation, want map[string]string) (int64, bool) {
	t.Helper()
	g, ok := data.(metricdata.Gauge[int64])
	if !ok {
		t.Fatalf("aggregation is %T, want Gauge[int64]", data)
	}
	for _, dp := range g.DataPoints {
		if attrsMatch(dp.Attributes, want) {
			return dp.Value, true
		}
	}
	return 0, false
}

func attrsMatch(set attribute.Set, want map[string]string) bool {
	for k, v := range want {
		got, ok := set.Value(attribute.Key(k))
		if !ok || got.AsString() != v {
			return false
		}
	}
	return true
}

func TestRecordMultipartUploadCountsOutcomes(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordMultipartUpload(ctx, UploadCreated)
	RecordMultipartUpload(ctx, UploadCreated)
	RecordMultipartUpload(ctx, UploadCompleted)
	RecordMultipartUpload(ctx, UploadRejected)

	m := collect(t, reader)
	if got := sumFor(t, m["predastore.multipart.uploads"], map[string]string{"outcome": UploadCreated}); got != 2 {
		t.Errorf("created uploads = %d, want 2", got)
	}
	if got := sumFor(t, m["predastore.multipart.uploads"], map[string]string{"outcome": UploadCompleted}); got != 1 {
		t.Errorf("completed uploads = %d, want 1", got)
	}
	if got := sumFor(t, m["predastore.multipart.uploads"], map[string]string{"outcome": UploadRejected}); got != 1 {
		t.Errorf("rejected uploads = %d, want 1", got)
	}
}

// A rejected completion leaves the upload in place, so it must not close one
// out: two creates, one completion and one rejection leaves one still active.
func TestRecordMultipartUploadTracksActiveAndRejectionLeavesItOpen(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordMultipartUpload(ctx, UploadCreated)
	RecordMultipartUpload(ctx, UploadCreated)
	RecordMultipartUpload(ctx, UploadRejected)
	RecordMultipartUpload(ctx, UploadCompleted)

	m := collect(t, reader)
	if got := sumFor(t, m["predastore.multipart.sessions"], nil); got != 1 {
		t.Errorf("open sessions = %d, want 1", got)
	}

	RecordMultipartUpload(ctx, UploadAborted)
	m = collect(t, reader)
	if got := sumFor(t, m["predastore.multipart.sessions"], nil); got != 0 {
		t.Errorf("open sessions after abort = %d, want 0", got)
	}
}

func TestRecordMultipartPartCountsPartsAndBytes(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordMultipartPart(ctx, 8<<20)
	RecordMultipartPart(ctx, 4<<20)
	// A zero-length part still counts as a part; it just adds no bytes.
	RecordMultipartPart(ctx, 0)

	m := collect(t, reader)
	if got := sumFor(t, m["predastore.multipart.part.count"], nil); got != 3 {
		t.Errorf("parts = %d, want 3", got)
	}
	if got := sumFor(t, m["predastore.multipart.part.bytes"], nil); got != 12<<20 {
		t.Errorf("part bytes = %d, want %d", got, 12<<20)
	}
}

func TestRecordMultipartPartFetchSeparatesOutcomes(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordMultipartPartFetch(ctx, "")
	RecordMultipartPartFetch(ctx, FetchReasonShardRead)
	RecordMultipartPartFetch(ctx, FetchReasonShardRead)
	RecordMultipartPartFetch(ctx, FetchReasonMetaMissing)

	m := collect(t, reader)
	data := m["predastore.multipart.part.fetches"]
	if got := sumFor(t, data, map[string]string{"outcome": "success"}); got != 1 {
		t.Errorf("successful fetches = %d, want 1", got)
	}
	if got := sumFor(t, data, map[string]string{"outcome": "error", "reason": FetchReasonShardRead}); got != 2 {
		t.Errorf("shard-read failures = %d, want 2", got)
	}
	if got := sumFor(t, data, map[string]string{"outcome": "error", "reason": FetchReasonMetaMissing}); got != 1 {
		t.Errorf("meta-missing failures = %d, want 1", got)
	}
}

func TestRecordShardErrorSeparatesReasons(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordShardError(ctx, ShardOpRead, ShardReasonNotFound, 5)
	RecordShardError(ctx, ShardOpRead, ShardReasonNotFound, 5)
	RecordShardError(ctx, ShardOpRead, ShardReasonTransport, 5)

	m := collect(t, reader)
	data := m["predastore.shard.errors"]
	if got := sumFor(t, data, map[string]string{"op": ShardOpRead, "reason": ShardReasonNotFound}); got != 2 {
		t.Errorf("not-found shard errors = %d, want 2", got)
	}
	if got := sumFor(t, data, map[string]string{"op": ShardOpRead, "reason": ShardReasonTransport}); got != 1 {
		t.Errorf("transport shard errors = %d, want 1", got)
	}
}

// A failing node is only actionable if the counter names it. mulga-fnsp1 was
// one blob node stalling writes for two days while the cluster read fine, and
// an unattributed error rate would not have separated it from the others.
func TestRecordShardErrorSeparatesNodes(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordShardError(ctx, ShardOpWrite, ShardReasonTransport, 5)
	RecordShardError(ctx, ShardOpWrite, ShardReasonTransport, 5)
	RecordShardError(ctx, ShardOpWrite, ShardReasonTransport, 6)

	data := collect(t, reader)["predastore.shard.errors"]
	if got := sumFor(t, data, map[string]string{"node": "5"}); got != 2 {
		t.Errorf("node 5 write errors = %d, want 2", got)
	}
	if got := sumFor(t, data, map[string]string{"node": "6"}); got != 1 {
		t.Errorf("node 6 write errors = %d, want 1", got)
	}
}

// The error counter needs a denominator: a rising error count on a busier node
// is not the same as a rising error rate.
func TestRecordShardOpCountsAttemptsAndDuration(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordShardOp(ctx, ShardOpRead, OutcomeSuccess, 5, 0.01)
	RecordShardOp(ctx, ShardOpRead, OutcomeSuccess, 5, 0.02)
	RecordShardOp(ctx, ShardOpRead, OutcomeError, 5, 5.0)

	m := collect(t, reader)
	ops := m["predastore.shard.ops"]
	if got := sumFor(t, ops, map[string]string{"op": ShardOpRead, "outcome": OutcomeSuccess, "node": "5"}); got != 2 {
		t.Errorf("successful reads = %d, want 2", got)
	}
	if got := sumFor(t, ops, map[string]string{"op": ShardOpRead, "outcome": OutcomeError, "node": "5"}); got != 1 {
		t.Errorf("failed reads = %d, want 1", got)
	}

	hist, ok := m["predastore.shard.duration"].(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("duration aggregation is %T, want Histogram[float64]", m["predastore.shard.duration"])
	}
	// Every attempt is timed, and the histogram carries op and node but not
	// outcome: a slow failure is part of the node's latency, not a separate
	// series to go looking for.
	var count uint64
	for _, dp := range hist.DataPoints {
		if attrsMatch(dp.Attributes, map[string]string{"op": ShardOpRead, "node": "5"}) {
			count += dp.Count
			if _, hasOutcome := dp.Attributes.Value(attribute.Key("outcome")); hasOutcome {
				t.Error("duration histogram carries an outcome attribute")
			}
		}
	}
	if count != 3 {
		t.Errorf("timed shard reads = %d, want 3", count)
	}
}

// The SDK default boundaries stop at 10000 and start at 5, which is a
// millisecond scale. Left alone, every duration below five seconds shares one
// bucket and every percentile reads back as the same 2.5.
func TestDurationHistogramsUseSecondScaleBuckets(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordShardOp(ctx, ShardOpRead, OutcomeSuccess, 5, 0.01)
	RecordObjectPhase(ctx, GateOpPut, PhaseShardFanout, 0.01)
	RecordMetaClientOp(ctx, MetaOpGet, OutcomeSuccess, 0.01)

	m := collect(t, reader)
	for _, name := range []string{
		"predastore.shard.duration",
		"predastore.object.phase.duration",
		"predastore.meta.client.duration",
	} {
		hist, ok := m[name].(metricdata.Histogram[float64])
		if !ok {
			t.Fatalf("%s aggregation is %T, want Histogram[float64]", name, m[name])
		}
		for _, dp := range hist.DataPoints {
			if !slices.Equal(dp.Bounds, secondsBuckets) {
				t.Errorf("%s bounds = %v, want %v", name, dp.Bounds, secondsBuckets)
			}
		}
	}
}

// The degraded-read rate is the whole point: a reconstructed read means parity
// was consumed to answer it, which is the only in-band evidence that a blob
// node is losing data.
func TestRecordObjectReadSeparatesDegradedReads(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordObjectRead(ctx, ReadPathDirect)
	RecordObjectRead(ctx, ReadPathDirect)
	RecordObjectRead(ctx, ReadPathDirect)
	RecordObjectRead(ctx, ReadPathReconstructed)

	data := collect(t, reader)["predastore.object.reads"]
	if got := sumFor(t, data, map[string]string{"path": ReadPathDirect}); got != 3 {
		t.Errorf("direct reads = %d, want 3", got)
	}
	if got := sumFor(t, data, map[string]string{"path": ReadPathReconstructed}); got != 1 {
		t.Errorf("reconstructed reads = %d, want 1", got)
	}
}

func TestRecordObjectWriteSeparatesOutcomes(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordObjectWrite(ctx, WriteOutcomeSuccess, "")
	RecordObjectWrite(ctx, WriteOutcomeFailed, WriteReasonStoreFull)
	RecordObjectWrite(ctx, WriteOutcomeFailed, WriteReasonShardWrite)
	RecordObjectWrite(ctx, WriteOutcomeFailed, WriteReasonShardWrite)

	data := collect(t, reader)["predastore.object.writes"]
	if got := sumFor(t, data, map[string]string{"outcome": WriteOutcomeSuccess}); got != 1 {
		t.Errorf("successful writes = %d, want 1", got)
	}
	if got := sumFor(t, data, map[string]string{"outcome": WriteOutcomeFailed, "reason": WriteReasonShardWrite}); got != 2 {
		t.Errorf("shard-write failures = %d, want 2", got)
	}
	if got := sumFor(t, data, map[string]string{"outcome": WriteOutcomeFailed, "reason": WriteReasonStoreFull}); got != 1 {
		t.Errorf("store-full failures = %d, want 1", got)
	}
}

// A success carries no reason, so the successful series must not be split by
// an empty-string reason attribute nobody would think to filter on.
func TestRecordObjectWriteOmitsReasonOnSuccess(t *testing.T) {
	reader := withManualReader(t)
	RecordObjectWrite(context.Background(), WriteOutcomeSuccess, "")

	sum, ok := collect(t, reader)["predastore.object.writes"].(metricdata.Sum[int64])
	if !ok {
		t.Fatal("object writes is not a Sum[int64]")
	}
	for _, dp := range sum.DataPoints {
		if _, has := dp.Attributes.Value(attribute.Key("reason")); has {
			t.Error("a successful write carries a reason attribute")
		}
	}
}

// The release must take back exactly what was added, or the gauge floor climbs
// and the in-flight reading stops meaning anything across a long run.
func TestEnterGateInflightReleasesWhatItTook(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	releaseA := EnterGateInflight(ctx, GateOpPut, 4<<20)
	releaseB := EnterGateInflight(ctx, GateOpPut, 1<<20)

	m := collect(t, reader)
	put := map[string]string{"op": GateOpPut}
	if got := sumFor(t, m["predastore.gate.inflight.bytes"], put); got != 5<<20 {
		t.Errorf("in-flight bytes = %d, want %d", got, 5<<20)
	}
	if got := sumFor(t, m["predastore.gate.inflight.requests"], put); got != 2 {
		t.Errorf("in-flight requests = %d, want 2", got)
	}

	releaseA()
	releaseB()

	m = collect(t, reader)
	if got := sumFor(t, m["predastore.gate.inflight.bytes"], put); got != 0 {
		t.Errorf("in-flight bytes after release = %d, want 0", got)
	}
	if got := sumFor(t, m["predastore.gate.inflight.requests"], put); got != 0 {
		t.Errorf("in-flight requests after release = %d, want 0", got)
	}
}

// Per-shard recorders run K+M times per object, so an attribute set built per
// call would allocate on every shard of every request. The cache is what makes
// them free; this is the test that keeps it that way.
//
// It measures the recorder itself with the global meter left at its no-op
// default, which is the caller-side cost the hot path pays. What the SDK does
// with a live provider is the SDK's own budget.
func TestPerShardRecordersDoNotAllocate(t *testing.T) {
	ctx := context.Background()

	// Prime the caches: the first call for a given key builds its set, and it
	// is every call after it that must be free.
	RecordShardOp(ctx, ShardOpRead, OutcomeSuccess, 7, 0.01)
	RecordShardError(ctx, ShardOpRead, ShardReasonNotFound, 7)

	if got := testing.AllocsPerRun(100, func() {
		RecordShardOp(ctx, ShardOpRead, OutcomeSuccess, 7, 0.01)
	}); got != 0 {
		t.Errorf("RecordShardOp allocations = %v, want 0", got)
	}
	if got := testing.AllocsPerRun(100, func() {
		RecordShardError(ctx, ShardOpRead, ShardReasonNotFound, 7)
	}); got != 0 {
		t.Errorf("RecordShardError allocations = %v, want 0", got)
	}
}

// Phases and meta calls run several times per request, and both take their
// attributes from bounded sets, so the same cache that makes the shard
// recorders free applies to them.
func TestPerRequestRecordersDoNotAllocate(t *testing.T) {
	ctx := context.Background()

	RecordObjectPhase(ctx, GateOpPut, PhaseShardFanout, 0.01)
	RecordMetaClientOp(ctx, MetaOpGet, OutcomeSuccess, 0.01)
	RecordMetaRedirect(ctx, RedirectNotLeader)

	if got := testing.AllocsPerRun(100, func() {
		RecordObjectPhase(ctx, GateOpPut, PhaseShardFanout, 0.01)
	}); got != 0 {
		t.Errorf("RecordObjectPhase allocations = %v, want 0", got)
	}
	if got := testing.AllocsPerRun(100, func() {
		RecordMetaClientOp(ctx, MetaOpGet, OutcomeSuccess, 0.01)
	}); got != 0 {
		t.Errorf("RecordMetaClientOp allocations = %v, want 0", got)
	}
	if got := testing.AllocsPerRun(100, func() {
		RecordMetaRedirect(ctx, RedirectNotLeader)
	}); got != 0 {
		t.Errorf("RecordMetaRedirect allocations = %v, want 0", got)
	}
}

// A node's attribute set is built once per peer, not once per stream: a stream
// is opened per shard, and the id has to be formatted to build one.
func TestNodeAttrsAreCached(t *testing.T) {
	first := nodeAttrs(9)
	second := nodeAttrs(9)
	if &first.add[0] != &second.add[0] {
		t.Error("node attribute option was rebuilt rather than cached")
	}

	if got := testing.AllocsPerRun(100, func() { _ = nodeAttrs(9) }); got != 0 {
		t.Errorf("nodeAttrs allocations = %v, want 0", got)
	}
}

// The attribute cache is keyed by bounded fields only, so it must hand back the
// identical option rather than an equal one: a cache that rebuilt on every call
// would still pass a behavioural test while allocating on every shard.
func TestShardAttrsAreCached(t *testing.T) {
	first := cachedShardAttrs(shardOpAttrs, "outcome", ShardOpRead, OutcomeSuccess, 11)
	second := cachedShardAttrs(shardOpAttrs, "outcome", ShardOpRead, OutcomeSuccess, 11)
	if &first.add[0] != &second.add[0] {
		t.Error("shard attribute option was rebuilt rather than cached")
	}

	other := cachedShardAttrs(shardOpAttrs, "outcome", ShardOpRead, OutcomeSuccess, 12)
	if &first.add[0] == &other.add[0] {
		t.Error("two nodes share one attribute option")
	}
}

func TestRegisterRaftGaugesObservesOneSnapshot(t *testing.T) {
	reader := withManualReader(t)

	unregister, err := RegisterRaftGauges(func() RaftSnapshot {
		return RaftSnapshot{
			NodeID:       "3",
			State:        "Leader",
			LeaderKnown:  true,
			Term:         "42",
			CommitIndex:  "1200",
			AppliedIndex: "1150",
		}
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() {
		if err := unregister(); err != nil {
			t.Errorf("unregister: %v", err)
		}
	})

	m := collect(t, reader)
	node := map[string]string{"node": "3"}

	if v, ok := gaugeFor(t, m["predastore.meta.raft.state"], map[string]string{"node": "3", "raft.state": "Leader"}); !ok || v != 1 {
		t.Errorf("raft state gauge = %d (found %v), want 1", v, ok)
	}
	for name, want := range map[string]int64{
		"predastore.meta.raft.term":          42,
		"predastore.meta.raft.commit_index":  1200,
		"predastore.meta.raft.applied_index": 1150,
		"predastore.meta.raft.applied_lag":   50,
		"predastore.meta.raft.leader_known":  1,
	} {
		v, ok := gaugeFor(t, m[name], node)
		if !ok {
			t.Errorf("%s: no datapoint for node 3", name)
			continue
		}
		if v != want {
			t.Errorf("%s = %d, want %d", name, v, want)
		}
	}
}

// A replica observing no leader reports leader_known 0 rather than failing to
// report: "answered, and sees no leader" is the condition worth alerting on.
func TestRegisterRaftGaugesReportsNoLeaderAsZero(t *testing.T) {
	reader := withManualReader(t)

	unregister, err := RegisterRaftGauges(func() RaftSnapshot {
		return RaftSnapshot{NodeID: "1", State: "Candidate", Term: "9", CommitIndex: "5", AppliedIndex: "5"}
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer func() { _ = unregister() }()

	m := collect(t, reader)
	if v, ok := gaugeFor(t, m["predastore.meta.raft.leader_known"], map[string]string{"node": "1"}); !ok || v != 0 {
		t.Errorf("leader_known = %d (found %v), want 0", v, ok)
	}
	if v, ok := gaugeFor(t, m["predastore.meta.raft.applied_lag"], map[string]string{"node": "1"}); !ok || v != 0 {
		t.Errorf("applied_lag = %d (found %v), want 0", v, ok)
	}
}

// Zero is a legitimate raft index, so a value that will not parse must produce
// no observation at all rather than a plausible-looking zero.
func TestRegisterRaftGaugesSkipsUnparseableIndexes(t *testing.T) {
	reader := withManualReader(t)

	unregister, err := RegisterRaftGauges(func() RaftSnapshot {
		return RaftSnapshot{NodeID: "2", State: "Follower", LeaderKnown: true, Term: "", CommitIndex: "not-a-number", AppliedIndex: "7"}
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer func() { _ = unregister() }()

	m := collect(t, reader)
	node := map[string]string{"node": "2"}

	if _, ok := m["predastore.meta.raft.term"]; ok {
		if _, found := gaugeFor(t, m["predastore.meta.raft.term"], node); found {
			t.Error("term observed despite an empty value")
		}
	}
	if _, ok := m["predastore.meta.raft.commit_index"]; ok {
		if _, found := gaugeFor(t, m["predastore.meta.raft.commit_index"], node); found {
			t.Error("commit index observed despite an unparseable value")
		}
	}
	if _, ok := m["predastore.meta.raft.applied_lag"]; ok {
		if _, found := gaugeFor(t, m["predastore.meta.raft.applied_lag"], node); found {
			t.Error("applied lag observed without a commit index to subtract from")
		}
	}
	if v, ok := gaugeFor(t, m["predastore.meta.raft.applied_index"], node); !ok || v != 7 {
		t.Errorf("applied index = %d (found %v), want 7", v, ok)
	}
}

// Two replicas in one process share the instruments, so the node attribute is
// the only thing keeping their readings apart.
func TestRegisterRaftGaugesSeparatesReplicasByNode(t *testing.T) {
	reader := withManualReader(t)

	for _, node := range []string{"1", "2"} {
		unregister, err := RegisterRaftGauges(func() RaftSnapshot {
			return RaftSnapshot{NodeID: node, State: "Follower", LeaderKnown: true, Term: "4", CommitIndex: node, AppliedIndex: node}
		})
		if err != nil {
			t.Fatalf("register node %s: %v", node, err)
		}
		defer func() { _ = unregister() }()
	}

	m := collect(t, reader)
	for _, want := range []struct {
		node  string
		value int64
	}{{"1", 1}, {"2", 2}} {
		v, ok := gaugeFor(t, m["predastore.meta.raft.commit_index"], map[string]string{"node": want.node})
		if !ok || v != want.value {
			t.Errorf("node %s commit index = %d (found %v), want %d", want.node, v, ok, want.value)
		}
	}
}

func TestRegisterRaftGaugesUnregisterStopsObservation(t *testing.T) {
	reader := withManualReader(t)

	unregister, err := RegisterRaftGauges(func() RaftSnapshot {
		return RaftSnapshot{NodeID: "5", State: "Leader", LeaderKnown: true, Term: "1", CommitIndex: "1", AppliedIndex: "1"}
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, ok := gaugeFor(t, collect(t, reader)["predastore.meta.raft.term"], map[string]string{"node": "5"}); !ok {
		t.Fatal("term not observed while registered")
	}

	if err := unregister(); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	m := collect(t, reader)
	if data, ok := m["predastore.meta.raft.term"]; ok {
		if _, found := gaugeFor(t, data, map[string]string{"node": "5"}); found {
			t.Error("term still observed after unregister")
		}
	}
}

// Elasticsearch maps a dotted metric name as a path, so a name that is also
// another name's prefix would have to be both a leaf and an object. The second
// one to arrive is rejected and its series are lost. Exercising every recorder
// and then comparing the names that actually reached the reader keeps this
// honest as instruments are added.
func TestNoMetricNameIsAnotherPrefix(t *testing.T) {
	names := slices.Collect(maps.Keys(recordEveryInstrument(t)))
	if len(names) == 0 {
		t.Fatal("no metrics collected")
	}
	for _, outer := range names {
		for _, inner := range names {
			if outer != inner && strings.HasPrefix(inner, outer+".") {
				t.Errorf("%q is a prefix of %q: Elasticsearch cannot map it as both a leaf and an object", outer, inner)
			}
		}
	}
}

func TestRecordObjectPhaseSeparatesPhasesAndOps(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordObjectPhase(ctx, GateOpPut, PhaseShardFanout, 0.4)
	RecordObjectPhase(ctx, GateOpPut, PhaseMetaPlacement, 0.01)
	RecordObjectPhase(ctx, GateOpGet, PhaseShardFanout, 0.2)

	m := collect(t, reader)
	h, ok := m["predastore.object.phase.duration"].(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("phase duration is %T, want Histogram[float64]", m["predastore.object.phase.duration"])
	}

	// A PUT's fanout and a GET's fanout share a phase name, so only the op
	// separates them; conflating the two would hide which side is slow.
	for _, want := range []struct {
		attrs map[string]string
		sum   float64
	}{
		{attrs: map[string]string{"op": GateOpPut, "phase": PhaseShardFanout}, sum: 0.4},
		{attrs: map[string]string{"op": GateOpPut, "phase": PhaseMetaPlacement}, sum: 0.01},
		{attrs: map[string]string{"op": GateOpGet, "phase": PhaseShardFanout}, sum: 0.2},
	} {
		found := false
		for _, dp := range h.DataPoints {
			if !attrsMatch(dp.Attributes, want.attrs) {
				continue
			}
			found = true
			if dp.Count != 1 || dp.Sum != want.sum {
				t.Errorf("phase %v: count=%d sum=%v, want 1 and %v", want.attrs, dp.Count, dp.Sum, want.sum)
			}
		}
		if !found {
			t.Errorf("no datapoint for %v", want.attrs)
		}
	}
}

// Not-found is the answer to a question, not a failure. Counting it as an
// error would make a HEAD of a missing object look like a broken meta plane.
func TestRecordMetaClientOpKeepsNotFoundSeparateFromError(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordMetaClientOp(ctx, MetaOpGet, OutcomeSuccess, 0.01)
	RecordMetaClientOp(ctx, MetaOpGet, MetaOutcomeNotFound, 0.02)
	RecordMetaClientOp(ctx, MetaOpGet, MetaOutcomeNotFound, 0.02)
	RecordMetaClientOp(ctx, MetaOpPut, OutcomeError, 0.5)

	m := collect(t, reader)
	ops := m["predastore.meta.client.ops"]
	if got := sumFor(t, ops, map[string]string{"op": MetaOpGet, "outcome": OutcomeSuccess}); got != 1 {
		t.Errorf("successful gets = %d, want 1", got)
	}
	if got := sumFor(t, ops, map[string]string{"op": MetaOpGet, "outcome": MetaOutcomeNotFound}); got != 2 {
		t.Errorf("not-found gets = %d, want 2", got)
	}
	if got := sumFor(t, ops, map[string]string{"op": MetaOpPut, "outcome": OutcomeError}); got != 1 {
		t.Errorf("failed puts = %d, want 1", got)
	}

	// The duration carries op but not outcome: a slow failure is part of the
	// operation's latency, not a separate series.
	h, ok := m["predastore.meta.client.duration"].(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("meta duration is %T, want Histogram[float64]", m["predastore.meta.client.duration"])
	}
	for _, dp := range h.DataPoints {
		if _, has := dp.Attributes.Value("outcome"); has {
			t.Errorf("meta duration carries an outcome attribute, want op only")
		}
	}
}

func TestRecordMetaRedirectSeparatesReasons(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordMetaRedirect(ctx, RedirectNotLeader)
	RecordMetaRedirect(ctx, RedirectNotLeader)
	RecordMetaRedirect(ctx, RedirectNoLeader)
	RecordMetaRedirect(ctx, RedirectRetryExhausted)

	m := collect(t, reader)
	redirects := m["predastore.meta.client.redirects"]
	for reason, want := range map[string]int64{
		RedirectNotLeader:      2,
		RedirectNoLeader:       1,
		RedirectRetryExhausted: 1,
	} {
		if got := sumFor(t, redirects, map[string]string{"reason": reason}); got != want {
			t.Errorf("%s redirects = %d, want %d", reason, got, want)
		}
	}
}

// The gauge must come back to zero when streams are released, and hold what is
// still open — a leaked stream is exactly what it is there to show.
func TestEnterRPCStreamTracksOpenStreams(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	releaseA := EnterRPCStream(ctx, 1)
	releaseB := EnterRPCStream(ctx, 1)
	release2 := EnterRPCStream(ctx, 2)
	releaseA()

	m := collect(t, reader)
	if got := sumFor(t, m["predastore.rpc.streams.open"], map[string]string{"node": "1"}); got != 1 {
		t.Errorf("open streams to node 1 = %d, want 1", got)
	}
	if got := sumFor(t, m["predastore.rpc.streams.open"], map[string]string{"node": "2"}); got != 1 {
		t.Errorf("open streams to node 2 = %d, want 1", got)
	}

	releaseB()
	release2()
	m = collect(t, reader)
	if got := sumFor(t, m["predastore.rpc.streams.open"], map[string]string{"node": "1"}); got != 0 {
		t.Errorf("open streams to node 1 after release = %d, want 0", got)
	}
}

func TestRecordRPCEvictionSeparatesPeersAndReasons(t *testing.T) {
	reader := withManualReader(t)
	ctx := context.Background()

	RecordRPCEviction(ctx, 3, EvictionStall)
	RecordRPCEviction(ctx, 3, EvictionStall)
	RecordRPCEviction(ctx, 4, EvictionError)

	m := collect(t, reader)
	evictions := m["predastore.rpc.evictions"]
	if got := sumFor(t, evictions, map[string]string{"node": "3", "reason": EvictionStall}); got != 2 {
		t.Errorf("stall evictions of node 3 = %d, want 2", got)
	}
	if got := sumFor(t, evictions, map[string]string{"node": "4", "reason": EvictionError}); got != 1 {
		t.Errorf("error evictions of node 4 = %d, want 1", got)
	}
}

func TestRegisterMetaGaugesReportsStorageState(t *testing.T) {
	reader := withManualReader(t)

	unregister, err := RegisterMetaGauges(func() MetaSnapshot {
		return MetaSnapshot{NodeID: "2", FSMBytes: 4096, SnapshotIndex: 100, LastLogIndex: 175}
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { _ = unregister() })

	m := collect(t, reader)
	node := map[string]string{"node": "2"}
	if got, _ := gaugeFor(t, m["predastore.meta.fsm.size_bytes"], node); got != 4096 {
		t.Errorf("fsm size = %d, want 4096", got)
	}
	if got, _ := gaugeFor(t, m["predastore.meta.snapshot.index"], node); got != 100 {
		t.Errorf("snapshot index = %d, want 100", got)
	}
	// The trailing log is what a snapshot would truncate: 175 - 100.
	if got, _ := gaugeFor(t, m["predastore.meta.log.trailing"], node); got != 75 {
		t.Errorf("trailing entries = %d, want 75", got)
	}
}

// A size that could not be read is reported as no observation. Zero would read
// as an empty state machine, which is a legitimate and very different state.
func TestRegisterMetaGaugesOmitsUnreadableFSMSize(t *testing.T) {
	reader := withManualReader(t)

	unregister, err := RegisterMetaGauges(func() MetaSnapshot {
		return MetaSnapshot{NodeID: "2", SnapshotIndex: 5, LastLogIndex: 5}
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { _ = unregister() })

	m := collect(t, reader)
	if data, ok := m["predastore.meta.fsm.size_bytes"]; ok {
		if g, isGauge := data.(metricdata.Gauge[int64]); isGauge && len(g.DataPoints) > 0 {
			t.Errorf("fsm size emitted %d datapoints with no size to report, want none", len(g.DataPoints))
		}
	}
}

// A snapshot index above the last log index is a torn read of two raft fields,
// not a negative backlog.
func TestRegisterMetaGaugesClampsTrailingLog(t *testing.T) {
	reader := withManualReader(t)

	unregister, err := RegisterMetaGauges(func() MetaSnapshot {
		return MetaSnapshot{NodeID: "2", SnapshotIndex: 200, LastLogIndex: 100}
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { _ = unregister() })

	m := collect(t, reader)
	if got, _ := gaugeFor(t, m["predastore.meta.log.trailing"], map[string]string{"node": "2"}); got != 0 {
		t.Errorf("trailing entries = %d, want 0", got)
	}
}

func TestRegisterPoolGaugesReportsHeldConnections(t *testing.T) {
	reader := withManualReader(t)

	held := int64(3)
	unregister, err := RegisterPoolGauges(7, func() int64 { return held })
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { _ = unregister() })

	m := collect(t, reader)
	if got, _ := gaugeFor(t, m["predastore.rpc.connections"], map[string]string{"node": "7"}); got != 3 {
		t.Errorf("connections = %d, want 3", got)
	}

	held = 1
	m = collect(t, reader)
	if got, _ := gaugeFor(t, m["predastore.rpc.connections"], map[string]string{"node": "7"}); got != 1 {
		t.Errorf("connections after a peer dropped = %d, want 1", got)
	}
}

func TestRegisterStoreGaugesReportsTheSnapshot(t *testing.T) {
	reader := withManualReader(t)

	unregister, err := RegisterStoreGauges(fullStoreSnapshot)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { _ = unregister() })

	m := collect(t, reader)
	node := map[string]string{"node": "1"}

	for _, tc := range []struct {
		metric string
		want   int64
	}{
		{metric: "predastore.blob.free_bytes", want: 500},
		{metric: "predastore.blob.total_bytes", want: 1000},
		{metric: "predastore.blob.segments", want: 2},
		{metric: "predastore.blob.seg_num", want: 3},
		{metric: "predastore.blob.value_num", want: 40},
		{metric: "predastore.blob.frag_num", want: 500},
		{metric: "predastore.blob.live_bytes", want: 750},
		{metric: "predastore.blob.dead_bytes", want: 250},
	} {
		got, ok := gaugeFor(t, m[tc.metric], node)
		if !ok {
			t.Errorf("%s emitted no datapoint for node 1", tc.metric)
			continue
		}
		if got != tc.want {
			t.Errorf("%s = %d, want %d", tc.metric, got, tc.want)
		}
	}

	if got, ok := gaugeFor(t, m["predastore.blob.pressure"], map[string]string{"node": "1", "blob.pressure": "ok"}); !ok || got != 1 {
		t.Errorf("pressure = %d (found=%v), want a constant 1 carrying the band", got, ok)
	}

	// Corruption is a counter, not a gauge: it must survive a collection where
	// nothing new failed.
	if got := sumFor(t, m["predastore.blob.integrity.failures"], node); got != 2 {
		t.Errorf("integrity failures = %d, want 2", got)
	}
}

// The live fraction is derived at observation time rather than carried, so it
// cannot disagree with the two figures it comes from.
func TestStoreGaugesDeriveLiveFraction(t *testing.T) {
	reader := withManualReader(t)

	unregister, err := RegisterStoreGauges(fullStoreSnapshot)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { _ = unregister() })

	m := collect(t, reader)
	g, ok := m["predastore.blob.live_frac"].(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("live_frac is %T, want Gauge[float64]", m["predastore.blob.live_frac"])
	}
	if len(g.DataPoints) != 1 {
		t.Fatalf("live_frac datapoints = %d, want 1", len(g.DataPoints))
	}
	// 750 live of 1000 total.
	if got := g.DataPoints[0].Value; got != 0.75 {
		t.Errorf("live_frac = %v, want 0.75", got)
	}
}

// A store that has never measured free space or never run a compaction scan
// must report nothing for those, not a zero: zero free bytes reads as a full
// disk and zero dead bytes as a perfectly compacted store.
func TestStoreGaugesOmitWhatHasNotBeenMeasured(t *testing.T) {
	reader := withManualReader(t)

	unregister, err := RegisterStoreGauges(func() StoreSnapshot {
		return StoreSnapshot{NodeID: "1", SegNum: 1, LiveSegments: 1}
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { _ = unregister() })

	m := collect(t, reader)
	for _, name := range []string{
		"predastore.blob.free_bytes",
		"predastore.blob.total_bytes",
		"predastore.blob.live_bytes",
		"predastore.blob.dead_bytes",
	} {
		if data, ok := m[name]; ok {
			if g, isGauge := data.(metricdata.Gauge[int64]); isGauge && len(g.DataPoints) > 0 {
				t.Errorf("%s emitted %d datapoints for an unmeasured store, want none", name, len(g.DataPoints))
			}
		}
	}
	// The maintained counters are always known, so they are always reported.
	if got, ok := gaugeFor(t, m["predastore.blob.segments"], map[string]string{"node": "1"}); !ok || got != 1 {
		t.Errorf("segments = %d (found=%v), want 1 even before any measurement", got, ok)
	}
}

// Cycles split into succeeded and failed, and the two must add back up to the
// total: a dashboard reading only "cycles" would otherwise double-count.
func TestStoreGaugesSplitCompactionOutcomes(t *testing.T) {
	reader := withManualReader(t)

	unregister, err := RegisterStoreGauges(fullStoreSnapshot)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { _ = unregister() })

	m := collect(t, reader)
	cycles := m["predastore.blob.compaction.cycles"]
	ok := sumFor(t, cycles, map[string]string{"node": "1", "outcome": OutcomeSuccess})
	failed := sumFor(t, cycles, map[string]string{"node": "1", "outcome": OutcomeError})
	if ok != 4 || failed != 1 {
		t.Errorf("cycles succeeded/failed = %d/%d, want 4/1", ok, failed)
	}

	segments := m["predastore.blob.compaction.segments"]
	if got := sumFor(t, segments, map[string]string{"node": "1", "kind": CompactionKindScanned}); got != 9 {
		t.Errorf("segments scanned = %d, want 9", got)
	}
	if got := sumFor(t, segments, map[string]string{"node": "1", "kind": CompactionKindDropped}); got != 7 {
		t.Errorf("segments dropped = %d, want 7", got)
	}

	bytes := m["predastore.blob.compaction.bytes"]
	if got := sumFor(t, bytes, map[string]string{"node": "1", "kind": CompactionKindRelocated}); got != 2048 {
		t.Errorf("bytes relocated = %d, want 2048", got)
	}
	if got := sumFor(t, bytes, map[string]string{"node": "1", "kind": CompactionKindReclaimed}); got != 8192 {
		t.Errorf("bytes reclaimed = %d, want 8192", got)
	}
}

// The prefix invariant is only as good as its coverage: an instrument nothing
// exercises escapes it and fails in the sink instead. metricNames is the
// declared set, so anything added there without a recorder here fails now.
func TestEveryDeclaredMetricIsExercised(t *testing.T) {
	collected := recordEveryInstrument(t)
	for _, name := range metricNames {
		if _, ok := collected[name]; !ok {
			t.Errorf("%q is declared but no recorder in recordEveryInstrument emits it", name)
		}
	}
	for name := range collected {
		if !slices.Contains(metricNames, name) {
			t.Errorf("%q was emitted but is not declared in metricNames", name)
		}
	}
}

// recordEveryInstrument drives one observation through every instrument the
// package registers and returns what reached the reader.
func recordEveryInstrument(t *testing.T) map[string]metricdata.Aggregation {
	t.Helper()
	reader := withManualReader(t)
	ctx := context.Background()

	RecordMultipartUpload(ctx, UploadCreated)
	RecordMultipartPart(ctx, 1)
	RecordMultipartPartFetch(ctx, "")
	RecordShardError(ctx, ShardOpRead, ShardReasonNotFound, 1)
	RecordShardOp(ctx, ShardOpRead, OutcomeSuccess, 1, 0.01)
	RecordObjectRead(ctx, ReadPathDirect)
	RecordObjectWrite(ctx, WriteOutcomeSuccess, "")
	EnterGateInflight(ctx, GateOpPut, 1)()
	RecordObjectPhase(ctx, GateOpPut, PhaseShardFanout, 0.01)
	RecordMetaClientOp(ctx, MetaOpGet, OutcomeSuccess, 0.01)
	RecordMetaRedirect(ctx, RedirectNotLeader)
	RecordRPCEviction(ctx, 1, EvictionStall)
	EnterRPCStream(ctx, 1)()

	unregister, err := RegisterRaftGauges(func() RaftSnapshot {
		return RaftSnapshot{NodeID: "1", State: "Leader", LeaderKnown: true, Term: "1", CommitIndex: "2", AppliedIndex: "1"}
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { _ = unregister() })

	// Measured and Scanned both true, so every conditional observation fires:
	// a snapshot with either false would leave instruments unexercised and let
	// them slip past the prefix invariant.
	unregisterStore, err := RegisterStoreGauges(func() StoreSnapshot {
		return fullStoreSnapshot()
	})
	if err != nil {
		t.Fatalf("register store: %v", err)
	}
	t.Cleanup(func() { _ = unregisterStore() })

	// A populated FSM size, since an unread one is deliberately not observed.
	unregisterMeta, err := RegisterMetaGauges(func() MetaSnapshot {
		return MetaSnapshot{NodeID: "1", FSMBytes: 1024, SnapshotIndex: 10, LastLogIndex: 20}
	})
	if err != nil {
		t.Fatalf("register meta: %v", err)
	}
	t.Cleanup(func() { _ = unregisterMeta() })

	unregisterPool, err := RegisterPoolGauges(1, func() int64 { return 2 })
	if err != nil {
		t.Fatalf("register pool: %v", err)
	}
	t.Cleanup(func() { _ = unregisterPool() })

	return collect(t, reader)
}

// fullStoreSnapshot is a snapshot with every field populated, so a collection
// over it observes every blob instrument.
func fullStoreSnapshot() StoreSnapshot {
	return StoreSnapshot{
		NodeID:                 "1",
		Measured:               true,
		FreeFrac:               0.5,
		FreeBytes:              500,
		TotalBytes:             1000,
		Pressure:               "ok",
		SegNum:                 3,
		ValueNum:               40,
		FragNum:                500,
		LiveSegments:           2,
		Scanned:                true,
		LiveBytes:              750,
		DeadBytes:              250,
		CompactionCycles:       5,
		CompactionCyclesFailed: 1,
		SegmentsScanned:        9,
		SegmentsDropped:        7,
		BytesRelocated:         2048,
		BytesReclaimed:         8192,
		LastCycleSeconds:       1.5,
		IntegrityFailures:      2,
	}
}
