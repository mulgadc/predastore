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

	unregister, err := RegisterRaftGauges(func() RaftSnapshot {
		return RaftSnapshot{NodeID: "1", State: "Leader", LeaderKnown: true, Term: "1", CommitIndex: "2", AppliedIndex: "1"}
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() { _ = unregister() })

	return collect(t, reader)
}
