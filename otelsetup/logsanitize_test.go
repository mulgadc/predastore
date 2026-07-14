package otelsetup

import (
	"context"
	"testing"
	"unicode/utf8"

	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// rawReason mimics a peer-supplied QUIC CONNECTION_CLOSE reason phrase carrying
// bytes that are not valid UTF-8, as surfaced through err.Error() on the
// shard-put path.
const rawReason = "reset by peer: \xff\xfe\x80 shard"

// assertAllStringsValid walks a record's body and attributes and fails if any
// string field would be rejected by protobuf UTF-8 marshaling.
func assertAllStringsValid(t *testing.T, rec sdklog.Record) {
	t.Helper()
	assertValueValid(t, rec.Body())
	rec.WalkAttributes(func(kv log.KeyValue) bool {
		if !utf8.ValidString(kv.Key) {
			t.Errorf("attribute key not valid UTF-8: %q", kv.Key)
		}
		assertValueValid(t, kv.Value)
		return true
	})
}

func assertValueValid(t *testing.T, v log.Value) {
	t.Helper()
	switch v.Kind() {
	case log.KindString:
		if !utf8.ValidString(v.AsString()) {
			t.Errorf("string value not valid UTF-8: %q", v.AsString())
		}
	case log.KindSlice:
		for _, e := range v.AsSlice() {
			assertValueValid(t, e)
		}
	case log.KindMap:
		for _, kv := range v.AsMap() {
			if !utf8.ValidString(kv.Key) {
				t.Errorf("map key not valid UTF-8: %q", kv.Key)
			}
			assertValueValid(t, kv.Value)
		}
	}
}

// TestUTF8ProcessorSanitizesPoisonedBatch is the regression guard for
// mulga-lo5a6: a record carrying a non-UTF-8 attribute must be exported with
// the bytes replaced, AND a valid record co-emitted through the same processor
// must survive intact — proving one poisoned record no longer fails the batch
// and drops its neighbours.
func TestUTF8ProcessorSanitizesPoisonedBatch(t *testing.T) {
	exp := &recordingLogExporter{}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(newUTF8Processor(sdklog.NewSimpleProcessor(exp))),
	)
	defer func() { _ = lp.Shutdown(context.Background()) }()

	logger := lp.Logger("test")

	var poisoned log.Record
	poisoned.SetBody(log.StringValue("handlePUTShard: append failed"))
	poisoned.AddAttributes(log.String("error", rawReason))
	logger.Emit(context.Background(), poisoned)

	var valid log.Record
	valid.SetBody(log.StringValue("healthy record"))
	valid.AddAttributes(log.String("bucket", "images"))
	logger.Emit(context.Background(), valid)

	records := exp.snapshot()
	if len(records) != 2 {
		t.Fatalf("got %d exported records, want 2 (valid record must survive a poisoned co-batch)", len(records))
	}

	for _, rec := range records {
		assertAllStringsValid(t, rec)
	}

	// The poisoned attribute keeps its non-invalid prefix/suffix; only the bad
	// bytes become the replacement rune.
	gotErr := attrString(records[0], "error")
	if utf8.ValidString(gotErr) == false || gotErr == rawReason {
		t.Errorf("poisoned error attr not sanitised: %q", gotErr)
	}
	if !containsReplacement(gotErr) {
		t.Errorf("expected replacement rune in sanitised error, got %q", gotErr)
	}

	// The valid record must be byte-for-byte unchanged.
	if got := attrString(records[1], "bucket"); got != "images" {
		t.Errorf("valid record mutated: bucket = %q, want images", got)
	}
}

// TestSanitizeValueLeavesValidUnchanged proves the hot path returns the same
// value (no allocation, identity preserved) when everything is already valid,
// including nested slices and maps.
func TestSanitizeValueLeavesValidUnchanged(t *testing.T) {
	cases := []log.Value{
		log.StringValue("plain ascii"),
		log.StringValue("valid utf-8: café €"),
		log.Int64Value(42),
		log.BoolValue(true),
		log.BytesValue([]byte{0xff, 0xfe}), // raw bytes marshal as protobuf bytes, untouched
		log.SliceValue(log.StringValue("a"), log.Int64Value(1)),
		log.MapValue(log.String("k", "v"), log.Int64("n", 2)),
	}
	for _, v := range cases {
		got, changed := sanitizeValue(v)
		if changed {
			t.Errorf("sanitizeValue(%v) reported changed, want unchanged", v)
		}
		if !got.Equal(v) {
			t.Errorf("sanitizeValue(%v) = %v, want identical", v, got)
		}
	}
}

// TestSanitizeValueRecursesNested proves invalid bytes nested inside slices and
// maps are fixed, not just top-level string attributes.
func TestSanitizeValueRecursesNested(t *testing.T) {
	nested := log.MapValue(
		log.String("reason", rawReason),
		log.Slice("frames", log.StringValue("ok"), log.StringValue("bad\xffbyte")),
	)
	got, changed := sanitizeValue(nested)
	if !changed {
		t.Fatal("sanitizeValue reported unchanged for nested invalid UTF-8")
	}
	assertValueValid(t, got)
}

func attrString(rec sdklog.Record, key string) string {
	var out string
	rec.WalkAttributes(func(kv log.KeyValue) bool {
		if kv.Key == key {
			out = kv.Value.AsString()
			return false
		}
		return true
	})
	return out
}

func containsReplacement(s string) bool {
	for _, r := range s {
		if r == '�' {
			return true
		}
	}
	return false
}
