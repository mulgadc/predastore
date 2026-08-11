package otelsetup

import (
	"context"
	"reflect"
	"testing"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
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
	rec.WalkAttributes(func(kv attribute.KeyValue) bool {
		if !utf8.ValidString(string(kv.Key)) {
			t.Errorf("attribute key not valid UTF-8: %q", kv.Key)
		}
		assertValueValid(t, kv.Value)
		return true
	})
}

func assertValueValid(t *testing.T, v attribute.Value) {
	t.Helper()
	switch v.Type() {
	case attribute.STRING:
		if !utf8.ValidString(v.AsString()) {
			t.Errorf("string value not valid UTF-8: %q", v.AsString())
		}
	case attribute.STRINGSLICE:
		for _, s := range v.AsStringSlice() {
			if !utf8.ValidString(s) {
				t.Errorf("string slice element not valid UTF-8: %q", s)
			}
		}
	case attribute.SLICE:
		for _, e := range v.AsSlice() {
			assertValueValid(t, e)
		}
	case attribute.MAP:
		for _, kv := range v.AsMap() {
			if !utf8.ValidString(string(kv.Key)) {
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
	poisoned.SetBody(attribute.StringValue("handlePUTShard: append failed"))
	poisoned.AddAttributes(attribute.String("error", rawReason))
	logger.Emit(context.Background(), poisoned)

	var valid log.Record
	valid.SetBody(attribute.StringValue("healthy record"))
	valid.AddAttributes(attribute.String("bucket", "images"))
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
	cases := []attribute.Value{
		attribute.StringValue("plain ascii"),
		attribute.StringValue("valid utf-8: café €"),
		attribute.Int64Value(42),
		attribute.BoolValue(true),
		attribute.ByteSliceValue([]byte{0xff, 0xfe}), // raw bytes marshal as protobuf bytes, untouched
		attribute.SliceValue(attribute.StringValue("a"), attribute.Int64Value(1)),
		attribute.MapValue(attribute.String("k", "v"), attribute.Int64("n", 2)),
		attribute.StringSliceValue([]string{"one", "two"}),
	}
	for _, v := range cases {
		got, changed := sanitizeValue(v)
		if changed {
			t.Errorf("sanitizeValue(%v) reported changed, want unchanged", v)
		}
		if !reflect.DeepEqual(got, v) {
			t.Errorf("sanitizeValue(%v) = %v, want identical", v, got)
		}
	}
}

// TestSanitizeValueRecursesNested proves invalid bytes nested inside slices and
// maps are fixed, not just top-level string attributes.
func TestSanitizeValueRecursesNested(t *testing.T) {
	nested := attribute.MapValue(
		attribute.String("reason", rawReason),
		attribute.Slice("frames", attribute.StringValue("ok"), attribute.StringValue("bad\xffbyte")),
	)
	got, changed := sanitizeValue(nested)
	if !changed {
		t.Fatal("sanitizeValue reported unchanged for nested invalid UTF-8")
	}
	assertValueValid(t, got)
}

// TestSanitizeValueRecursesMapInSlice proves a map nested inside a slice is
// still walked, so invalid bytes two containers deep are replaced.
func TestSanitizeValueRecursesMapInSlice(t *testing.T) {
	nested := attribute.SliceValue(
		attribute.StringValue("ok"),
		attribute.MapValue(attribute.String("reason", rawReason)),
	)
	got, changed := sanitizeValue(nested)
	if !changed {
		t.Fatal("sanitizeValue reported unchanged for a map nested in a slice")
	}
	assertValueValid(t, got)

	inner := got.AsSlice()[1].AsMap()[0].Value.AsString()
	if !containsReplacement(inner) {
		t.Errorf("nested map value not sanitised: %q", inner)
	}
}

// TestSanitizeValueStringSlice covers attribute.STRINGSLICE, which the otelslog
// bridge emits for a []string attribute and which AsSlice does not decompose.
func TestSanitizeValueStringSlice(t *testing.T) {
	got, changed := sanitizeValue(attribute.StringSliceValue([]string{"ok", rawReason}))
	if !changed {
		t.Fatal("sanitizeValue reported unchanged for invalid UTF-8 in a string slice")
	}
	assertValueValid(t, got)

	elems := got.AsStringSlice()
	if len(elems) != 2 {
		t.Fatalf("got %d elements, want 2", len(elems))
	}
	if elems[0] != "ok" {
		t.Errorf("valid element mutated: %q, want ok", elems[0])
	}
	if !containsReplacement(elems[1]) {
		t.Errorf("expected replacement rune in sanitised element, got %q", elems[1])
	}
}

// TestUTF8ProcessorSanitizesStringSliceAttribute proves the STRINGSLICE path is
// reached through the processor, as a []string attribute arrives from the bridge.
func TestUTF8ProcessorSanitizesStringSliceAttribute(t *testing.T) {
	exp := &recordingLogExporter{}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(newUTF8Processor(sdklog.NewSimpleProcessor(exp))),
	)
	defer func() { _ = lp.Shutdown(context.Background()) }()

	var rec log.Record
	rec.SetBody(attribute.StringValue("shard put failed"))
	rec.AddAttributes(attribute.StringSlice("reasons", []string{"ok", rawReason}))
	lp.Logger("test").Emit(context.Background(), rec)

	records := exp.snapshot()
	if len(records) != 1 {
		t.Fatalf("got %d exported records, want 1", len(records))
	}
	assertAllStringsValid(t, records[0])
}

func attrString(rec sdklog.Record, key string) string {
	var out string
	rec.WalkAttributes(func(kv attribute.KeyValue) bool {
		if string(kv.Key) == key {
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
