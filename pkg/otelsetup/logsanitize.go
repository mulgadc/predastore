package otelsetup

import (
	"context"
	"strings"
	"unicode/utf8"

	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// replacementRune substitutes each maximal run of invalid UTF-8 bytes.
const replacementRune = "�"

var _ sdklog.Processor = (*utf8Processor)(nil)

// utf8Processor coerces every string field of a log record (body, attribute
// keys, attribute values, recursively through slices and maps) to valid UTF-8
// before delegating to next. OTLP marshals these as protobuf `string` fields,
// which reject invalid UTF-8 and fail the whole export batch — dropping every
// co-batched record. Peer-supplied QUIC CONNECTION_CLOSE/RESET_STREAM reason
// phrases (not UTF-8 mandated) reach here via `err.Error()` on shard-put logs;
// sanitising at the processor defends the pipeline regardless of call site.
type utf8Processor struct {
	next sdklog.Processor
}

// newUTF8Processor wraps next so records are UTF-8-sanitised before it runs.
func newUTF8Processor(next sdklog.Processor) sdklog.Processor {
	return &utf8Processor{next: next}
}

func (p *utf8Processor) OnEmit(ctx context.Context, record *sdklog.Record) error {
	if v, changed := sanitizeValue(record.Body()); changed {
		record.SetBody(v)
	}

	var dirty bool
	record.WalkAttributes(func(kv log.KeyValue) bool {
		if !utf8.ValidString(kv.Key) {
			dirty = true
			return false
		}
		if _, changed := sanitizeValue(kv.Value); changed {
			dirty = true
			return false
		}
		return true
	})
	if dirty {
		attrs := make([]log.KeyValue, 0, record.AttributesLen())
		record.WalkAttributes(func(kv log.KeyValue) bool {
			v, _ := sanitizeValue(kv.Value)
			attrs = append(attrs, log.KeyValue{Key: sanitizeString(kv.Key), Value: v})
			return true
		})
		record.SetAttributes(attrs...)
	}

	return p.next.OnEmit(ctx, record)
}

func (p *utf8Processor) Enabled(ctx context.Context, param sdklog.EnabledParameters) bool {
	return p.next.Enabled(ctx, param)
}

func (p *utf8Processor) Shutdown(ctx context.Context) error {
	return p.next.Shutdown(ctx)
}

func (p *utf8Processor) ForceFlush(ctx context.Context) error {
	return p.next.ForceFlush(ctx)
}

// sanitizeString returns s with each run of invalid UTF-8 replaced. Valid
// strings are returned unchanged with no allocation (the common hot path).
func sanitizeString(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, replacementRune)
}

// sanitizeValue recurses through string, slice, and map values, returning the
// sanitised value and whether anything changed. Non-string scalars (bool, int,
// float, bytes) pass through untouched — bytes marshal as a protobuf `bytes`
// field, which permits arbitrary octets.
func sanitizeValue(v log.Value) (log.Value, bool) {
	switch v.Kind() {
	case log.KindString:
		s := v.AsString()
		if utf8.ValidString(s) {
			return v, false
		}
		return log.StringValue(strings.ToValidUTF8(s, replacementRune)), true
	case log.KindSlice:
		elems := v.AsSlice()
		var changed bool
		out := make([]log.Value, len(elems))
		for i, e := range elems {
			sv, c := sanitizeValue(e)
			out[i] = sv
			changed = changed || c
		}
		if !changed {
			return v, false
		}
		return log.SliceValue(out...), true
	case log.KindMap:
		kvs := v.AsMap()
		var changed bool
		out := make([]log.KeyValue, len(kvs))
		for i, kv := range kvs {
			sv, c := sanitizeValue(kv.Value)
			key := kv.Key
			if !utf8.ValidString(key) {
				key = strings.ToValidUTF8(key, replacementRune)
				c = true
			}
			out[i] = log.KeyValue{Key: key, Value: sv}
			changed = changed || c
		}
		if !changed {
			return v, false
		}
		return log.MapValue(out...), true
	default:
		return v, false
	}
}
