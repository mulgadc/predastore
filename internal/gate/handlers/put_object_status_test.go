package handlers

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/blob/engine"
	"github.com/mulgadc/predastore/internal/gate/chunked"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/telemetry"
	"github.com/stretchr/testify/assert"
)

// TestMapPutErr covers how a failure during the shard write is reported. The
// body is decoded as the shards are written, so a client's framing or signature
// failure surfaces there and must not be answered as a server fault.
func TestMapPutErr(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		code   model.S3ErrorCode
		status int
		reason string
	}{
		{
			name:   "storage exhaustion",
			err:    fmt.Errorf("put shard: %w", engine.ErrStoreFull),
			code:   model.ErrInsufficientStorage,
			status: 507,
			reason: telemetry.WriteReasonStoreFull,
		},
		{
			name:   "broken chunk signature",
			err:    fmt.Errorf("read body: %w", chunked.ErrChunkSignature),
			code:   model.ErrSignatureDoesNotMatch,
			status: 403,
			reason: telemetry.WriteReasonBadRequest,
		},
		{
			name:   "malformed framing",
			err:    fmt.Errorf("read body: %w", chunked.ErrMalformedFraming),
			code:   model.ErrMalformedChunkedBody,
			status: 400,
			reason: telemetry.WriteReasonBadRequest,
		},
		{
			name:   "a genuine shard failure is still ours",
			err:    errors.New("node stalled"),
			code:   model.ErrInternalError,
			status: 500,
			reason: telemetry.WriteReasonShardWrite,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mapPutErr(tc.err)
			assert.Equal(t, tc.code, got.Code)
			assert.Equal(t, tc.status, got.StatusCode)
			assert.Equal(t, tc.reason, writeFailureReason(tc.err),
				"a client error must not be counted as a storage failure")
		})
	}
}

// TestDecoderErrorReachesTheClient walks the sequence PutObject runs, so the
// error the client would receive comes from the same call ordering rather than
// from mapPutErr in isolation.
func TestDecoderErrorReachesTheClient(t *testing.T) {
	const payload = "hello world"
	f := newWriteFixture(4, 2)

	// A body that ends before its terminating chunk. The write path reads only
	// the declared length, so this fails as the shards are written.
	truncated := fmt.Sprintf("%x\r\n%s", len(payload), payload)
	req := chunkedPutBody(t, truncated, payload, "", SignedPayload{
		Signed: true, Mode: sigv4.StreamingUnsignedTrailer,
	})

	body, size, dec := decodeBody(req)
	_, _, err := f.write(t.Context(), model.ObjectHash("b", "k"), body, size)
	if err == nil {
		// The declared length was satisfied before the truncation showed, so the
		// failure lands on the drain instead.
		err = finishPayload(req, dec)
	}

	if s3err, ok := model.IsS3Error(err); ok {
		assert.Equal(t, 400, s3err.StatusCode)
		return
	}
	got := mapPutErr(err)
	assert.Equal(t, model.ErrMalformedChunkedBody, got.Code)
	assert.Equal(t, 400, got.StatusCode,
		"a truncated body is the client's error, not a 500")
}

// TestTruncatedBodyIsNotACleanEnd is the decoder-level statement of the same
// fault: a body with no terminating chunk must not read as a complete one.
func TestTruncatedBodyIsNotACleanEnd(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"no terminating chunk", "5\r\nhello\r\n"},
		{"chunk shorter than its header", "b\r\nhello"},
		{"terminator with no trailer block", "5\r\nhello\r\n0\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dec := chunked.NewDecoder(strings.NewReader(tc.body), 0)
			_, err := io.ReadAll(dec)
			assert.ErrorIs(t, err, chunked.ErrMalformedFraming)
		})
	}
}
