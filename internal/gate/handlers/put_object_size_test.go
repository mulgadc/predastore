package handlers

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tripwireBody fails every read and counts the attempts, so a test can tell
// whether the handler started consuming a body without anyone sending 5 GiB.
type tripwireBody struct{ reads atomic.Int64 }

var errTripwire = errors.New("tripwire body read")

func (b *tripwireBody) Read([]byte) (int, error) {
	b.reads.Add(1)
	return 0, errTripwire
}

func (b *tripwireBody) Close() error { return nil }

// sizedPut is a PUT whose declared length is decoupled from its body. A
// negative length leaves the size undeclared.
func sizedPut(body *tripwireBody, contentLength int64) *http.Request {
	r := httptest.NewRequest(http.MethodPut, "/"+testBucket+"/k", nil)
	r.Body = body
	r.ContentLength = contentLength

	return r.WithContext(WithObject(r.Context(), model.Object{
		Bucket: model.Bucket{Name: testBucket}, Key: "k",
	}))
}

// framedPut is an unsigned aws-chunked PUT. An empty decoded length leaves the
// object size undeclared, which only the framing's Content-Length would hint at.
func framedPut(body *tripwireBody, decodedLength string) *http.Request {
	r := sizedPut(body, 1<<20)
	r.Header.Set("Content-Encoding", "aws-chunked")
	if decodedLength != "" {
		r.Header.Set("X-Amz-Decoded-Content-Length", decodedLength)
	}
	return r
}

func TestPutObjectSizeLimit(t *testing.T) {
	t.Parallel()

	overLimit := strconv.FormatInt(model.MaxPutObjectSize+1, 10)
	tests := []struct {
		name   string
		req    func(*tripwireBody) *http.Request
		status int
		code   string
	}{
		{
			name:   "declared length one byte over the limit",
			req:    func(b *tripwireBody) *http.Request { return sizedPut(b, model.MaxPutObjectSize+1) },
			status: http.StatusBadRequest,
			code:   string(model.ErrEntityTooLarge),
		},
		{
			name:   "decoded length one byte over the limit",
			req:    func(b *tripwireBody) *http.Request { return framedPut(b, overLimit) },
			status: http.StatusBadRequest,
			code:   string(model.ErrEntityTooLarge),
		},
		{
			name:   "plain body of undeclared length",
			req:    func(b *tripwireBody) *http.Request { return sizedPut(b, -1) },
			status: http.StatusLengthRequired,
			code:   string(model.ErrMissingContentLength),
		},
		{
			name:   "framed body of undeclared decoded length",
			req:    func(b *tripwireBody) *http.Request { return framedPut(b, "") },
			status: http.StatusLengthRequired,
			code:   string(model.ErrMissingContentLength),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newWriteFixture(2, 1)
			body := &tripwireBody{}
			w := httptest.NewRecorder()

			PutObject(f.mc, f.bc, f.ring, testCache(), f.cfg).ServeHTTP(w, tt.req(body))

			require.Equal(t, tt.status, w.Code, "body: %s", w.Body.String())
			assert.Equal(t, tt.code, s3ErrorCode(t, w))
			assert.Zero(t, body.reads.Load(), "the refusal must come before the body is read")
			assert.Zero(t, f.bc.putCalls.Load(), "a refused write must not prepare a shard")
		})
	}
}

// Exactly 5 GiB is the largest single PUT S3 accepts, so it has to reach the
// write path rather than be refused up front.
func TestPutObjectAdmitsTheLimitItself(t *testing.T) {
	t.Parallel()

	f := newWriteFixture(2, 1)
	body := &tripwireBody{}
	w := httptest.NewRecorder()

	PutObject(f.mc, f.bc, f.ring, testCache(), f.cfg).
		ServeHTTP(w, sizedPut(body, model.MaxPutObjectSize))

	assert.NotEqual(t, string(model.ErrEntityTooLarge), s3ErrorCode(t, w))
	assert.Positive(t, body.reads.Load(), "a body at the limit must be admitted to the write path")
}
