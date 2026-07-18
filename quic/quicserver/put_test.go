package quicserver

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/quic/quicproto"
	"github.com/mulgadc/predastore/store"
)

// handlePUTShard writes its response header + meta straight onto bw; decode
// it back to inspect the status the client would have seen.
func readPutResponse(t *testing.T, out *bytes.Buffer) quicproto.Header {
	t.Helper()
	hdr, err := quicproto.ReadHeader(bufio.NewReader(out))
	if err != nil {
		t.Fatalf("read response header: %v", err)
	}
	return hdr
}

// A store whose full watermark is set to reject everything (0.9999 free-space
// fraction, guaranteed above any real disk's free fraction) exercises the
// exact seam handlePUTShard uses to translate store.ErrStoreFull into a wire
// status, without mocking statfs.
func fullStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir(),
		store.WithAEAD(storetest.TestAEAD()),
		store.WithFreeSpaceWatermark(0.9999, 0.9999),
	)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestHandlePUTShardStoreFullReturnsInsufficientStorage(t *testing.T) {
	qs := &QuicServer{store: fullStore(t)}

	var out bytes.Buffer
	bw := bufio.NewWriter(&out)
	br := bufio.NewReader(bytes.NewReader(nil))

	req := quicproto.Header{Version: quicproto.Version1, Method: quicproto.MethodPUT, ReqID: 1, BodyLen: 4}
	putReq := PutRequest{ObjectHash: [32]byte{0x1}, ShardIndex: 0, ShardSize: 4}

	qs.handlePUTShard(br, bw, req, putReq)

	hdr := readPutResponse(t, &out)
	if hdr.Status != quicproto.StatusInsufficientStorage {
		t.Fatalf("response status = %d, want %d (StatusInsufficientStorage)", hdr.Status, quicproto.StatusInsufficientStorage)
	}
}

// A store that is not full must still surface an ordinary 500 for an
// unrelated Append failure — the 507 path must not swallow other errors.
// Closing the store makes Append fail with store.ErrClosedStore, which is
// deliberately NOT store.ErrStoreFull.
func TestHandlePUTShardOtherAppendErrorStaysServerError(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.WithAEAD(storetest.TestAEAD()))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	qs := &QuicServer{store: st}

	var out bytes.Buffer
	bw := bufio.NewWriter(&out)
	br := bufio.NewReader(bytes.NewReader(nil))

	req := quicproto.Header{Version: quicproto.Version1, Method: quicproto.MethodPUT, ReqID: 1, BodyLen: 4}
	putReq := PutRequest{ObjectHash: [32]byte{0x2}, ShardIndex: 0, ShardSize: 4}

	qs.handlePUTShard(br, bw, req, putReq)

	hdr := readPutResponse(t, &out)
	if hdr.Status != quicproto.StatusServerError {
		t.Fatalf("response status = %d, want %d (StatusServerError)", hdr.Status, quicproto.StatusServerError)
	}
}
