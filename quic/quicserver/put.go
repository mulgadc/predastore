package quicserver

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/mulgadc/predastore/quic/quicproto"
	"github.com/mulgadc/predastore/store"
)

// handlePUTShard receives shard data via QUIC and writes it to the local store.
// Append reserves slots under a short lock, then the caller streams directly
// from the QUIC stream lock-free — no pre-buffer needed.
func (qs *QuicServer) handlePUTShard(br *bufio.Reader, bw *bufio.Writer, req quicproto.Header, putReq PutRequest) {
	// Determine how many bytes to read
	var bodyLen int64
	if req.BodyLen > 0 {
		bodyLen = int64(req.BodyLen) //nolint:gosec // G115: quicproto.ReadHeader rejects BodyLen > MaxInt64 with ErrBodyLenOverflow.
	} else if putReq.ShardSize > 0 {
		bodyLen = int64(putReq.ShardSize)
	} else {
		writeErr(bw, req, quicproto.StatusBadRequest, "no body length specified")
		return
	}

	writer, err := qs.store.Append(putReq.ObjectHash, putReq.ShardIndex, bodyLen)
	if err != nil {
		// Append rejected before consuming the request body. The client is
		// still streaming it, so drain (and discard) the body before replying:
		// returning without reading resets the un-drained QUIC stream mid-upload,
		// and the client sees a stream cancellation instead of our status code —
		// which loses the out-of-space signal entirely. Draining lets the client
		// finish its write and read the real status.
		if _, derr := io.Copy(io.Discard, io.LimitReader(br, bodyLen)); derr != nil {
			slog.Warn("handlePUTShard: draining body after append error failed", "error", derr)
		}
		// The pool free-space watermark tripped: surface a distinguishable
		// status so the client sees a real out-of-space error instead of a
		// generic server failure. Every other Append error stays 500.
		if errors.Is(err, store.ErrStoreFull) {
			slog.Warn("handlePUTShard: store full, rejecting write", "error", err)
			writeErr(bw, req, quicproto.StatusInsufficientStorage, fmt.Sprintf("append: %v", err))
			return
		}
		slog.Error("handlePUTShard: append failed", "error", err)
		writeErr(bw, req, quicproto.StatusServerError, fmt.Sprintf("append: %v", err))
		return
	}

	if _, err := writer.ReadFrom(io.LimitReader(br, bodyLen)); err != nil {
		slog.Error("handlePUTShard: write failed", "error", err)
		writeErr(bw, req, quicproto.StatusServerError, fmt.Sprintf("write: %v", err))
		return
	}

	if err := writer.Close(); err != nil {
		slog.Error("handlePUTShard: commit failed", "error", err)
		writeErr(bw, req, quicproto.StatusServerError, fmt.Sprintf("commit: %v", err))
		return
	}

	// Surface pool pressure on the success path so a client backing off early
	// (e.g. viperblock refusing new guest writes) learns about the nearfull
	// band before this node ever rejects a write outright.
	response := PutResponse{ShardSize: bodyLen, PoolNearFull: qs.store.NearFull()}
	respBytes, err := json.Marshal(response)
	if err != nil {
		writeErr(bw, req, quicproto.StatusServerError, "marshal response failed")
		return
	}

	rh := quicproto.Header{
		Version: quicproto.Version1,
		Method:  req.Method,
		Status:  quicproto.StatusOK,
		ReqID:   req.ReqID,
		KeyLen:  0,
		MetaLen: uint32(len(respBytes)), //nolint:gosec // G115: PutResponse JSON is bounded (tens of bytes).
		BodyLen: 0,
	}
	if err := quicproto.WriteHeader(bw, rh); err != nil {
		slog.Error("handlePUTShard: write header failed", "error", err)
		return
	}
	if _, err := bw.Write(respBytes); err != nil {
		slog.Error("handlePUTShard: write response failed", "error", err)
		return
	}
	if err := bw.Flush(); err != nil {
		slog.Error("handlePUTShard: flush failed", "error", err)
		return
	}
}
