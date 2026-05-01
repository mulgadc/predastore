package quicserver

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"

	"github.com/mulgadc/predastore/quic/quicproto"
	"github.com/mulgadc/predastore/utils"
)

// handlePUTShard receives shard data via QUIC and writes it to the local store.
// Append reserves slots under a short lock, then the caller streams directly
// from the QUIC stream lock-free — no pre-buffer needed.
func (qs *QuicServer) handlePUTShard(br *bufio.Reader, bw *bufio.Writer, req quicproto.Header, putReq PutRequest) {
	slog.Debug("handlePUTShard: starting",
		"bucket", putReq.Bucket,
		"shardIndex", putReq.ShardIndex,
		"shardSize", putReq.ShardSize,
		"bodyLen", req.BodyLen,
	)

	// Determine how many bytes to read
	var bodyLen int64
	if req.BodyLen > 0 {
		bodyLen = utils.Uint64ToInt64(req.BodyLen)
	} else if putReq.ShardSize > 0 {
		bodyLen = int64(putReq.ShardSize)
	} else {
		writeErr(bw, req, quicproto.StatusBadRequest, "no body length specified")
		return
	}

	writer, err := qs.store.Append(putReq.ObjectHash, utils.IntToUint32(putReq.ShardIndex), bodyLen)
	if err != nil {
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

	slog.Debug("handlePUTShard: stored shard",
		"bucket", putReq.Bucket,
		"object", putReq.Object,
		"shardIndex", putReq.ShardIndex,
		"size", bodyLen,
	)

	response := PutResponse{ShardSize: bodyLen}
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
		MetaLen: utils.IntToUint32(len(respBytes)),
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

	slog.Debug("handlePUTShard: response sent",
		"bucket", putReq.Bucket,
		"shardIndex", putReq.ShardIndex,
	)
}
