package quicserver

import (
	"bufio"
	"encoding/json"
	"log/slog"

	"github.com/mulgadc/predastore/quic/quicproto"
)

// handleDELETEShard marks shard metadata in the store index as deleted so that it may be reclaimed by compaction.
func (qs *QuicServer) handleDELETEShard(bw *bufio.Writer, req quicproto.Header, delReq DeleteRequest) {
	deleted, err := qs.store.Delete(delReq.ObjectHash, delReq.ShardIndex)
	if err != nil {
		slog.Error("handleDELETEShard: delete failed", "bucket", delReq.Bucket, "object", delReq.Object, "shardIndex", delReq.ShardIndex, "error", err)
		qs.sendDeleteResponse(bw, req, false, err.Error())
		return
	}

	slog.Debug("handleDELETEShard: deleted shard",
		"bucket", delReq.Bucket,
		"object", delReq.Object,
		"shardIndex", delReq.ShardIndex,
		"deleted", deleted,
	)

	qs.sendDeleteResponse(bw, req, deleted, "")
}

func (qs *QuicServer) sendDeleteResponse(bw *bufio.Writer, req quicproto.Header, deleted bool, errMsg string) {
	response := DeleteResponse{
		Deleted: deleted,
		Error:   errMsg,
	}
	respBytes, _ := json.Marshal(response)

	rh := quicproto.Header{
		Version: quicproto.Version1,
		Method:  req.Method,
		Status:  quicproto.StatusOK,
		ReqID:   req.ReqID,
		KeyLen:  0,
		MetaLen: uint32(len(respBytes)), //nolint:gosec // G115: DeleteResponse JSON is bounded (tens of bytes).
		BodyLen: 0,
	}
	_ = quicproto.WriteHeader(bw, rh)
	_, _ = bw.Write(respBytes)
	_ = bw.Flush()
}
