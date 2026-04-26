package quicserver

import (
	"bufio"
	"encoding/json"
	"log/slog"

	"github.com/mulgadc/predastore/quic/quicproto"
	"github.com/mulgadc/predastore/utils"
)

// handleDELETEShard removes shard metadata from the store index. The on-disk
// extent becomes dead space reclaimable by a future compactor.
func (qs *QuicServer) handleDELETEShard(bw *bufio.Writer, req quicproto.Header, delReq DeleteRequest) {
	if err := qs.store.Delete(delReq.ObjectHash, utils.IntToUint32(delReq.ShardIndex)); err != nil {
		slog.Debug("handleDELETEShard: delete failed", "bucket", delReq.Bucket, "object", delReq.Object, "shardIndex", delReq.ShardIndex, "error", err)
	}

	slog.Debug("handleDELETEShard: deleted shard",
		"bucket", delReq.Bucket,
		"object", delReq.Object,
		"shardIndex", delReq.ShardIndex,
	)

	qs.sendDeleteResponse(bw, req, true, "")
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
		MetaLen: utils.IntToUint32(len(respBytes)),
		BodyLen: 0,
	}
	_ = quicproto.WriteHeader(bw, rh)
	_, _ = bw.Write(respBytes)
	_ = bw.Flush()
}
