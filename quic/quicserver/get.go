package quicserver

import (
	"bufio"
	"io"
	"log/slog"

	"github.com/mulgadc/predastore/quic/quicproto"
	"github.com/mulgadc/predastore/s3db"
	"github.com/mulgadc/predastore/utils"
)

func (qs *QuicServer) handleGET(bw *bufio.Writer, req quicproto.Header, objectRequest ObjectRequest) {
	objectHash := s3db.GenObjectHash(objectRequest.Bucket, objectRequest.Object)

	reader, err := qs.store.Lookup(objectHash, utils.IntToUint32(objectRequest.ShardIndex))
	if err != nil {
		slog.Debug("handleGET: shard not found", "bucket", objectRequest.Bucket, "object", objectRequest.Object, "shardIndex", objectRequest.ShardIndex, "error", err)
		writeErr(bw, req, quicproto.StatusNotFound, "shard not found")
		return
	}
	defer reader.Close()

	totalSize := reader.Size()

	// Handle range request
	// Values >= 0 indicate explicit range bounds (including 0 for "start from beginning")
	// Value -1 (or < 0) means "not set" - use defaults (0 for start, totalSize-1 for end)
	rangeStart := objectRequest.RangeStart
	rangeEnd := objectRequest.RangeEnd
	isRangeRequest := rangeStart >= 0 || rangeEnd >= 0

	if rangeStart < 0 {
		rangeStart = 0
	}
	if rangeEnd < 0 || rangeEnd >= totalSize {
		rangeEnd = totalSize - 1
	}

	// Validate range
	if rangeStart > rangeEnd || rangeStart >= totalSize {
		writeErr(bw, req, quicproto.StatusBadRequest, "invalid range")
		return
	}

	responseSize := rangeEnd - rangeStart + 1

	// Send response header with body length
	rh := quicproto.Header{
		Version: quicproto.Version1,
		Method:  req.Method,
		Status:  quicproto.StatusOK,
		ReqID:   req.ReqID,
		KeyLen:  0,
		MetaLen: 0,
		BodyLen: utils.Int64ToUint64(responseSize),
	}
	if err := quicproto.WriteHeader(bw, rh); err != nil {
		slog.Error("handleGET: write header failed", "error", err)
		return
	}
	if err := bw.Flush(); err != nil {
		slog.Error("handleGET: flush header failed", "error", err)
		return
	}

	// Stream shard data from store
	var src io.Reader
	if isRangeRequest {
		src = io.NewSectionReader(reader, rangeStart, responseSize)
	} else {
		src = io.NewSectionReader(reader, 0, totalSize)
	}

	bytesWritten, err := io.Copy(bw, src)
	if err != nil {
		slog.Error("handleGET: streaming failed", "error", err)
		return
	}

	if isRangeRequest {
		slog.Debug("handleGET: range request completed",
			"bucket", objectRequest.Bucket,
			"object", objectRequest.Object,
			"rangeStart", rangeStart,
			"rangeEnd", rangeEnd,
			"bytesWritten", bytesWritten)
	} else {
		slog.Debug("handleGET: completed",
			"bucket", objectRequest.Bucket,
			"object", objectRequest.Object,
			"bytes", bytesWritten)
	}
}
