package quicserver

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mulgadc/predastore/quic/quicproto"
	"github.com/mulgadc/predastore/s3/wal"
	"github.com/mulgadc/predastore/s3db"
	"github.com/mulgadc/predastore/utils"
	quic "github.com/quic-go/quic-go"
)

const (
	alpn              = "mulga-repl-v1"
	maxKeyLen  uint32 = 4 * 1024
	maxMetaLen uint32 = 64 * 1024
)

// makeShardKey creates a unique key for storing a specific shard's metadata.
// This ensures that multiple shards of the same object can be stored on the same node
// without overwriting each other's metadata.
func makeShardKey(objectHash [32]byte, shardIndex int) []byte {
	// Append shard index as 4 bytes to the 32-byte hash
	key := make([]byte, 36)
	copy(key[:32], objectHash[:])
	binary.BigEndian.PutUint32(key[32:], utils.IntToUint32(shardIndex))
	return key
}

// QuicServer handles QUIC RPC requests for shard storage operations
type QuicServer struct {
	Addr   string
	WalDir string

	// Single WAL instance shared across all handlers
	// The WAL has internal mutex protection for concurrent access
	wal   *wal.WAL
	walMu sync.RWMutex // Additional mutex for WAL lifecycle operations

	// Listener for graceful shutdown
	listener *quic.Listener

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// Shutdown signaling
	shutdownCh chan struct{}
	shutdownMu sync.Mutex
	closed     bool

	// Active stream counter for debugging
	activeStreams int64
}

type ObjectRequest struct {
	Bucket     string `json:"bucket"`
	Object     string `json:"object"`
	Owner      string `json:"owner,omitempty"`
	RangeStart int64  `json:"range_start"` // -1 means from start (unset), >= 0 is actual offset
	RangeEnd   int64  `json:"range_end"`   // -1 means to end (unset), >= 0 is actual offset
	ShardIndex int    `json:"shard_index"` // Index of shard being requested (for multi-shard objects)
}

// PutRequest contains metadata for storing a shard via QUIC PUT
type PutRequest struct {
	Bucket     string   `json:"bucket"`
	Object     string   `json:"object"`
	ObjectHash [32]byte `json:"object_hash"` // SHA256 of bucket/object for metadata
	ShardSize  int      `json:"shard_size"`  // Expected size of the shard data
	ShardIndex int      `json:"shard_index"` // Index of this shard (0-based, for multi-shard objects)
}

// PutResponse contains the result of a QUIC PUT operation
type PutResponse struct {
	WriteResult wal.WriteResult `json:"write_result"`
	Error       string          `json:"error,omitempty"`
}

// DeleteRequest contains metadata for deleting a shard via QUIC DELETE
type DeleteRequest struct {
	Bucket     string   `json:"bucket"`
	Object     string   `json:"object"`
	ObjectHash [32]byte `json:"object_hash"` // SHA256 of bucket/object for metadata lookup
}

// DeleteResponse contains the result of a QUIC DELETE operation
type DeleteResponse struct {
	Deleted bool   `json:"deleted"`
	Error   string `json:"error,omitempty"`
}

// NewWithRetry creates and starts a new QUIC server with retry logic for port binding
// Returns the server instance and any error encountered
func NewWithRetry(walDir string, addr string, maxRetries int) (*QuicServer, error) {
	// Ensure WAL directory exists
	if err := os.MkdirAll(walDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory %s: %w", walDir, err)
	}

	// Open WAL once at startup - this holds the Badger DB lock
	walInstance, err := wal.New("", walDir)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL in %s: %w", walDir, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	qs := &QuicServer{
		WalDir:     walDir,
		Addr:       addr,
		wal:        walInstance,
		ctx:        ctx,
		cancel:     cancel,
		shutdownCh: make(chan struct{}),
	}

	tlsConf, err := makeServerTLSConfig()
	if err != nil {
		_ = walInstance.Close()
		return nil, fmt.Errorf("tls config: %w", err)
	}
	tlsConf.NextProtos = []string{alpn}

	// Retry port binding with exponential backoff
	var l *quic.Listener
	for i := 0; i < maxRetries; i++ {
		l, err = quic.ListenAddr(addr, tlsConf, &quic.Config{
			KeepAlivePeriod:       15 * time.Second,
			MaxIdleTimeout:        60 * time.Second,
			MaxIncomingStreams:    1000, // Allow more concurrent streams
			MaxIncomingUniStreams: 1000,
		})
		if err == nil {
			break
		}
		if i < maxRetries-1 {
			time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
		}
	}
	if err != nil {
		_ = walInstance.Close()
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}

	qs.listener = l
	log.Printf("QUIC RPC server listening on %s (ALPN %q, WAL: %s)", addr, alpn, walDir)

	// Start accept loop in goroutine
	go qs.acceptLoop()

	return qs, nil
}

// New creates and starts a new QUIC server for shard operations
// The WAL is opened once and shared across all request handlers
// Returns the server instance for graceful shutdown control
// Panics on error - use NewWithRetry for error handling
func New(walDir string, addr string) *QuicServer {
	qs, err := NewWithRetry(walDir, addr, 10)
	if err != nil {
		log.Fatalf("failed to start QUIC server: %v", err)
	}
	return qs
}

// acceptLoop handles incoming connections until shutdown
func (qs *QuicServer) acceptLoop() {
	for {
		// Use cancellable context for Accept
		conn, err := qs.listener.Accept(qs.ctx)
		if err != nil {
			// Check if we're shutting down
			select {
			case <-qs.shutdownCh:
				return
			default:
				// Only log if not a context cancellation
				if qs.ctx.Err() != nil {
					return
				}
				slog.Debug("accept conn error", "error", err)
				continue
			}
		}
		go qs.serveConn(conn)
	}
}

// Close gracefully shuts down the QUIC server and releases the WAL lock
func (qs *QuicServer) Close() error {
	qs.shutdownMu.Lock()
	if qs.closed {
		qs.shutdownMu.Unlock()
		return nil
	}
	qs.closed = true

	// Cancel context first to interrupt Accept call
	if qs.cancel != nil {
		qs.cancel()
	}

	close(qs.shutdownCh)
	qs.shutdownMu.Unlock()

	// Close listener to stop accepting new connections
	if qs.listener != nil {
		_ = qs.listener.Close()
	}

	// Give a brief moment for the accept loop to exit
	time.Sleep(10 * time.Millisecond)

	// Close WAL to release Badger lock
	qs.walMu.Lock()
	defer qs.walMu.Unlock()
	if qs.wal != nil {
		if err := qs.wal.Close(); err != nil {
			return fmt.Errorf("failed to close WAL: %w", err)
		}
		qs.wal = nil
	}

	slog.Info("QUIC server shut down", "addr", qs.Addr)
	return nil
}

func (qs *QuicServer) serveConn(conn *quic.Conn) {
	defer conn.CloseWithError(0, "bye")
	slog.Debug("QUIC connection from", "remote", conn.RemoteAddr())

	for {
		s, err := conn.AcceptStream(context.Background())
		if err != nil {
			slog.Debug("accept stream error", "error", err)
			return
		}
		go qs.handleStream(s)
	}
}

func (qs *QuicServer) handleStream(s *quic.Stream) {
	activeCount := atomic.AddInt64(&qs.activeStreams, 1)
	streamID := s.StreamID()
	slog.Debug("handleStream: started", "streamID", streamID, "activeStreams", activeCount)

	defer func() {
		// Close both sides of the stream to fully release it.
		// By the time this runs, bw.Flush() has already executed (defer LIFO order),
		// so the response has been sent. It's now safe to close both sides:
		// - CancelRead(0): close read side (we've already read all request data)
		// - Close(): close write side (sends FIN to client)
		s.CancelRead(0)
		if err := s.Close(); err != nil {
			slog.Debug("handleStream: close error", "streamID", streamID, "error", err)
		}
		finalCount := atomic.AddInt64(&qs.activeStreams, -1)
		slog.Debug("handleStream: closed", "streamID", streamID, "activeStreams", finalCount)
	}()

	if activeCount%100 == 0 || activeCount > 50 {
		slog.Info("handleStream: active streams high", "count", activeCount, "streamID", streamID)
	}

	br := bufio.NewReaderSize(s, 128*1024)
	bw := bufio.NewWriterSize(s, 128*1024)
	defer bw.Flush()

	reqHdr, err := quicproto.ReadHeader(br)
	if err != nil {
		slog.Debug("handleStream: read header failed", "error", err)
		return
	}

	requestBytes, err := quicproto.ReadExactBytes(br, reqHdr.KeyLen, maxKeyLen)
	if err != nil {
		writeErr(bw, reqHdr, quicproto.StatusBadRequest, "bad key")
		return
	}

	switch reqHdr.Method {
	case quicproto.MethodSTATUS:
		qs.handleSTATUS(bw, reqHdr)
	case quicproto.MethodGET:
		var objectRequest ObjectRequest
		if err := json.Unmarshal(requestBytes, &objectRequest); err != nil {
			writeErr(bw, reqHdr, quicproto.StatusBadRequest, "bad object request")
			return
		}
		qs.handleGET(bw, reqHdr, objectRequest)
	case quicproto.MethodPUT:
		var putRequest PutRequest
		if err := json.Unmarshal(requestBytes, &putRequest); err != nil {
			writeErr(bw, reqHdr, quicproto.StatusBadRequest, "bad put request")
			return
		}
		qs.handlePUTShard(br, bw, reqHdr, putRequest)
	case quicproto.MethodDELETE:
		var deleteRequest DeleteRequest
		if err := json.Unmarshal(requestBytes, &deleteRequest); err != nil {
			writeErr(bw, reqHdr, quicproto.StatusBadRequest, "bad delete request")
			return
		}
		qs.handleDELETEShard(bw, reqHdr, deleteRequest)
	default:
		writeErr(bw, reqHdr, quicproto.StatusBadRequest, "unknown method")
	}
}

func (qs *QuicServer) handleSTATUS(bw *bufio.Writer, req quicproto.Header) {
	resp := map[string]any{
		"ok":         true,
		"ts_unix_ms": time.Now().UnixMilli(),
		"node":       hostname(),
		"version":    "v1",
		"wal_dir":    qs.WalDir,
	}
	b, _ := json.Marshal(resp)

	rh := quicproto.Header{
		Version: quicproto.Version1,
		Method:  req.Method,
		Status:  quicproto.StatusOK,
		ReqID:   req.ReqID,
		KeyLen:  0,
		MetaLen: utils.IntToUint32(len(b)),
		BodyLen: 0,
	}
	_ = quicproto.WriteHeader(bw, rh)
	_, _ = bw.Write(b)
	_ = bw.Flush()
}

func (qs *QuicServer) handleGET(bw *bufio.Writer, req quicproto.Header, objectRequest ObjectRequest) {
	// Use shared WAL instance with read lock
	qs.walMu.RLock()
	walInstance := qs.wal
	qs.walMu.RUnlock()

	if walInstance == nil {
		writeErr(bw, req, quicproto.StatusServerError, "WAL not initialized")
		return
	}

	objectHash := s3db.GenObjectHash(objectRequest.Bucket, objectRequest.Object)

	// Query local node for shard location using shard-specific key
	shardKey := makeShardKey(objectHash, objectRequest.ShardIndex)
	result, err := walInstance.DB.Get(shardKey)
	if err != nil {
		slog.Debug("handleGET: shard not found", "bucket", objectRequest.Bucket, "object", objectRequest.Object, "shardIndex", objectRequest.ShardIndex)
		writeErr(bw, req, quicproto.StatusNotFound, "shard not found")
		return
	}

	var objectWriteResult wal.ObjectWriteResult
	if err := gob.NewDecoder(bytes.NewReader(result)).Decode(&objectWriteResult); err != nil {
		slog.Error("handleGET: failed to decode metadata", "error", err)
		writeErr(bw, req, quicproto.StatusServerError, "corrupt metadata")
		return
	}

	totalSize := objectWriteResult.WriteResult.TotalSize

	// Handle range request
	// Values >= 0 indicate explicit range bounds (including 0 for "start from beginning")
	// Value -1 (or < 0) means "not set" - use defaults (0 for start, totalSize-1 for end)
	rangeStart := objectRequest.RangeStart
	rangeEnd := objectRequest.RangeEnd
	isRangeRequest := rangeStart >= 0 || rangeEnd >= 0

	if rangeStart < 0 {
		rangeStart = 0
	}
	if rangeEnd < 0 || rangeEnd >= int64(totalSize) {
		rangeEnd = int64(totalSize) - 1
	}

	// Validate range
	if rangeStart > rangeEnd || rangeStart >= int64(totalSize) {
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

	// Stream shard data from WAL files
	var globalBytePos int64 // Current position in the shard data (0-indexed)
	var bytesWritten int64
	headerBuf := make([]byte, wal.FragmentHeaderBytes)
	fullChunkBuffer := make([]byte, int(walInstance.Shard.ChunkSize))
	headerForChecksum := make([]byte, wal.FragmentHeaderBytes)

	for _, walFile := range objectWriteResult.WriteResult.WALFiles {
		if walFile.Size < 0 {
			slog.Error("handleGET: invalid WAL file size", "size", walFile.Size, "walNum", walFile.WALNum)
			return
		}

		f, err := os.Open(filepath.Join(qs.WalDir, wal.FormatWalFile(walFile.WALNum)))
		if err != nil {
			slog.Error("handleGET: failed to open WAL file", "walNum", walFile.WALNum, "error", err)
			return
		}

		err = func() error {
			defer f.Close()

			walHeaderSize := int64(walInstance.WALHeaderSize())
			if _, err := f.Seek(walHeaderSize+walFile.Offset, io.SeekStart); err != nil {
				return fmt.Errorf("seek failed: %w", err)
			}

			var fileBytesRead int64
			for fileBytesRead < walFile.Size {
				if _, err := io.ReadFull(f, headerBuf); err != nil {
					return fmt.Errorf("read header failed: %w", err)
				}

				shardNum := binary.BigEndian.Uint64(headerBuf[8:16])
				shardFragment := binary.BigEndian.Uint32(headerBuf[16:20])
				length := binary.BigEndian.Uint32(headerBuf[20:24])
				flags := wal.Flags(binary.BigEndian.Uint32(headerBuf[24:28]))
				checksum := binary.BigEndian.Uint32(headerBuf[28:32])

				if shardNum != objectWriteResult.WriteResult.ShardNum {
					return fmt.Errorf("shard num mismatch: expected %d, got %d",
						objectWriteResult.WriteResult.ShardNum, shardNum)
				}
				if length > walInstance.Shard.ChunkSize {
					return fmt.Errorf("chunk length %d exceeds max %d", length, walInstance.Shard.ChunkSize)
				}

				if _, err := io.ReadFull(f, fullChunkBuffer); err != nil {
					return fmt.Errorf("read chunk failed: %w", err)
				}

				// Validate checksum
				copy(headerForChecksum, headerBuf)
				headerForChecksum[28], headerForChecksum[29], headerForChecksum[30], headerForChecksum[31] = 0, 0, 0, 0
				calculated := crc32.ChecksumIEEE(headerForChecksum)
				calculated = crc32.Update(calculated, crc32.IEEETable, fullChunkBuffer)
				if calculated != checksum {
					return fmt.Errorf("checksum mismatch for fragment %d: expected %d, got %d",
						shardFragment, checksum, calculated)
				}

				// Determine actual data length in this chunk
				chunkDataLen := int64(length)
				remaining := int64(totalSize) - globalBytePos
				if chunkDataLen > remaining {
					chunkDataLen = remaining
				}

				// Calculate overlap with requested range
				chunkStart := globalBytePos
				chunkEnd := globalBytePos + chunkDataLen - 1

				// Check if this chunk overlaps with the requested range
				if chunkEnd >= rangeStart && chunkStart <= rangeEnd {
					// Calculate the portion of this chunk to write
					writeStart := int64(0)
					if rangeStart > chunkStart {
						writeStart = rangeStart - chunkStart
					}
					writeEnd := chunkDataLen
					if rangeEnd < chunkEnd {
						writeEnd = rangeEnd - chunkStart + 1
					}

					toWrite := writeEnd - writeStart
					if toWrite > 0 && bytesWritten < responseSize {
						// Don't write more than requested
						if bytesWritten+toWrite > responseSize {
							toWrite = responseSize - bytesWritten
						}
						if _, err := bw.Write(fullChunkBuffer[writeStart : writeStart+toWrite]); err != nil {
							return fmt.Errorf("write to client failed: %w", err)
						}
						bytesWritten += toWrite
					}
				}

				globalBytePos += chunkDataLen
				fileBytesRead += int64(wal.FragmentHeaderBytes) + int64(walInstance.Shard.ChunkSize)

				// Stop if we've written all requested bytes
				if bytesWritten >= responseSize {
					return nil
				}

				if flags&wal.FlagEndOfShard != 0 {
					return nil
				}
			}
			return nil
		}()

		if err != nil {
			slog.Error("handleGET: streaming failed", "error", err)
			return
		}

		// Stop if we've written all requested bytes
		if bytesWritten >= responseSize {
			break
		}
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

// handlePUTShard receives shard data via QUIC and writes it to the local WAL
func (qs *QuicServer) handlePUTShard(br *bufio.Reader, bw *bufio.Writer, req quicproto.Header, putReq PutRequest) {
	slog.Debug("handlePUTShard: starting",
		"bucket", putReq.Bucket,
		"shardIndex", putReq.ShardIndex,
		"shardSize", putReq.ShardSize,
		"bodyLen", req.BodyLen,
	)

	// Use shared WAL instance
	qs.walMu.RLock()
	walInstance := qs.wal
	qs.walMu.RUnlock()

	if walInstance == nil {
		writeErr(bw, req, quicproto.StatusServerError, "WAL not initialized")
		return
	}

	// Determine how many bytes to read
	var bodyLen int
	if req.BodyLen > 0 {
		bodyLen = utils.Uint64ToInt(req.BodyLen)
	} else if putReq.ShardSize > 0 {
		bodyLen = putReq.ShardSize
	} else {
		writeErr(bw, req, quicproto.StatusBadRequest, "no body length specified")
		return
	}

	// Create a limited reader for the shard data
	shardReader := io.LimitReader(br, int64(bodyLen))

	slog.Debug("handlePUTShard: writing to WAL", "bodyLen", bodyLen)
	// Write to WAL (WAL has internal mutex for write serialization)
	writeResult, err := walInstance.Write(shardReader, bodyLen)
	if err != nil {
		slog.Error("handlePUTShard: WAL write failed", "error", err)
		writeErr(bw, req, quicproto.StatusServerError, fmt.Sprintf("wal write: %v", err))
		return
	}
	slog.Debug("handlePUTShard: WAL write completed", "shardNum", writeResult.ShardNum)

	// Store metadata in local Badger DB using shard-specific key
	// This ensures multiple shards of the same object can be stored on the same node
	shardKey := makeShardKey(putReq.ObjectHash, putReq.ShardIndex)
	err = walInstance.UpdateShardToWAL(shardKey, putReq.ObjectHash, writeResult)
	if err != nil {
		slog.Error("handlePUTShard: failed to update metadata", "error", err)
		writeErr(bw, req, quicproto.StatusServerError, fmt.Sprintf("metadata update: %v", err))
		return
	}

	slog.Debug("handlePUTShard: stored shard",
		"bucket", putReq.Bucket,
		"object", putReq.Object,
		"shardIndex", putReq.ShardIndex,
		"shardNum", writeResult.ShardNum,
		"totalSize", writeResult.TotalSize,
	)

	// Build and send response
	response := PutResponse{
		WriteResult: *writeResult,
	}
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

// deletedShardPrefix is the key prefix for tracking deleted shards in local badger
const deletedShardPrefix = "deleted:"

// DeletedShardInfo tracks a deleted shard for future WAL compaction
type DeletedShardInfo struct {
	ObjectHash  [32]byte        `json:"object_hash"`
	Bucket      string          `json:"bucket"`
	Object      string          `json:"object"`
	DeletedAt   int64           `json:"deleted_at"`
	WriteResult wal.WriteResult `json:"write_result"`
}

// handleDELETEShard removes shard metadata from local badger and logs deletion for WAL compaction
func (qs *QuicServer) handleDELETEShard(bw *bufio.Writer, req quicproto.Header, delReq DeleteRequest) {
	// Use shared WAL instance
	qs.walMu.RLock()
	walInstance := qs.wal
	qs.walMu.RUnlock()

	if walInstance == nil {
		writeErr(bw, req, quicproto.StatusServerError, "WAL not initialized")
		return
	}

	// Check if shard exists locally
	existingMeta, err := walInstance.DB.Get(delReq.ObjectHash[:])
	if err != nil {
		slog.Debug("handleDELETEShard: object not found locally", "bucket", delReq.Bucket, "object", delReq.Object)
		// Return success - idempotent delete
		qs.sendDeleteResponse(bw, req, true, "")
		return
	}

	// Decode existing write result for compaction tracking
	var objectWriteResult wal.ObjectWriteResult
	if err := gob.NewDecoder(bytes.NewReader(existingMeta)).Decode(&objectWriteResult); err != nil {
		slog.Error("handleDELETEShard: failed to decode metadata", "error", err)
		writeErr(bw, req, quicproto.StatusServerError, "corrupt metadata")
		return
	}

	// Log deletion for future WAL compaction
	deletedInfo := DeletedShardInfo{
		ObjectHash:  delReq.ObjectHash,
		Bucket:      delReq.Bucket,
		Object:      delReq.Object,
		DeletedAt:   time.Now().Unix(),
		WriteResult: objectWriteResult.WriteResult,
	}

	var deletedBuf bytes.Buffer
	if err := gob.NewEncoder(&deletedBuf).Encode(deletedInfo); err != nil {
		slog.Error("handleDELETEShard: failed to encode deletion info", "error", err)
		writeErr(bw, req, quicproto.StatusServerError, "encode error")
		return
	}

	// Store deletion record
	deletedKey := []byte(deletedShardPrefix + string(delReq.ObjectHash[:]))
	if err := walInstance.DB.Set(deletedKey, deletedBuf.Bytes()); err != nil {
		slog.Error("handleDELETEShard: failed to store deletion record", "error", err)
		writeErr(bw, req, quicproto.StatusServerError, "store deletion failed")
		return
	}

	// Delete object metadata
	if err := walInstance.DB.Delete(delReq.ObjectHash[:]); err != nil {
		slog.Error("handleDELETEShard: failed to delete metadata", "error", err)
		writeErr(bw, req, quicproto.StatusServerError, "delete failed")
		return
	}

	slog.Debug("handleDELETEShard: deleted shard",
		"bucket", delReq.Bucket,
		"object", delReq.Object,
		"walFiles", len(objectWriteResult.WriteResult.WALFiles),
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

func writeErr(bw *bufio.Writer, req quicproto.Header, code uint16, msg string) {
	meta := fmt.Sprintf(`{"error":%q}`, msg)
	rh := quicproto.Header{
		Version: quicproto.Version1,
		Method:  req.Method,
		Status:  code,
		ReqID:   req.ReqID,
		MetaLen: utils.IntToUint32(len(meta)),
		BodyLen: 0,
	}
	_ = quicproto.WriteHeader(bw, rh)
	_, _ = bw.WriteString(meta)
	_ = bw.Flush()
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}

func makeServerTLSConfig() (*tls.Config, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,

		DNSNames: []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
}
