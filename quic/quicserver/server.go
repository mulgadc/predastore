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
	"time"

	"github.com/mulgadc/predastore/quic/quicproto"
	"github.com/mulgadc/predastore/s3/wal"
	"github.com/mulgadc/predastore/s3db"
	quic "github.com/quic-go/quic-go"
)

const (
	alpn               = "mulga-repl-v1"
	addr               = "0.0.0.0:7443"
	maxKeyLen   uint32 = 4 * 1024
	maxMetaLen  uint32 = 64 * 1024
	storageRoot        = "./data" // demo backing store
)

type QuicServer struct {
	Addr   string
	WalDir string
}

type ObjectRequest struct {
	Bucket string
	Object string
	Owner  string
}

// PutRequest contains metadata for storing a shard via QUIC PUT
type PutRequest struct {
	Bucket     string   `json:"bucket"`
	Object     string   `json:"object"`
	ObjectHash [32]byte `json:"object_hash"` // SHA256 of bucket/object for metadata
	ShardSize  int      `json:"shard_size"`  // Expected size of the shard data
}

// PutResponse contains the result of a QUIC PUT operation
type PutResponse struct {
	WriteResult wal.WriteResult `json:"write_result"`
	Error       string          `json:"error,omitempty"`
}

func New(walDir string, addr string) {

	qs := &QuicServer{
		WalDir: walDir,
		Addr:   addr,
	}

	_ = os.MkdirAll(storageRoot, 0o755)

	tlsConf, err := makeServerTLSConfig()
	if err != nil {
		log.Fatalf("tls: %v", err)
	}
	tlsConf.NextProtos = []string{alpn}

	l, err := quic.ListenAddr(addr, tlsConf, &quic.Config{
		KeepAlivePeriod: 15 * time.Second,
		MaxIdleTimeout:  60 * time.Second,
	})
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("QUIC RPC server listening on %s (ALPN %q)", addr, alpn)

	for {
		conn, err := l.Accept(context.Background())
		if err != nil {
			log.Printf("accept conn: %v", err)
			continue
		}
		go qs.serveConn(conn)
	}

}

func (qs *QuicServer) serveConn(conn *quic.Conn) {
	defer conn.CloseWithError(0, "bye")
	log.Printf("conn from %s", conn.RemoteAddr())

	for {

		fmt.Println("accept stream")
		s, err := conn.AcceptStream(context.Background())
		if err != nil {
			log.Printf("accept stream: %v", err)
			return
		}
		go qs.handleStream(s)
	}
}

func (qs *QuicServer) handleStream(s *quic.Stream) {
	defer s.Close()

	br := bufio.NewReaderSize(s, 128*1024)
	bw := bufio.NewWriterSize(s, 128*1024)
	defer bw.Flush()

	reqHdr, err := quicproto.ReadHeader(br)
	if err != nil {
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
		qs.handleGET(br, bw, reqHdr, objectRequest)
	case quicproto.MethodPUT:
		var putRequest PutRequest
		if err := json.Unmarshal(requestBytes, &putRequest); err != nil {
			writeErr(bw, reqHdr, quicproto.StatusBadRequest, "bad put request")
			return
		}
		qs.handlePUTShard(br, bw, reqHdr, putRequest)
	default:
		writeErr(bw, reqHdr, quicproto.StatusBadRequest, "unknown method")
	}
}

func (qs *QuicServer) handleSTATUS(bw *bufio.Writer, req quicproto.Header) {
	resp := map[string]any{
		"ok":         true,
		"ts_unix_ms": time.Now().UnixMilli(),
		"node":       hostname(),
		"version":    "v1-demo",
	}
	b, _ := json.Marshal(resp)

	rh := quicproto.Header{
		Version: quicproto.Version1,
		Method:  req.Method,
		Status:  quicproto.StatusOK,
		ReqID:   req.ReqID,
		KeyLen:  0,
		MetaLen: uint32(len(b)),
		BodyLen: 0,
	}
	_ = quicproto.WriteHeader(bw, rh)
	_, _ = bw.Write(b)
	_ = bw.Flush()
}

func (qs *QuicServer) handleGET(br *bufio.Reader, bw *bufio.Writer, req quicproto.Header, objectRequest ObjectRequest) {
	// Key is treated like "bucket/object". Map to a safe path.

	// Catch errors with defer, write QUIC wire message and return
	var pipeErr error
	defer func() {
		if pipeErr != nil {
			slog.Error("handleGET", "error", pipeErr.Error())
			writeErr(bw, req, quicproto.StatusServerError, pipeErr.Error())
			return
		}
	}()

	// First, determine the shard number and WAL file number
	//nodeDir := backend.nodeDir(fmt.Sprintf("node-%d", totalNodes[i]))

	walInstance, pipeErr := wal.New("", qs.WalDir)
	fmt.Println("wal", qs.WalDir)

	if pipeErr != nil {
		return
	}

	defer walInstance.Close()

	//	fmt.Println(objectRequest.Bucket, objectRequest.Object)

	objectHash := s3db.GenObjectHash(objectRequest.Bucket, objectRequest.Object)

	// Query local node, where does the shard belong?
	result, pipeErr := walInstance.DB.Get(objectHash[:])

	//	fmt.Println("result")
	//	spew.Dump(result)

	if pipeErr != nil {
		return
	}

	var objectWriteResult wal.ObjectWriteResult

	r := bytes.NewReader(result)
	dec := gob.NewDecoder(r)
	if pipeErr := dec.Decode(&objectWriteResult); pipeErr != nil {
		return
	}

	fmt.Println("objectWriteResult")

	//spew.Dump(objectWriteResult)

	// TODO: Need to improve error handling, success header sent, however writeErr will attempt to set headers
	rh := quicproto.Header{
		Version: quicproto.Version1,
		Method:  req.Method,
		Status:  quicproto.StatusOK,
		ReqID:   req.ReqID,
		KeyLen:  0,
		MetaLen: 0,
		BodyLen: uint64(objectWriteResult.WriteResult.TotalSize),
	}
	if pipeErr := quicproto.WriteHeader(bw, rh); pipeErr != nil {
		return
	}
	// Flush the response header immediately so the client can start reading.
	if pipeErr := bw.Flush(); pipeErr != nil {
		return
	}

	var bytesWritten int

	// Reuse buffers to avoid per-fragment allocations.
	headerBuf := make([]byte, wal.FragmentHeaderBytes)
	fullChunkBuffer := make([]byte, int(walInstance.Shard.ChunkSize))
	headerForChecksum := make([]byte, wal.FragmentHeaderBytes)

	for _, walFile := range objectWriteResult.WriteResult.WALFiles {

		slog.Info("handleGET", "walFile", walFile.WALNum, "size", walFile.Size, "offset", walFile.Offset)

		if walFile.Size < 0 {
			pipeErr = fmt.Errorf("invalid WALFileInfo.Size %d for WAL %d", walFile.Size, walFile.WALNum)
			return
		}

		f, err := os.Open(filepath.Join(qs.WalDir, wal.FormatWalFile(walFile.WALNum)))
		if err != nil {
			pipeErr = fmt.Errorf("failed to open WAL file %d: %w", walFile.WALNum, err)
			return
		}

		// Ensure file is closed before moving to the next WAL segment.
		func() {
			defer f.Close()

			// Skip WAL header and seek to this object's start offset (Offset excludes WAL header).
			walHeaderSize := int64(walInstance.WALHeaderSize())
			if _, err := f.Seek(walHeaderSize+walFile.Offset, io.SeekStart); err != nil {
				pipeErr = fmt.Errorf("failed to seek in WAL %d: %w", walFile.WALNum, err)
				return
			}

			var fileBytesRead int64
			for fileBytesRead < walFile.Size {
				// Read fixed-size fragment header (32 bytes).
				if _, err := io.ReadFull(f, headerBuf); err != nil {
					pipeErr = fmt.Errorf("could not read fragment header from WAL %d: %w", walFile.WALNum, err)
					return
				}

				seqNum := binary.BigEndian.Uint64(headerBuf[0:8])
				shardNum := binary.BigEndian.Uint64(headerBuf[8:16])
				shardFragment := binary.BigEndian.Uint32(headerBuf[16:20])
				length := binary.BigEndian.Uint32(headerBuf[20:24])
				flags := wal.Flags(binary.BigEndian.Uint32(headerBuf[24:28]))
				checksum := binary.BigEndian.Uint32(headerBuf[28:32])

				_ = seqNum // currently unused, but kept for symmetry/debuggability

				// Sanity checks (same intent as ReadFromWriteResult).
				if shardNum != objectWriteResult.WriteResult.ShardNum {
					pipeErr = fmt.Errorf(
						"shard num mismatch in WAL %d: expected %d, got %d",
						walFile.WALNum, objectWriteResult.WriteResult.ShardNum, shardNum,
					)
					return
				}
				if length > walInstance.Shard.ChunkSize {
					pipeErr = fmt.Errorf(
						"chunk length %d exceeds max %d in WAL %d",
						length, walInstance.Shard.ChunkSize, walFile.WALNum,
					)
					return
				}

				// Read full on-disk payload (fixed ChunkSize).
				if _, err := io.ReadFull(f, fullChunkBuffer); err != nil {
					pipeErr = fmt.Errorf("could not read chunk from WAL %d: %w", walFile.WALNum, err)
					return
				}

				// Validate checksum (same method as ReadFromWriteResult):
				// CRC(header-with-checksum-zeroed + full padded payload).
				copy(headerForChecksum, headerBuf)
				headerForChecksum[28], headerForChecksum[29], headerForChecksum[30], headerForChecksum[31] = 0, 0, 0, 0

				calculated := crc32.ChecksumIEEE(headerForChecksum)
				calculated = crc32.Update(calculated, crc32.IEEETable, fullChunkBuffer)
				if calculated != checksum {
					pipeErr = fmt.Errorf(
						"checksum mismatch for fragment %d in WAL %d: expected %d, got %d",
						shardFragment, walFile.WALNum, checksum, calculated,
					)
					return
				}

				// Stream logical bytes into the pipe.
				remaining := objectWriteResult.WriteResult.TotalSize - bytesWritten
				if remaining < 0 {
					pipeErr = fmt.Errorf("wrote past TotalSize (TotalSize=%d)", objectWriteResult.WriteResult.TotalSize)
					return
				}

				toWrite := int(length)
				if toWrite > remaining {
					toWrite = remaining
				}

				if toWrite > 0 {
					if _, err := bw.Write(fullChunkBuffer[:toWrite]); err != nil {
						// Reader side likely closed early.
						pipeErr = err
						return
					}
					bytesWritten += toWrite
				}

				// Track on-disk consumption for this segment.
				fileBytesRead += int64(wal.FragmentHeaderBytes) + int64(walInstance.Shard.ChunkSize)

				// End-of-shard validation (same intent as ReadFromWriteResult).
				if flags&wal.FlagEndOfShard != 0 {
					if bytesWritten != objectWriteResult.WriteResult.TotalSize {
						pipeErr = fmt.Errorf(
							"end-of-shard set in WAL %d but wrote %d/%d bytes",
							walFile.WALNum, bytesWritten, objectWriteResult.WriteResult.TotalSize,
						)
						return
					}
					return
				}
			}
		}()
	}

	// Stream body.
	//_, _ = io.Copy(bw, f)
}

// handlePUTShard receives shard data via QUIC and writes it to the local WAL
func (qs *QuicServer) handlePUTShard(br *bufio.Reader, bw *bufio.Writer, req quicproto.Header, putReq PutRequest) {
	// Open WAL for this node
	walInstance, err := wal.New("", qs.WalDir)
	if err != nil {
		slog.Error("handlePUTShard: failed to open WAL", "error", err)
		writeErr(bw, req, quicproto.StatusServerError, fmt.Sprintf("wal open: %v", err))
		return
	}
	defer walInstance.Close()

	// Determine how many bytes to read
	var bodyLen int
	if req.BodyLen > 0 {
		bodyLen = int(req.BodyLen)
	} else if putReq.ShardSize > 0 {
		bodyLen = putReq.ShardSize
	} else {
		writeErr(bw, req, quicproto.StatusBadRequest, "no body length specified")
		return
	}

	// Create a limited reader for the shard data
	shardReader := io.LimitReader(br, int64(bodyLen))

	// Write to WAL
	writeResult, err := walInstance.Write(shardReader, bodyLen)
	if err != nil {
		slog.Error("handlePUTShard: WAL write failed", "error", err)
		writeErr(bw, req, quicproto.StatusServerError, fmt.Sprintf("wal write: %v", err))
		return
	}

	// Store metadata in local Badger DB (object hash -> WriteResult)
	err = walInstance.UpdateObjectToWAL(putReq.ObjectHash, writeResult)
	if err != nil {
		slog.Error("handlePUTShard: failed to update metadata", "error", err)
		writeErr(bw, req, quicproto.StatusServerError, fmt.Sprintf("metadata update: %v", err))
		return
	}

	slog.Info("handlePUTShard: stored shard",
		"bucket", putReq.Bucket,
		"object", putReq.Object,
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
		MetaLen: uint32(len(respBytes)),
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

type rebuildReq struct {
	ShardID  int    `json:"shard_id"`
	Reason   string `json:"reason"`
	JobID    string `json:"job_id"`
	Priority int    `json:"priority"`
}

func (qs *QuicServer) handleREBUILD(bw *bufio.Writer, req quicproto.Header, key string, meta []byte) {
	var r rebuildReq
	if len(meta) > 0 {
		if err := json.Unmarshal(meta, &r); err != nil {
			writeErr(bw, req, quicproto.StatusBadRequest, "bad rebuild meta json")
			return
		}
	}

	// Demo behavior: acknowledge. In real code, enqueue a job and return job status.
	resp := map[string]any{
		"accepted": true,
		"key":      key,
		"job_id":   r.JobID,
		"shard_id": r.ShardID,
		"ts":       time.Now().UnixMilli(),
	}
	b, _ := json.Marshal(resp)

	rh := quicproto.Header{
		Version: quicproto.Version1,
		Method:  req.Method,
		Status:  quicproto.StatusOK,
		ReqID:   req.ReqID,
		MetaLen: uint32(len(b)),
	}
	_ = quicproto.WriteHeader(bw, rh)
	_, _ = bw.Write(b)
	_ = bw.Flush()
}

func writeErr(bw *bufio.Writer, req quicproto.Header, code uint16, msg string) {
	meta := fmt.Sprintf(`{"error":%q}`, msg)
	rh := quicproto.Header{
		Version: quicproto.Version1,
		Method:  req.Method,
		Status:  code,
		ReqID:   req.ReqID,
		MetaLen: uint32(len(meta)),
		BodyLen: 0,
	}
	_ = quicproto.WriteHeader(bw, rh)
	_, _ = bw.WriteString(meta)
	_ = bw.Flush()
}

func safePath(root, key string) string {
	// Very basic safety for demo: clean + disallow absolute.
	clean := filepath.Clean(key)
	for len(clean) > 0 && clean[0] == '/' {
		clean = clean[1:]
	}
	return filepath.Join(root, clean)
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
