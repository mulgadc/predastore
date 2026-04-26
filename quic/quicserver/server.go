package quicserver

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mulgadc/predastore/quic/quicconf"
	"github.com/mulgadc/predastore/quic/quicproto"
	"github.com/mulgadc/predastore/store"
	"github.com/mulgadc/predastore/utils"
	quic "github.com/quic-go/quic-go"
)

const (
	alpn              = "mulga-repl-v1"
	maxKeyLen  uint32 = 4 * 1024
	maxMetaLen uint32 = 64 * 1024
)

// QuicServer handles QUIC RPC requests for shard storage operations
type QuicServer struct {
	Addr   string
	WalDir string

	store *store.Store

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
	ShardSize int64  `json:"shard_size"`
	Error     string `json:"error,omitempty"`
}

// DeleteRequest contains metadata for deleting a shard via QUIC DELETE
type DeleteRequest struct {
	Bucket     string   `json:"bucket"`
	Object     string   `json:"object"`
	ObjectHash [32]byte `json:"object_hash"` // SHA256 of bucket/object for metadata lookup
	ShardIndex int      `json:"shard_index"` // Index of shard to delete
}

// DeleteResponse contains the result of a QUIC DELETE operation
type DeleteResponse struct {
	Deleted bool   `json:"deleted"`
	Error   string `json:"error,omitempty"`
}

// NewWithRetry creates and starts a new QUIC server with retry logic for port binding
// Returns the server instance and any error encountered
func NewWithRetry(walDir string, addr string, maxRetries int) (*QuicServer, error) {
	// Ensure storage directory exists
	if err := os.MkdirAll(walDir, 0750); err != nil {
		return nil, fmt.Errorf("create storage directory %s: %w", walDir, err)
	}

	s, err := store.Open(walDir)
	if err != nil {
		return nil, fmt.Errorf("open store in %s: %w", walDir, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	qs := &QuicServer{
		WalDir:     walDir,
		Addr:       addr,
		store:      s,
		ctx:        ctx,
		cancel:     cancel,
		shutdownCh: make(chan struct{}),
	}

	tlsConf, err := makeServerTLSConfig()
	if err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("tls config: %w", err)
	}
	tlsConf.NextProtos = []string{alpn}

	// Retry port binding with exponential backoff
	var l *quic.Listener
	for i := range maxRetries {
		l, err = quic.ListenAddr(addr, tlsConf, &quic.Config{
			KeepAlivePeriod:       15 * time.Second,
			MaxIdleTimeout:        60 * time.Second,
			MaxIncomingStreams:    1000, // Allow more concurrent streams
			MaxIncomingUniStreams: 1000,
			// See docs/development/bugs/multipart-upload-deadlock.md (Bug C).
			InitialStreamReceiveWindow:     quicconf.InitialStreamReceiveWindow,
			MaxStreamReceiveWindow:         quicconf.MaxStreamReceiveWindow,
			InitialConnectionReceiveWindow: quicconf.InitialConnectionReceiveWindow,
			MaxConnectionReceiveWindow:     quicconf.MaxConnectionReceiveWindow,
		})
		if err == nil {
			break
		}
		if i < maxRetries-1 {
			time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
		}
	}
	if err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("listen on %s: %w", addr, err)
	}

	qs.listener = l
	slog.Info("QUIC RPC server listening", "addr", addr, "alpn", alpn, "walDir", walDir)

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
		slog.Error("failed to start QUIC server", "error", err)
		os.Exit(1)
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

	if qs.store != nil {
		if err := qs.store.Close(); err != nil {
			return fmt.Errorf("close store: %w", err)
		}
		qs.store = nil
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
		slog.Debug("handleStream: active streams high", "count", activeCount, "streamID", streamID)
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
