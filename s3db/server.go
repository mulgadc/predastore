package s3db

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/mulgadc/predastore/auth"
	"github.com/mulgadc/predastore/internal/tlsconfig"
)

// Server provides HTTP REST API for the distributed database
type Server struct {
	config *ServerConfig
	router chi.Router
	server *http.Server
	node   *RaftNode
}

// ServerConfig holds server configuration
type ServerConfig struct {
	// HTTP settings
	Addr         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration

	// TLS settings
	TLSCert string // Path to TLS certificate file
	TLSKey  string // Path to TLS private key file

	// Authentication - map of AccessKeyID -> SecretAccessKey
	Credentials map[string]string
	Region      string // Region for signature validation (default: us-east-1)
	Service     string // Service name for signature validation (default: s3db)

	// Cluster configuration
	ClusterConfig *ClusterConfig

	// Debug enables verbose request logging (chi middleware.Logger)
	// WARNING: Enabling this in production adds significant CPU overhead (~17%)
	Debug bool
}

// DefaultServerConfig returns sensible defaults
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Addr:         "0.0.0.0:6660",
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
		TLSCert:      "certs/server.pem",
		TLSKey:       "certs/server.key",
	}
}

// NewServer creates a new database server
func NewServer(config *ServerConfig) (*Server, error) {
	if config.ClusterConfig == nil {
		return nil, fmt.Errorf("cluster config is required")
	}

	// Fail closed: an empty credentials map would otherwise grant unauthenticated
	// access to the Raft-backed IAM mutation endpoints under /v1/*.
	if len(config.Credentials) == 0 {
		return nil, fmt.Errorf("s3db: no credentials configured; refusing to start (set credentials on [[db]] or [[auth]])")
	}

	// Plumb the same cert/key used by the s3db HTTPS REST listener into the
	// Raft transport. NewRaftNode fails closed if either is empty.
	config.ClusterConfig.TLSCert = config.TLSCert
	config.ClusterConfig.TLSKey = config.TLSKey

	// Initialize Raft node
	node, err := NewRaftNode(config.ClusterConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft node: %w", err)
	}

	s := &Server{
		config: config,
		node:   node,
		router: chi.NewRouter(),
	}

	// Setup middleware
	// Only enable request logging in debug mode - adds ~17% CPU overhead
	if config.Debug {
		s.router.Use(middleware.Logger)
	}
	s.router.Use(middleware.Recoverer)
	s.router.Use(s.authMiddleware)

	// Setup routes
	s.setupRoutes()

	return s, nil
}

// setupRoutes configures all HTTP endpoints
func (s *Server) setupRoutes() {
	// Health check (no auth required - handled in middleware)
	s.router.Get("/health", s.handleHealth)

	// Status endpoint (no auth required - handled in middleware)
	s.router.Get("/status", s.handleStatus)

	// Database operations (auth required)
	s.router.Route("/v1", func(r chi.Router) {
		// Key-value operations
		r.Get("/get/{table}/{key}", s.handleGet)
		r.Post("/put/{table}/{key}", s.handlePut)
		r.Delete("/delete/{table}/{key}", s.handleDelete)

		// Scan operations
		r.Get("/scan/{table}", s.handleScan)

		// Cluster management
		r.Post("/join", s.handleJoin)
		r.Get("/leader", s.handleLeader)
	})
}

// authMiddleware validates AWS Signature V4 authentication.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" || r.URL.Path == "/status" {
			next.ServeHTTP(w, r)
			return
		}

		region := s.config.Region
		if region == "" {
			region = DefaultRegion
		}
		service := s.config.Service
		if service == "" {
			service = DefaultService
		}

		sig, err := auth.ParseReq(r)
		if err != nil {
			slog.Debug("Auth parse failed", "error", err)
			s.writeJSON(w, http.StatusForbidden, ErrorResponse{
				Error:   "AccessDenied",
				Message: err.Error(),
			})
			return
		}

		secret, ok := s.config.Credentials[sig.AccessKeyID]
		if !ok {
			slog.Debug("Unknown access key", "accessKey", sig.AccessKeyID)
			s.writeJSON(w, http.StatusForbidden, ErrorResponse{
				Error:   "AccessDenied",
				Message: fmt.Sprintf("invalid access key: %s", sig.AccessKeyID),
			})
			return
		}

		if err := sig.Verify(secret, service, region); err != nil {
			slog.Debug("Auth verify failed", "error", err)
			s.writeJSON(w, http.StatusForbidden, ErrorResponse{
				Error:   "AccessDenied",
				Message: err.Error(),
			})
			return
		}

		ctx := context.WithValue(r.Context(), contextKeyAccessKey, sig.AccessKeyID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// contextKey for storing values in request context
type contextKey string

const contextKeyAccessKey contextKey = "accessKey"

// writeJSON writes a JSON response
func (s *Server) writeJSON(w http.ResponseWriter, statusCode int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Debug("Failed to encode JSON response", "error", err)
	}
}

// handleHealth returns server health status
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]any{
		"status": "healthy",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

// handleStatus returns detailed server status
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	stats := s.node.Stats()

	s.writeJSON(w, http.StatusOK, StatusResponse{
		NodeID:     strconv.FormatUint(s.config.ClusterConfig.NodeID, 10),
		State:      stats["state"],
		Leader:     s.node.LeaderID(),
		LeaderAddr: s.node.LeaderAddr(),
		Term:       stats["term"],
		CommitIdx:  stats["commit_index"],
		AppliedIdx: stats["applied_index"],
		IsLeader:   s.node.IsLeader(),
	})
}

// handleGet retrieves a value by key
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	hexKey := chi.URLParam(r, "key")

	if table == "" || hexKey == "" {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Table and key are required",
		})
		return
	}

	// Hex-decode the key (client sends hex-encoded keys)
	keyBytes, err := hex.DecodeString(hexKey)
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Invalid hex-encoded key",
		})
		return
	}
	key := string(keyBytes)

	value, err := s.node.Get(table, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			s.writeJSON(w, http.StatusNotFound, ErrorResponse{
				Error:   "KeyNotFound",
				Message: fmt.Sprintf("Key '%s' not found in table '%s'", key, table),
			})
			return
		}
		slog.Error("Get failed", "table", table, "key", key, "error", err)
		s.writeJSON(w, http.StatusInternalServerError, ErrorResponse{
			Error:   "InternalError",
			Message: err.Error(),
		})
		return
	}

	s.writeJSON(w, http.StatusOK, GetResponse{
		Table: table,
		Key:   key,
		Value: value,
	})
}

// handlePut stores a key-value pair
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	hexKey := chi.URLParam(r, "key")

	if table == "" || hexKey == "" {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Table and key are required",
		})
		return
	}

	// Hex-decode the key (client sends hex-encoded keys)
	keyBytes, err := hex.DecodeString(hexKey)
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Invalid hex-encoded key",
		})
		return
	}
	key := string(keyBytes)

	value, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Failed to read request body",
		})
		return
	}
	if len(value) == 0 {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Request body (value) is required",
		})
		return
	}

	// Writes must go through leader
	if !s.node.IsLeader() {
		s.writeJSON(w, http.StatusTemporaryRedirect, ErrorResponse{
			Error:   "NotLeader",
			Message: "This node is not the leader",
			Leader:  s.node.LeaderAddr(),
		})
		return
	}

	if err := s.node.Put(table, key, value); err != nil {
		slog.Error("Put failed", "table", table, "key", key, "error", err)
		s.writeJSON(w, http.StatusInternalServerError, ErrorResponse{
			Error:   "InternalError",
			Message: err.Error(),
		})
		return
	}

	slog.Debug("Put succeeded", "table", table, "key", key, "size", len(value))
	s.writeJSON(w, http.StatusCreated, PutResponse{
		Table: table,
		Key:   key,
		Size:  len(value),
	})
}

// handleDelete removes a key
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	hexKey := chi.URLParam(r, "key")

	if table == "" || hexKey == "" {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Table and key are required",
		})
		return
	}

	// Hex-decode the key (client sends hex-encoded keys)
	keyBytes, err := hex.DecodeString(hexKey)
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Invalid hex-encoded key",
		})
		return
	}
	key := string(keyBytes)

	// Writes must go through leader
	if !s.node.IsLeader() {
		s.writeJSON(w, http.StatusTemporaryRedirect, ErrorResponse{
			Error:   "NotLeader",
			Message: "This node is not the leader",
			Leader:  s.node.LeaderAddr(),
		})
		return
	}

	if err := s.node.Delete(table, key); err != nil {
		slog.Error("Delete failed", "table", table, "key", key, "error", err)
		s.writeJSON(w, http.StatusInternalServerError, ErrorResponse{
			Error:   "InternalError",
			Message: err.Error(),
		})
		return
	}

	slog.Debug("Delete succeeded", "table", table, "key", key)
	w.WriteHeader(http.StatusNoContent)
}

// handleScan lists keys with optional prefix
func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	prefix := r.URL.Query().Get("prefix")
	limitStr := r.URL.Query().Get("limit")
	limit := 1000
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil {
			limit = l
		}
	}

	if table == "" {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Table is required",
		})
		return
	}

	var items []ScanItem
	count := 0
	err := s.node.Scan(table, prefix, func(key string, value []byte) error {
		if count >= limit {
			return nil // Stop iteration
		}
		items = append(items, ScanItem{
			Key:   key,
			Value: value,
		})
		count++
		return nil
	})

	if err != nil {
		slog.Error("Scan failed", "table", table, "prefix", prefix, "error", err)
		s.writeJSON(w, http.StatusInternalServerError, ErrorResponse{
			Error:   "InternalError",
			Message: err.Error(),
		})
		return
	}

	s.writeJSON(w, http.StatusOK, ScanResponse{
		Table:  table,
		Prefix: prefix,
		Count:  len(items),
		Items:  items,
	})
}

// handleJoin adds a new node to the cluster
func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Failed to read request body",
		})
		return
	}

	var req JoinRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Invalid JSON body",
		})
		return
	}

	if req.NodeID == "" || req.Addr == "" {
		s.writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "InvalidRequest",
			Message: "node_id and addr are required",
		})
		return
	}

	if !s.node.IsLeader() {
		s.writeJSON(w, http.StatusTemporaryRedirect, ErrorResponse{
			Error:   "NotLeader",
			Message: "This node is not the leader",
			Leader:  s.node.LeaderAddr(),
		})
		return
	}

	if err := s.node.Join(req.NodeID, req.Addr); err != nil {
		slog.Error("Join failed", "node_id", req.NodeID, "addr", req.Addr, "error", err)
		s.writeJSON(w, http.StatusInternalServerError, ErrorResponse{
			Error:   "InternalError",
			Message: err.Error(),
		})
		return
	}

	slog.Info("Node joined cluster", "node_id", req.NodeID, "addr", req.Addr)
	s.writeJSON(w, http.StatusOK, map[string]any{
		"status":  "joined",
		"node_id": req.NodeID,
	})
}

// handleLeader returns the current leader information
func (s *Server) handleLeader(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]any{
		"leader_id":   s.node.LeaderID(),
		"leader_addr": s.node.LeaderAddr(),
		"is_leader":   s.node.IsLeader(),
	})
}

// Start begins listening for HTTPS requests with TLS and HTTP/2 support
func (s *Server) Start() error {
	slog.Info("Starting database server with TLS/HTTP2", "addr", s.config.Addr, "node_id", s.config.ClusterConfig.NodeID)

	// Load TLS certificates
	cert, err := tls.LoadX509KeyPair(s.config.TLSCert, s.config.TLSKey)
	if err != nil {
		return fmt.Errorf("failed to load TLS certificate: %w", err)
	}

	// Configure TLS with HTTP/2 support
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		// NextProtos enables ALPN for HTTP/2 negotiation
		NextProtos:       []string{"h2", "http/1.1"},
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: tlsconfig.Curves,
	}

	s.server = &http.Server{
		Addr:              s.config.Addr,
		Handler:           s.router,
		TLSConfig:         tlsConfig,
		ReadTimeout:       s.config.ReadTimeout,
		WriteTimeout:      s.config.WriteTimeout,
		IdleTimeout:       s.config.IdleTimeout,
		ReadHeaderTimeout: 10 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1MB
	}

	ln, err := net.Listen("tcp", s.config.Addr)
	if err != nil {
		return err
	}

	tlsListener := tls.NewListener(ln, tlsConfig)
	return s.server.Serve(tlsListener)
}

// Shutdown gracefully stops the server
func (s *Server) Shutdown() error {
	slog.Info("DBServer: shutting down", "node_id", s.config.ClusterConfig.NodeID)

	slog.Info("DBServer: stopping HTTP server")
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.server.Shutdown(ctx); err != nil {
			slog.Warn("DBServer: HTTP shutdown error", "error", err)
		}
	}

	slog.Info("DBServer: closing Raft node")
	if err := s.node.Close(); err != nil {
		slog.Warn("DBServer: Raft node close error", "error", err)
	}

	slog.Info("DBServer: shutdown complete", "node_id", s.config.ClusterConfig.NodeID)
	return nil
}

// WaitForLeader blocks until a leader is elected
func (s *Server) WaitForLeader(timeout time.Duration) error {
	return s.node.WaitForLeader(timeout)
}

// IsLeader returns true if this node is the leader
func (s *Server) IsLeader() bool {
	return s.node.IsLeader()
}

// Node returns the underlying Raft node
func (s *Server) Node() *RaftNode {
	return s.node
}

// GetRouter returns the chi router for testing
func (s *Server) GetRouter() chi.Router {
	return s.router
}

// Response types

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Leader  string `json:"leader,omitempty"` // Set when NotLeader error
}

// StatusResponse represents server status
type StatusResponse struct {
	NodeID     string `json:"node_id"`
	State      string `json:"state"`
	Leader     string `json:"leader"`
	LeaderAddr string `json:"leader_addr"`
	Term       string `json:"term"`
	CommitIdx  string `json:"commit_index"`
	AppliedIdx string `json:"applied_index"`
	IsLeader   bool   `json:"is_leader"`
}

// GetResponse represents a successful GET response
type GetResponse struct {
	Table string `json:"table"`
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// PutResponse represents a successful PUT response
type PutResponse struct {
	Table string `json:"table"`
	Key   string `json:"key"`
	Size  int    `json:"size"`
}

// ScanItem represents a single item in scan results
type ScanItem struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// ScanResponse represents scan results
type ScanResponse struct {
	Table  string     `json:"table"`
	Prefix string     `json:"prefix"`
	Count  int        `json:"count"`
	Items  []ScanItem `json:"items"`
}

// JoinRequest represents a request to join the cluster
type JoinRequest struct {
	NodeID string `json:"node_id"`
	Addr   string `json:"addr"`
}
