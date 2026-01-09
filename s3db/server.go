package s3db

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

// Server provides HTTP REST API for the distributed database
type Server struct {
	config *ServerConfig
	app    *fiber.App
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
}

// DefaultServerConfig returns sensible defaults
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Addr:         "0.0.0.0:6660",
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
		TLSCert:      "config/server.pem",
		TLSKey:       "config/server.key",
	}
}

// NewServer creates a new database server
func NewServer(config *ServerConfig) (*Server, error) {
	if config.ClusterConfig == nil {
		return nil, fmt.Errorf("cluster config is required")
	}

	// Initialize Raft node
	node, err := NewRaftNode(config.ClusterConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft node: %w", err)
	}

	s := &Server{
		config: config,
		node:   node,
	}

	// Setup Fiber app
	s.app = fiber.New(fiber.Config{
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		IdleTimeout:  config.IdleTimeout,
		ErrorHandler: s.errorHandler,
	})

	// Middleware
	s.app.Use(logger.New(logger.Config{
		Format:     "${time} | ${status} | ${latency} | ${ip} | ${method} | ${path}\n",
		TimeFormat: "15:04:05",
	}))

	// Setup routes
	s.setupRoutes()

	return s, nil
}

// setupRoutes configures all HTTP endpoints
func (s *Server) setupRoutes() {
	// Health check (no auth required)
	s.app.Get("/health", s.handleHealth)

	// Status endpoint (no auth required)
	s.app.Get("/status", s.handleStatus)

	// Database operations (auth required)
	api := s.app.Group("/v1", s.authMiddleware)

	// Key-value operations
	api.Get("/get/:table/:key", s.handleGet)
	api.Post("/put/:table/:key", s.handlePut)
	api.Delete("/delete/:table/:key", s.handleDelete)

	// Scan operations
	api.Get("/scan/:table", s.handleScan)

	// Cluster management
	api.Post("/join", s.handleJoin)
	api.Get("/leader", s.handleLeader)
}

// authMiddleware validates AWS Signature V4 authentication
func (s *Server) authMiddleware(c *fiber.Ctx) error {
	// Skip auth if no credentials configured
	if len(s.config.Credentials) == 0 {
		return c.Next()
	}

	// Get region and service, with defaults
	region := s.config.Region
	if region == "" {
		region = DefaultRegion
	}
	service := s.config.Service
	if service == "" {
		service = DefaultService
	}

	// Validate the signature
	accessKey, err := ValidateSignature(c, s.config.Credentials, region, service)
	if err != nil {
		slog.Debug("Auth failed", "error", err)
		return c.Status(http.StatusForbidden).JSON(ErrorResponse{
			Error:   "AccessDenied",
			Message: err.Error(),
		})
	}

	// Store authenticated access key in context for potential audit logging
	c.Locals("accessKey", accessKey)

	return c.Next()
}

// handleHealth returns server health status
func (s *Server) handleHealth(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status": "healthy",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

// handleStatus returns detailed server status
func (s *Server) handleStatus(c *fiber.Ctx) error {
	stats := s.node.Stats()

	return c.JSON(StatusResponse{
		NodeID:     fmt.Sprintf("%d", s.config.ClusterConfig.NodeID),
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
func (s *Server) handleGet(c *fiber.Ctx) error {
	table := c.Params("table")
	hexKey := c.Params("key")

	if table == "" || hexKey == "" {
		return c.Status(http.StatusBadRequest).JSON(ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Table and key are required",
		})
	}

	// Hex-decode the key (client sends hex-encoded keys)
	keyBytes, err := hex.DecodeString(hexKey)
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Invalid hex-encoded key",
		})
	}
	key := string(keyBytes)

	value, err := s.node.Get(table, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return c.Status(http.StatusNotFound).JSON(ErrorResponse{
				Error:   "KeyNotFound",
				Message: fmt.Sprintf("Key '%s' not found in table '%s'", key, table),
			})
		}
		slog.Error("Get failed", "table", table, "key", key, "error", err)
		return c.Status(http.StatusInternalServerError).JSON(ErrorResponse{
			Error:   "InternalError",
			Message: err.Error(),
		})
	}

	return c.JSON(GetResponse{
		Table: table,
		Key:   key,
		Value: value,
	})
}

// handlePut stores a key-value pair
func (s *Server) handlePut(c *fiber.Ctx) error {
	table := c.Params("table")
	hexKey := c.Params("key")

	if table == "" || hexKey == "" {
		return c.Status(http.StatusBadRequest).JSON(ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Table and key are required",
		})
	}

	// Hex-decode the key (client sends hex-encoded keys)
	keyBytes, err := hex.DecodeString(hexKey)
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Invalid hex-encoded key",
		})
	}
	key := string(keyBytes)

	value := c.Body()
	if len(value) == 0 {
		return c.Status(http.StatusBadRequest).JSON(ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Request body (value) is required",
		})
	}

	// Writes must go through leader
	if !s.node.IsLeader() {
		return c.Status(http.StatusTemporaryRedirect).JSON(ErrorResponse{
			Error:   "NotLeader",
			Message: "This node is not the leader",
			Leader:  s.node.LeaderAddr(),
		})
	}

	if err := s.node.Put(table, key, value); err != nil {
		slog.Error("Put failed", "table", table, "key", key, "error", err)
		return c.Status(http.StatusInternalServerError).JSON(ErrorResponse{
			Error:   "InternalError",
			Message: err.Error(),
		})
	}

	slog.Info("Put succeeded", "table", table, "key", key, "size", len(value))
	return c.Status(http.StatusCreated).JSON(PutResponse{
		Table: table,
		Key:   key,
		Size:  len(value),
	})
}

// handleDelete removes a key
func (s *Server) handleDelete(c *fiber.Ctx) error {
	table := c.Params("table")
	hexKey := c.Params("key")

	if table == "" || hexKey == "" {
		return c.Status(http.StatusBadRequest).JSON(ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Table and key are required",
		})
	}

	// Hex-decode the key (client sends hex-encoded keys)
	keyBytes, err := hex.DecodeString(hexKey)
	if err != nil {
		return c.Status(http.StatusBadRequest).JSON(ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Invalid hex-encoded key",
		})
	}
	key := string(keyBytes)

	// Writes must go through leader
	if !s.node.IsLeader() {
		return c.Status(http.StatusTemporaryRedirect).JSON(ErrorResponse{
			Error:   "NotLeader",
			Message: "This node is not the leader",
			Leader:  s.node.LeaderAddr(),
		})
	}

	if err := s.node.Delete(table, key); err != nil {
		slog.Error("Delete failed", "table", table, "key", key, "error", err)
		return c.Status(http.StatusInternalServerError).JSON(ErrorResponse{
			Error:   "InternalError",
			Message: err.Error(),
		})
	}

	slog.Info("Delete succeeded", "table", table, "key", key)
	return c.Status(http.StatusNoContent).Send(nil)
}

// handleScan lists keys with optional prefix
func (s *Server) handleScan(c *fiber.Ctx) error {
	table := c.Params("table")
	prefix := c.Query("prefix", "")
	limit := c.QueryInt("limit", 1000)

	if table == "" {
		return c.Status(http.StatusBadRequest).JSON(ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Table is required",
		})
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
		return c.Status(http.StatusInternalServerError).JSON(ErrorResponse{
			Error:   "InternalError",
			Message: err.Error(),
		})
	}

	return c.JSON(ScanResponse{
		Table:  table,
		Prefix: prefix,
		Count:  len(items),
		Items:  items,
	})
}

// handleJoin adds a new node to the cluster
func (s *Server) handleJoin(c *fiber.Ctx) error {
	var req JoinRequest
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(ErrorResponse{
			Error:   "InvalidRequest",
			Message: "Invalid JSON body",
		})
	}

	if req.NodeID == "" || req.Addr == "" {
		return c.Status(http.StatusBadRequest).JSON(ErrorResponse{
			Error:   "InvalidRequest",
			Message: "node_id and addr are required",
		})
	}

	if !s.node.IsLeader() {
		return c.Status(http.StatusTemporaryRedirect).JSON(ErrorResponse{
			Error:   "NotLeader",
			Message: "This node is not the leader",
			Leader:  s.node.LeaderAddr(),
		})
	}

	if err := s.node.Join(req.NodeID, req.Addr); err != nil {
		slog.Error("Join failed", "node_id", req.NodeID, "addr", req.Addr, "error", err)
		return c.Status(http.StatusInternalServerError).JSON(ErrorResponse{
			Error:   "InternalError",
			Message: err.Error(),
		})
	}

	slog.Info("Node joined cluster", "node_id", req.NodeID, "addr", req.Addr)
	return c.JSON(fiber.Map{
		"status":  "joined",
		"node_id": req.NodeID,
	})
}

// handleLeader returns the current leader information
func (s *Server) handleLeader(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"leader_id":   s.node.LeaderID(),
		"leader_addr": s.node.LeaderAddr(),
		"is_leader":   s.node.IsLeader(),
	})
}

// errorHandler handles Fiber errors
func (s *Server) errorHandler(c *fiber.Ctx, err error) error {
	code := http.StatusInternalServerError
	if e, ok := err.(*fiber.Error); ok {
		code = e.Code
	}
	return c.Status(code).JSON(ErrorResponse{
		Error:   "InternalError",
		Message: err.Error(),
	})
}

// Start begins listening for HTTPS requests with TLS
func (s *Server) Start() error {
	slog.Info("Starting database server with TLS", "addr", s.config.Addr, "node_id", s.config.ClusterConfig.NodeID)
	return s.app.ListenTLS(s.config.Addr, s.config.TLSCert, s.config.TLSKey)
}

// Shutdown gracefully stops the server
func (s *Server) Shutdown() error {
	if err := s.app.Shutdown(); err != nil {
		return err
	}
	return s.node.Close()
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
