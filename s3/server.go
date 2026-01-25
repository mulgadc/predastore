package s3

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/distributed"
	"github.com/mulgadc/predastore/backend/filesystem"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/mulgadc/predastore/s3db"
	"go.uber.org/automaxprocs/maxprocs"
)

// BackendType specifies the storage backend type
type BackendType string

const (
	// BackendFilesystem uses local filesystem storage (simple, single-node)
	BackendFilesystem BackendType = "filesystem"
	// BackendDistributed uses distributed storage with erasure coding
	BackendDistributed BackendType = "distributed"
)

// Server encapsulates the S3-compatible server with all its components
type Server struct {
	// Configuration
	configPath  string
	host        string
	port        int
	tlsCert     string
	tlsKey      string
	basePath    string
	debug       bool
	backendType BackendType
	nodeID      int // For distributed mode: specific node to run (-1 = dev mode)

	// Runtime state
	config  *Config
	server  *HTTP2Server
	backend backend.Backend
	dbServers   []*s3db.Server
	quicCancel  context.CancelFunc

	// Profiling
	pprofEnabled    bool
	pprofFile       *os.File
	pprofOutputPath string

	// Lifecycle
	mu       sync.Mutex
	running  bool
	shutdown chan struct{}
}

// Option configures a Server
type Option func(*Server) error

// NewServer creates a new S3 server with the given options.
// By default, it uses the filesystem backend which is simple and requires no additional setup.
//
// For distributed storage with erasure coding, use WithBackend(BackendDistributed).
func NewServer(opts ...Option) (*Server, error) {
	s := &Server{
		host:        "0.0.0.0",
		port:        8443,
		backendType: BackendFilesystem,
		nodeID:      -1, // Dev mode by default
		shutdown:    make(chan struct{}),
	}

	// Apply options
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}

	// Initialize the server
	if err := s.init(); err != nil {
		return nil, fmt.Errorf("failed to initialize server: %w", err)
	}

	return s, nil
}

// WithConfigPath sets the path to the TOML configuration file
func WithConfigPath(path string) Option {
	return func(s *Server) error {
		s.configPath = path
		return nil
	}
}

// WithAddress sets the server host and port
func WithAddress(host string, port int) Option {
	return func(s *Server) error {
		s.host = host
		s.port = port
		return nil
	}
}

// WithTLS sets the TLS certificate and key paths
func WithTLS(certPath, keyPath string) Option {
	return func(s *Server) error {
		s.tlsCert = certPath
		s.tlsKey = keyPath
		return nil
	}
}

// WithBasePath sets the base directory for data storage
func WithBasePath(path string) Option {
	return func(s *Server) error {
		s.basePath = path
		return nil
	}
}

// WithDebug enables debug logging
func WithDebug(enabled bool) Option {
	return func(s *Server) error {
		s.debug = enabled
		return nil
	}
}

// WithBackend sets the storage backend type.
// Use BackendFilesystem for simple single-node storage.
// Use BackendDistributed for distributed storage with erasure coding.
// If empty string is passed, the default (BackendFilesystem) is used.
func WithBackend(backendType BackendType) Option {
	return func(s *Server) error {
		// Only override if a non-empty value is provided
		if backendType != "" {
			s.backendType = backendType
		}
		return nil
	}
}

// WithNodeID sets the node ID for distributed mode.
// Use -1 (default) for dev mode which runs all nodes locally.
// Use a specific ID to run only that node (for production deployments).
func WithNodeID(nodeID int) Option {
	return func(s *Server) error {
		s.nodeID = nodeID
		return nil
	}
}

// WithPprof enables CPU profiling.
// The profile is written to a temp file during operation and saved to outputPath on shutdown.
// If outputPath is empty, it defaults to /tmp/predastore-cpu.prof
func WithPprof(enabled bool, outputPath string) Option {
	return func(s *Server) error {
		s.pprofEnabled = enabled
		if outputPath == "" {
			outputPath = "/tmp/predastore-cpu.prof"
		}
		s.pprofOutputPath = outputPath
		return nil
	}
}


// init initializes the server components
func (s *Server) init() error {
	// Auto-tune GOMAXPROCS for cgroups
	undo, err := maxprocs.Set(maxprocs.Logger(log.Printf))
	if err != nil {
		slog.Warn("Failed to set GOMAXPROCS", "error", err)
	} else {
		defer undo()
	}

	// Check environment variable for pprof if not already enabled
	if !s.pprofEnabled && os.Getenv("PPROF_ENABLED") == "1" {
		s.pprofEnabled = true
		if s.pprofOutputPath == "" {
			s.pprofOutputPath = os.Getenv("PPROF_OUTPUT")
			if s.pprofOutputPath == "" {
				s.pprofOutputPath = "/tmp/predastore-cpu.prof"
			}
		}
	}

	// Start CPU profiling if enabled
	if s.pprofEnabled {
		if err := s.startProfiling(); err != nil {
			slog.Error("Failed to start CPU profiling", "error", err)
			// Don't fail server start, just log the error
		}
	}

	// Create and load configuration
	s.config = &Config{
		ConfigPath: s.configPath,
		Port:       s.port,
		Host:       s.host,
		Debug:      s.debug,
		BasePath:   s.basePath,
	}

	// Read configuration file if provided
	if s.configPath != "" {
		if err := s.config.ReadConfig(); err != nil {
			return fmt.Errorf("failed to read config: %w", err)
		}
	}

	// CLI/env flags override config file settings
	// This ensures HIVE_PREDASTORE_DEBUG=true works even if config file has debug=false
	if s.debug {
		s.config.Debug = true
	}

	// Set log level early so debug logs during backend initialization are visible
	var logLevel slog.Level
	if s.config.Debug {
		logLevel = slog.LevelDebug
	} else if s.config.DisableLogging {
		logLevel = slog.LevelError
	} else {
		logLevel = slog.LevelInfo
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})
	slog.SetDefault(slog.New(handler))

	if s.config.Debug {
		slog.Info("Debug logging enabled")
	}

	// Initialize the appropriate backend
	switch s.backendType {
	case BackendFilesystem:
		if err := s.initFilesystemBackend(); err != nil {
			return err
		}
	case BackendDistributed:
		if err := s.initDistributedBackend(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown backend type: %s", s.backendType)
	}

	// Setup HTTP routes with the backend using HTTP/2 server
	slog.Info("Server init", "backendType", s.backendType)
	s.server = NewHTTP2ServerWithBackend(s.config, s.backend)
	slog.Info("HTTP/2 server initialized - using net/http for connection multiplexing")

	return nil
}

// initFilesystemBackend initializes the filesystem storage backend
func (s *Server) initFilesystemBackend() error {
	buckets := make([]filesystem.BucketConfig, len(s.config.Buckets))
	for i, b := range s.config.Buckets {
		buckets[i] = filesystem.BucketConfig{
			Name:     b.Name,
			Pathname: b.Pathname,
			Region:   b.Region,
			Type:     b.Type,
			Public:   b.Public,
		}
	}

	fsConfig := &filesystem.Config{
		Buckets:   buckets,
		OwnerID:   "predastore",
		OwnerName: "predastore",
	}

	be, err := filesystem.New(fsConfig)
	if err != nil {
		return fmt.Errorf("failed to create filesystem backend: %w", err)
	}

	s.backend = be
	slog.Info("Initialized filesystem backend", "buckets", len(buckets))
	return nil
}

// initDistributedBackend initializes the distributed storage backend with DB and QUIC servers
func (s *Server) initDistributedBackend() error {
	// Launch DB servers first (required for distributed state)
	hasDBNodes := len(s.config.DB) > 0

	if hasDBNodes {
		s.dbServers = s.launchDBServers()
	} else {
		s.dbServers = s.launchDefaultDB()
	}

	// Wait for leader election
	if len(s.dbServers) > 0 {
		s.waitForDBLeader()
	}

	// Create the distributed backend
	be, err := s.createDistributedBackend()
	if err != nil {
		return err
	}
	s.backend = be

	// Launch QUIC servers for shard storage
	s.launchQUICServers()

	return nil
}

// launchDBServers starts the distributed database servers from config
func (s *Server) launchDBServers() []*s3db.Server {
	var servers []*s3db.Server

	// Convert config to s3db types, applying base-dir to paths
	dbNodes := make([]s3db.DBNodeConfig, len(s.config.DB))
	for i, n := range s.config.DB {
		// Apply base-dir to relative paths
		nodePath := checkBaseDir(s.basePath, n.Path)

		dbNodes[i] = s3db.DBNodeConfig{
			ID:              uint64(n.ID),
			Host:            n.Host,
			Port:            n.Port,
			RaftPort:        n.RaftPort,
			Path:            nodePath,
			AccessKeyID:     n.AccessKeyID,
			SecretAccessKey: n.SecretAccessKey,
			Leader:          n.Leader,
		}
	}

	// Build credentials map
	credentials := make(map[string]string)
	for _, n := range s.config.DB {
		if n.AccessKeyID != "" && n.SecretAccessKey != "" {
			credentials[n.AccessKeyID] = n.SecretAccessKey
		}
	}
	for _, auth := range s.config.Auth {
		if auth.AccessKeyID != "" && auth.SecretAccessKey != "" {
			credentials[auth.AccessKeyID] = auth.SecretAccessKey
		}
	}

	// Determine which nodes to launch
	var nodesToLaunch []s3db.DBNodeConfig
	if s.nodeID > 0 {
		for _, n := range dbNodes {
			if n.ID == uint64(s.nodeID) {
				nodesToLaunch = append(nodesToLaunch, n)
				break
			}
		}
		if len(nodesToLaunch) == 0 {
			slog.Error("Database node not found", "id", s.nodeID)
			return nil
		}
	} else if allDBHostsSame(dbNodes) {
		nodesToLaunch = dbNodes
		slog.Info("Dev mode: launching all database nodes locally")
	} else {
		slog.Error("Distributed mode requires -node flag when DB nodes have different hosts")
		return nil
	}

	// Check bootstrap status
	bootstrap := true
	for _, n := range nodesToLaunch {
		if _, err := os.Stat(filepath.Join(n.Path, "raft")); err == nil {
			bootstrap = false
			break
		}
	}

	// Launch each node
	for _, node := range nodesToLaunch {
		clusterCfg := s3db.DefaultClusterConfig()
		clusterCfg.NodeID = node.ID
		clusterCfg.Nodes = dbNodes
		clusterCfg.DataDir = node.Path
		clusterCfg.Bootstrap = bootstrap && node.Leader

		if err := os.MkdirAll(node.Path, 0750); err != nil {
			slog.Error("Failed to create DB node directory", "path", node.Path, "error", err)
			continue
		}

		serverCfg := &s3db.ServerConfig{
			Addr:          node.HTTPAddr(),
			TLSCert:       s.tlsCert,
			TLSKey:        s.tlsKey,
			Credentials:   credentials,
			Region:        s.config.Region,
			Service:       "s3db",
			ClusterConfig: clusterCfg,
		}

		server, err := s3db.NewServer(serverCfg)
		if err != nil {
			slog.Error("Failed to create database server", "nodeID", node.ID, "error", err)
			continue
		}

		servers = append(servers, server)

		go func(srv *s3db.Server, nodeID uint64, addr string) {
			slog.Info("Starting database server", "nodeID", nodeID, "addr", addr)
			if err := srv.Start(); err != nil {
				slog.Error("Database server failed", "nodeID", nodeID, "error", err)
			}
		}(server, node.ID, node.HTTPAddr())
	}

	return servers
}

// launchDefaultDB creates a default embedded database when no [[db]] is configured
func (s *Server) launchDefaultDB() []*s3db.Server {
	slog.Info("No [[db]] configuration found, launching default embedded database")

	defaultPort := 6660
	defaultPath := filepath.Join(s.basePath, "db")
	if defaultPath == "" {
		defaultPath = "data/db"
	}

	var accessKeyID, secretAccessKey string
	if len(s.config.Auth) > 0 {
		accessKeyID = s.config.Auth[0].AccessKeyID
		secretAccessKey = s.config.Auth[0].SecretAccessKey
	} else {
		accessKeyID = "predastore"
		secretAccessKey = "predastore"
	}

	dbNode := s3db.DBNodeConfig{
		ID:              1,
		Host:            "127.0.0.1",
		Port:            defaultPort,
		RaftPort:        defaultPort + 1000,
		Path:            defaultPath,
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		Leader:          true,
	}

	if err := os.MkdirAll(defaultPath, 0750); err != nil {
		slog.Error("Failed to create default DB directory", "path", defaultPath, "error", err)
		return nil
	}

	clusterCfg := s3db.DefaultClusterConfig()
	clusterCfg.NodeID = 1
	clusterCfg.Nodes = []s3db.DBNodeConfig{dbNode}
	clusterCfg.DataDir = defaultPath
	clusterCfg.Bootstrap = true

	serverCfg := &s3db.ServerConfig{
		Addr:          dbNode.HTTPAddr(),
		TLSCert:       s.tlsCert,
		TLSKey:        s.tlsKey,
		Credentials:   map[string]string{accessKeyID: secretAccessKey},
		Region:        s.config.Region,
		Service:       "s3db",
		ClusterConfig: clusterCfg,
	}

	server, err := s3db.NewServer(serverCfg)
	if err != nil {
		slog.Error("Failed to create default database server", "error", err)
		return nil
	}

	go func() {
		slog.Info("Starting default embedded database", "addr", dbNode.HTTPAddr())
		if err := server.Start(); err != nil {
			slog.Error("Default database server failed", "error", err)
		}
	}()

	return []*s3db.Server{server}
}

// waitForDBLeader waits for leader election in the DB cluster
func (s *Server) waitForDBLeader() {
	if len(s.dbServers) == 0 {
		return
	}

	slog.Info("Waiting for database leader election...")
	timeout := 30 * time.Second

	for _, srv := range s.dbServers {
		if err := srv.WaitForLeader(timeout); err != nil {
			slog.Warn("Timeout waiting for leader on node", "error", err)
		} else {
			slog.Info("Database leader elected", "leader", srv.Node().LeaderID())
			return
		}
	}

	slog.Warn("No leader elected within timeout, continuing anyway")
}

// createDistributedBackend creates a distributed storage backend
func (s *Server) createDistributedBackend() (backend.Backend, error) {
	nodes := make([]distributed.NodeConfig, len(s.config.Nodes))
	for i, n := range s.config.Nodes {

		n.Path = checkBaseDir(s.basePath, n.Path)
		n.DBPath = checkBaseDir(s.basePath, n.DBPath)

		nodes[i] = distributed.NodeConfig{
			ID:     n.ID,
			Host:   n.Host,
			Port:   n.Port,
			Path:   n.Path,
			DB:     n.DB,
			DBPort: n.DBPort,
			DBPath: n.DBPath,
			Leader: n.Leader,
			Epoch:  n.Epoch,
		}
	}

	buckets := make([]distributed.BucketConfig, len(s.config.Buckets))
	for i, b := range s.config.Buckets {
		buckets[i] = distributed.BucketConfig{
			Name:   b.Name,
			Region: b.Region,
			Type:   b.Type,
			Public: b.Public,
		}
	}

	// Determine data directories
	badgerDir := s.config.BadgerDir
	if badgerDir == "" {
		if s.nodeID > 0 {
			for _, n := range s.config.Nodes {
				if n.ID == s.nodeID && n.DBPath != "" {
					badgerDir = n.DBPath
					break
				}
			}
		}

		// Append base-dir if not Absolute Path in config
		if !filepath.IsAbs(badgerDir) && s.basePath != "" && badgerDir != "" {
			badgerDir = filepath.Join(s.basePath, badgerDir)
		} else if badgerDir == "" {
			badgerDir = filepath.Join(s.basePath, "distributed/db")
		}

	}

	if err := os.MkdirAll(badgerDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create badger directory: %w", err)
	}

	dataDir := ""
	if s.nodeID > 0 {
		for _, n := range s.config.Nodes {
			if n.ID == s.nodeID {
				dataDir = filepath.Dir(n.Path)
				break
			}
		}
	}

	// Append base-dir if not Absolute Path in config
	if !filepath.IsAbs(dataDir) && s.basePath != "" && dataDir != "" {
		dataDir = filepath.Join(s.basePath, "distributed/nodes")
	} else if dataDir == "" {
		dataDir = filepath.Join(s.basePath, "distributed/nodes")
	}

	// Build database client configuration
	var dbClientConfig *distributed.DBClientConfig
	if len(s.config.DB) > 0 {
		dbNodes := make([]string, len(s.config.DB))
		for i, n := range s.config.DB {
			dbNodes[i] = fmt.Sprintf("%s:%d", n.Host, n.Port)
		}

		var accessKeyID, secretAccessKey string
		if s.config.DB[0].AccessKeyID != "" {
			accessKeyID = s.config.DB[0].AccessKeyID
			secretAccessKey = s.config.DB[0].SecretAccessKey
		} else if len(s.config.Auth) > 0 {
			accessKeyID = s.config.Auth[0].AccessKeyID
			secretAccessKey = s.config.Auth[0].SecretAccessKey
		}

		dbClientConfig = &distributed.DBClientConfig{
			Nodes:           dbNodes,
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
			Region:          s.config.Region,
		}
	} else if len(s.dbServers) > 0 {
		dbClientConfig = &distributed.DBClientConfig{
			Nodes:  []string{"127.0.0.1:6660"},
			Region: s.config.Region,
		}
		if len(s.config.Auth) > 0 {
			dbClientConfig.AccessKeyID = s.config.Auth[0].AccessKeyID
			dbClientConfig.SecretAccessKey = s.config.Auth[0].SecretAccessKey
		} else {
			dbClientConfig.AccessKeyID = "predastore"
			dbClientConfig.SecretAccessKey = "predastore"
		}
	}

	cfg := &distributed.Config{
		DataDir:           dataDir,
		BadgerDir:         badgerDir,
		DataShards:        s.config.RS.Data,
		ParityShards:      s.config.RS.Parity,
		PartitionCount:    len(s.config.Nodes),
		ReplicationFactor: 100,
		QuicBasePort:      9991,
		UseQUIC:           true,
		Nodes:             nodes,
		Buckets:           buckets,
		DBClient:          dbClientConfig,
	}

	be, err := distributed.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create distributed backend: %w", err)
	}

	slog.Info("Initialized distributed backend",
		"nodes", len(nodes),
		"buckets", len(buckets),
		"dataShards", s.config.RS.Data,
		"parityShards", s.config.RS.Parity,
		"badgerDir", badgerDir,
		"dataDir", dataDir,
	)
	return be, nil
}

// launchQUICServers starts QUIC servers for shard storage
func (s *Server) launchQUICServers() {
	if len(s.config.Nodes) == 0 {
		slog.Warn("No nodes configured, skipping QUIC server launch")
		return
	}

	if s.nodeID > 0 {
		// Launch specific node
		for _, n := range s.config.Nodes {
			if n.ID == s.nodeID {
				// Apply base-dir to relative paths
				nodePath := checkBaseDir(s.basePath, n.Path)

				quicAddr := fmt.Sprintf("%s:%d", n.Host, n.Port)
				slog.Info("Launching QUIC server for node", "nodeID", s.nodeID, "path", nodePath, "addr", quicAddr)

				if err := os.MkdirAll(nodePath, 0750); err != nil {
					slog.Error("Failed to create node directory", "path", nodePath, "error", err)
					return
				}

				quicserver.New(nodePath, quicAddr)
				return
			}
		}
		slog.Error("Node ID not found in configuration", "nodeID", s.nodeID)
		return
	}

	// Dev mode: launch all nodes locally
	if allHostsSame(s.config.Nodes) {
		slog.Info("Dev mode: all nodes have same host, launching all QUIC servers as goroutines")
		for _, n := range s.config.Nodes {
			// Apply base-dir to relative paths
			nodePath := checkBaseDir(s.basePath, n.Path)

			quicAddr := fmt.Sprintf("%s:%d", n.Host, n.Port)
			slog.Info("Launching QUIC server", "nodeID", n.ID, "path", nodePath, "addr", quicAddr)

			if err := os.MkdirAll(nodePath, 0750); err != nil {
				slog.Error("Failed to create node directory", "path", nodePath, "error", err)
				continue
			}

			quicserver.New(nodePath, quicAddr)
		}
	} else {
		slog.Error("Distributed mode requires -node flag when nodes have different hosts")
	}
}

// ListenAndServe starts the server and blocks until shutdown
func (s *Server) ListenAndServe() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}
	s.running = true
	s.mu.Unlock()

	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	slog.Info("Starting S3 server", "host", s.host, "port", s.port, "backend", s.backendType)

	if s.tlsCert == "" || s.tlsKey == "" {
		return fmt.Errorf("TLS is required - set tlsCert and tlsKey")
	}

	slog.Info(">>> USING HTTP/2 SERVER (net/http + chi) <<<", "addr", addr)
	return s.server.ListenAndServe(addr, s.tlsCert, s.tlsKey)
}

// ListenAndServeAsync starts the server in a goroutine
func (s *Server) ListenAndServeAsync() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}
	s.running = true
	s.mu.Unlock()

	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	slog.Info("Starting S3 server (async)", "host", s.host, "port", s.port, "backend", s.backendType)

	if s.tlsCert == "" || s.tlsKey == "" {
		return fmt.Errorf("TLS is required - set tlsCert and tlsKey")
	}

	go func() {
		slog.Info(">>> USING HTTP/2 SERVER (net/http + chi) <<<", "addr", addr)
		if err := s.server.ListenAndServe(addr, s.tlsCert, s.tlsKey); err != nil {
			slog.Error("Server error", "error", err)
		}
	}()

	return nil
}

// startProfiling starts CPU profiling to a temp file
func (s *Server) startProfiling() error {
	// Create temp file for profiling
	tmpFile, err := os.CreateTemp("", "predastore-cpu-*.prof.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp profile file: %w", err)
	}
	s.pprofFile = tmpFile

	if err := pprof.StartCPUProfile(tmpFile); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		return fmt.Errorf("failed to start CPU profile: %w", err)
	}

	slog.Info("CPU profiling started", "tempFile", tmpFile.Name(), "outputPath", s.pprofOutputPath)
	return nil
}

// stopProfiling stops CPU profiling and saves the profile to the output path
func (s *Server) stopProfiling() error {
	if s.pprofFile == nil {
		return nil
	}

	pprof.StopCPUProfile()
	tempPath := s.pprofFile.Name()
	s.pprofFile.Close()

	// Copy temp file to output path
	if err := copyFile(tempPath, s.pprofOutputPath); err != nil {
		slog.Error("Failed to save CPU profile", "error", err, "tempPath", tempPath)
		return err
	}

	// Remove temp file
	os.Remove(tempPath)

	slog.Info("CPU profile saved", "path", s.pprofOutputPath)
	return nil
}

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(dst), 0750); err != nil {
		return err
	}

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := dstFile.ReadFrom(srcFile); err != nil {
		return err
	}
	return dstFile.Sync()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	slog.Info("Shutting down S3 server...")

	// Shutdown HTTP server first (stop accepting new requests)
	slog.Info("Shutting down HTTP server...")
	if s.server != nil {
		if err := s.server.Shutdown(ctx); err != nil {
			slog.Error("Error shutting down HTTP server", "error", err)
		}
	}

	// Close backend (stops accepting new storage operations)
	if s.backend != nil {
		slog.Info("Closing storage backend...")
		s.backend.Close()
	}

	// Shutdown DB servers in parallel with timeout
	// Each Raft node has its own 5s timeout, but we also impose an overall limit
	if len(s.dbServers) > 0 {
		slog.Info("Shutting down DB servers...", "count", len(s.dbServers))

		var wg sync.WaitGroup
		for i, srv := range s.dbServers {
			wg.Add(1)
			go func(idx int, server *s3db.Server) {
				defer wg.Done()
				slog.Info("Shutting down DB server", "index", idx)
				server.Shutdown()
				slog.Info("DB server shutdown complete", "index", idx)
			}(i, srv)
		}

		// Wait for all servers with overall timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			slog.Info("All DB servers shut down successfully")
		case <-time.After(15 * time.Second):
			slog.Warn("DB server shutdown timed out after 15s, continuing...")
		case <-ctx.Done():
			slog.Warn("Shutdown context cancelled, continuing...")
		}
	}

	// Stop profiling and save profile
	if s.pprofEnabled {
		if err := s.stopProfiling(); err != nil {
			slog.Error("Error stopping CPU profile", "error", err)
		}
	}

	s.running = false
	close(s.shutdown)

	slog.Info("S3 server shutdown complete")
	return nil
}

// WaitForShutdownSignal blocks until SIGINT or SIGTERM is received
func (s *Server) WaitForShutdownSignal() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}

// Helper functions

func allDBHostsSame(nodes []s3db.DBNodeConfig) bool {
	if len(nodes) == 0 {
		return true
	}
	firstHost := nodes[0].Host
	for _, n := range nodes[1:] {
		if n.Host != firstHost {
			return false
		}
	}
	return true
}

func allHostsSame(nodes []Nodes) bool {
	if len(nodes) == 0 {
		return true
	}
	firstHost := nodes[0].Host
	for _, n := range nodes[1:] {
		if n.Host != firstHost {
			return false
		}
	}
	return true
}

// Base directory checks
func checkBaseDir(baseDir, path string) (newpath string) {

	if path == "" {
		return
	}

	// Append base-dir if not Absolute Path in config
	if !filepath.IsAbs(path) && baseDir != "" {
		newpath = filepath.Join(baseDir, path)
	} else {
		newpath = path
	}

	slog.Info("checkBaseDir", "baseDir", baseDir, "path", path, "newpath", newpath)

	return newpath
}
