package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/distributed"
	"github.com/mulgadc/predastore/backend/filesystem"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/mulgadc/predastore/s3"
	"github.com/mulgadc/predastore/s3db"
	"go.uber.org/automaxprocs/maxprocs"
)

func main() {
	config := flag.String("config", "config/server.toml", "S3 server configuration file")
	tlsKey := flag.String("tls-key", "config/server.key", "Path to TLS key")
	tlsCert := flag.String("tls-cert", "config/server.pem", "Path to TLS cert")
	basePath := flag.String("base-path", "", "Base path for the S3 directory when undefined in the config file")
	debug := flag.Bool("debug", false, "Enable verbose debug logs")
	port := flag.Int("port", 443, "Server port")
	host := flag.String("host", "0.0.0.0", "Server host")
	backendType := flag.String("backend", "filesystem", "Storage backend type (filesystem, distributed)")
	nodeID := flag.Int("node", -1, "Node ID to run (distributed mode only, -1 = dev mode runs all nodes)")

	// Database mode flags
	dbOnly := flag.Bool("db", false, "Run only the distributed database server")
	dbNode := flag.Int("db-node", -1, "Database node ID to run (-1 = auto-detect or run all in dev mode)")

	flag.Parse()

	// Environment variables override CLI options
	if os.Getenv("CONFIG") != "" {
		*config = os.Getenv("CONFIG")
	}
	if os.Getenv("TLS_KEY") != "" {
		*tlsKey = os.Getenv("TLS_KEY")
	}
	if os.Getenv("TLS_CERT") != "" {
		*tlsCert = os.Getenv("TLS_CERT")
	}
	if os.Getenv("PORT") != "" {
		*port, _ = strconv.Atoi(os.Getenv("PORT"))
	}
	if os.Getenv("BACKEND") != "" {
		*backendType = os.Getenv("BACKEND")
	}
	if os.Getenv("NODE") != "" {
		*nodeID, _ = strconv.Atoi(os.Getenv("NODE"))
	}
	if os.Getenv("DB_ONLY") == "true" {
		*dbOnly = true
	}
	if os.Getenv("DB_NODE") != "" {
		*dbNode, _ = strconv.Atoi(os.Getenv("DB_NODE"))
	}

	// Initialize S3 configuration
	s3Config := s3.New(&s3.Config{
		ConfigPath: *config,
		Port:       *port,
		Host:       *host,
		Debug:      *debug,
		BasePath:   *basePath,
	})

	// Adjust MAXPROCS for cgroups environments
	undo, err := maxprocs.Set(maxprocs.Logger(log.Printf))
	if err != nil {
		log.Printf("Failed to set GOMAXPROCS: %v", err)
	} else {
		defer undo()
	}

	// Read configuration file
	if err := s3Config.ReadConfig(); err != nil {
		slog.Error("Error reading config file", "error", err)
		os.Exit(1)
	}

	// Database-only mode
	if *dbOnly {
		runDatabaseOnly(s3Config, *dbNode, *tlsCert, *tlsKey)
		return
	}

	// Check if we need to run distributed database
	hasDBNodes := len(s3Config.DB) > 0
	hasDistributedNodes := len(s3Config.Nodes) > 0 && *backendType == "distributed"

	// If distributed backend with DB nodes, launch DB service(s) first
	var dbServers []*s3db.Server
	if hasDistributedNodes {
		if hasDBNodes {
			// Explicit DB nodes defined - launch them
			dbServers = launchDBServers(s3Config, *dbNode, *tlsCert, *tlsKey)
		} else {
			// No explicit DB nodes - launch default embedded DB
			dbServers = launchDefaultDB(s3Config, *tlsCert, *tlsKey)
		}

		// Wait for leader election if we launched DB servers
		if len(dbServers) > 0 {
			waitForDBLeader(dbServers)
		}
	}

	// Create the storage backend
	var be backend.Backend

	switch *backendType {
	case "filesystem":
		be, err = createFilesystemBackend(s3Config)
		if err != nil {
			slog.Error("Failed to initialize filesystem backend", "error", err)
			os.Exit(1)
		}

	case "distributed":
		be, err = createDistributedBackend(s3Config, *nodeID, dbServers)
		if err != nil {
			slog.Error("Failed to initialize distributed backend", "error", err)
			os.Exit(1)
		}

		// Launch QUIC server(s) based on node configuration
		launchQUICServers(s3Config, *nodeID)

	default:
		slog.Error("Unknown backend type", "type", *backendType)
		os.Exit(1)
	}

	defer be.Close()

	// Setup graceful shutdown
	setupShutdown(func() {
		be.Close()
		for _, srv := range dbServers {
			srv.Shutdown()
		}
	})

	// Setup HTTP routes with the backend
	app := s3Config.SetupRoutesWithBackend(be)

	// Start the S3 server
	slog.Info("Starting S3 server", "host", *host, "port", *port, "backend", *backendType)
	log.Fatal(app.ListenTLS(fmt.Sprintf("%s:%d", *host, *port), *tlsCert, *tlsKey))
}

// runDatabaseOnly runs only the distributed database server
func runDatabaseOnly(s3Config *s3.Config, dbNode int, tlsCert, tlsKey string) {
	if len(s3Config.DB) == 0 {
		slog.Error("No database nodes configured in config file")
		slog.Error("Add [[db]] sections to your configuration")
		os.Exit(1)
	}

	dbServers := launchDBServers(s3Config, dbNode, tlsCert, tlsKey)
	if len(dbServers) == 0 {
		slog.Error("No database servers launched")
		os.Exit(1)
	}

	// Wait for leader election
	waitForDBLeader(dbServers)

	slog.Info("Database server(s) running", "nodes", len(dbServers))

	// Setup graceful shutdown and wait
	setupShutdown(func() {
		for _, srv := range dbServers {
			srv.Shutdown()
		}
	})

	// Block forever
	select {}
}

// launchDBServers starts the distributed database servers
func launchDBServers(s3Config *s3.Config, dbNode int, tlsCert, tlsKey string) []*s3db.Server {
	var servers []*s3db.Server

	// Convert s3.DBNode to s3db.DBNodeConfig
	dbNodes := make([]s3db.DBNodeConfig, len(s3Config.DB))
	for i, n := range s3Config.DB {
		dbNodes[i] = s3db.DBNodeConfig{
			ID:              uint64(n.ID),
			Host:            n.Host,
			Port:            n.Port,
			RaftPort:        n.RaftPort,
			Path:            n.Path,
			AccessKeyID:     n.AccessKeyID,
			SecretAccessKey: n.SecretAccessKey,
			Leader:          n.Leader,
		}
	}

	// Build credentials map for auth
	credentials := make(map[string]string)
	for _, n := range s3Config.DB {
		if n.AccessKeyID != "" && n.SecretAccessKey != "" {
			credentials[n.AccessKeyID] = n.SecretAccessKey
		}
	}
	// Also add auth entries from main config
	for _, auth := range s3Config.Auth {
		if auth.AccessKeyID != "" && auth.SecretAccessKey != "" {
			credentials[auth.AccessKeyID] = auth.SecretAccessKey
		}
	}

	// Determine which nodes to launch
	var nodesToLaunch []s3db.DBNodeConfig
	if dbNode > 0 {
		// Launch specific node
		for _, n := range dbNodes {
			if n.ID == uint64(dbNode) {
				nodesToLaunch = append(nodesToLaunch, n)
				break
			}
		}
		if len(nodesToLaunch) == 0 {
			slog.Error("Database node not found", "id", dbNode)
			os.Exit(1)
		}
	} else if allDBHostsSame(dbNodes) {
		// Dev mode: launch all nodes
		nodesToLaunch = dbNodes
		slog.Info("Dev mode: launching all database nodes locally")
	} else {
		slog.Error("Distributed mode requires -db-node flag when DB nodes have different hosts")
		slog.Error("Use: ./s3d -db -db-node <id> to run a specific database node")
		os.Exit(1)
	}

	// Determine if we're bootstrapping
	bootstrap := true
	for _, n := range nodesToLaunch {
		// Check if data directory exists and has data
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

		// Ensure data directory exists
		if err := os.MkdirAll(node.Path, 0750); err != nil {
			slog.Error("Failed to create DB node directory", "path", node.Path, "error", err)
			continue
		}

		serverCfg := &s3db.ServerConfig{
			Addr:        node.HTTPAddr(),
			TLSCert:     tlsCert,
			TLSKey:      tlsKey,
			Credentials: credentials,
			Region:      s3Config.Region,
			Service:     "s3db",
			ClusterConfig: clusterCfg,
		}

		server, err := s3db.NewServer(serverCfg)
		if err != nil {
			slog.Error("Failed to create database server", "nodeID", node.ID, "error", err)
			continue
		}

		servers = append(servers, server)

		// Start server in goroutine
		go func(srv *s3db.Server, nodeID uint64) {
			slog.Info("Starting database server", "nodeID", nodeID, "addr", node.HTTPAddr())
			if err := srv.Start(); err != nil {
				slog.Error("Database server failed", "nodeID", nodeID, "error", err)
			}
		}(server, node.ID)
	}

	return servers
}

// launchDefaultDB creates a default embedded database when no [[db]] is configured
func launchDefaultDB(s3Config *s3.Config, tlsCert, tlsKey string) []*s3db.Server {
	slog.Info("No [[db]] configuration found, launching default embedded database")

	// Create default DB configuration
	defaultPort := 6660
	defaultPath := filepath.Join(s3Config.BasePath, "db")
	if defaultPath == "" {
		defaultPath = "data/db"
	}

	// Use first auth entry for DB auth, or generate default
	var accessKeyID, secretAccessKey string
	if len(s3Config.Auth) > 0 {
		accessKeyID = s3Config.Auth[0].AccessKeyID
		secretAccessKey = s3Config.Auth[0].SecretAccessKey
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

	// Ensure directory exists
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
		TLSCert:       tlsCert,
		TLSKey:        tlsKey,
		Credentials:   map[string]string{accessKeyID: secretAccessKey},
		Region:        s3Config.Region,
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
func waitForDBLeader(servers []*s3db.Server) {
	if len(servers) == 0 {
		return
	}

	slog.Info("Waiting for database leader election...")
	timeout := 30 * time.Second

	for _, srv := range servers {
		if err := srv.WaitForLeader(timeout); err != nil {
			slog.Warn("Timeout waiting for leader on node", "error", err)
		} else {
			slog.Info("Database leader elected", "leader", srv.Node().LeaderID())
			return
		}
	}

	slog.Warn("No leader elected within timeout, continuing anyway")
}

// allDBHostsSame checks if all database nodes have the same host
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

// createFilesystemBackend creates a filesystem storage backend
func createFilesystemBackend(s3Config *s3.Config) (backend.Backend, error) {
	buckets := make([]filesystem.BucketConfig, len(s3Config.Buckets))
	for i, b := range s3Config.Buckets {
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
		return nil, err
	}
	slog.Info("Initialized filesystem backend", "buckets", len(buckets))
	return be, nil
}

// createDistributedBackend creates a distributed storage backend
func createDistributedBackend(s3Config *s3.Config, nodeID int, dbServers []*s3db.Server) (backend.Backend, error) {
	// Convert s3.Nodes to distributed.NodeConfig
	nodes := make([]distributed.NodeConfig, len(s3Config.Nodes))
	for i, n := range s3Config.Nodes {
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

	// Convert s3.S3_Buckets to distributed.BucketConfig
	buckets := make([]distributed.BucketConfig, len(s3Config.Buckets))
	for i, b := range s3Config.Buckets {
		buckets[i] = distributed.BucketConfig{
			Name:   b.Name,
			Region: b.Region,
			Type:   b.Type,
			Public: b.Public,
		}
	}

	// Determine badger directory for local state (deprecated, now uses distributed DB)
	badgerDir := s3Config.BadgerDir
	if badgerDir == "" {
		// Default based on node ID or first node
		if nodeID > 0 {
			// Find the node config
			for _, n := range s3Config.Nodes {
				if n.ID == nodeID && n.DBPath != "" {
					badgerDir = n.DBPath
					break
				}
			}
		}
		if badgerDir == "" {
			badgerDir = "s3/tests/data/distributed/db"
		}
	}

	// Create badger directory if it doesn't exist
	if err := os.MkdirAll(badgerDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create badger directory: %w", err)
	}

	// Determine data directory
	dataDir := ""
	if nodeID > 0 {
		for _, n := range s3Config.Nodes {
			if n.ID == nodeID {
				dataDir = filepath.Dir(n.Path) // Parent of node path
				break
			}
		}
	}
	if dataDir == "" {
		dataDir = "s3/tests/data/distributed/nodes"
	}

	// Build database client configuration
	var dbClientConfig *distributed.DBClientConfig
	if len(s3Config.DB) > 0 {
		// Use configured DB nodes
		dbNodes := make([]string, len(s3Config.DB))
		for i, n := range s3Config.DB {
			dbNodes[i] = fmt.Sprintf("%s:%d", n.Host, n.Port)
		}

		// Get credentials from first DB node or auth config
		var accessKeyID, secretAccessKey string
		if s3Config.DB[0].AccessKeyID != "" {
			accessKeyID = s3Config.DB[0].AccessKeyID
			secretAccessKey = s3Config.DB[0].SecretAccessKey
		} else if len(s3Config.Auth) > 0 {
			accessKeyID = s3Config.Auth[0].AccessKeyID
			secretAccessKey = s3Config.Auth[0].SecretAccessKey
		}

		dbClientConfig = &distributed.DBClientConfig{
			Nodes:           dbNodes,
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
			Region:          s3Config.Region,
		}
	} else if len(dbServers) > 0 {
		// Use default embedded DB
		dbClientConfig = &distributed.DBClientConfig{
			Nodes:  []string{"127.0.0.1:6660"},
			Region: s3Config.Region,
		}
		if len(s3Config.Auth) > 0 {
			dbClientConfig.AccessKeyID = s3Config.Auth[0].AccessKeyID
			dbClientConfig.SecretAccessKey = s3Config.Auth[0].SecretAccessKey
		} else {
			dbClientConfig.AccessKeyID = "predastore"
			dbClientConfig.SecretAccessKey = "predastore"
		}
	}

	cfg := &distributed.Config{
		DataDir:           dataDir,
		BadgerDir:         badgerDir,
		DataShards:        s3Config.RS.Data,
		ParityShards:      s3Config.RS.Parity,
		PartitionCount:    len(s3Config.Nodes),
		ReplicationFactor: 100,
		QuicBasePort:      9991, // Base port for QUIC servers
		UseQUIC:           true, // Enable QUIC-based distribution
		Nodes:             nodes,
		Buckets:           buckets,
		DBClient:          dbClientConfig,
	}

	be, err := distributed.New(cfg)
	if err != nil {
		return nil, err
	}

	slog.Info("Initialized distributed backend",
		"nodes", len(nodes),
		"buckets", len(buckets),
		"dataShards", s3Config.RS.Data,
		"parityShards", s3Config.RS.Parity,
		"badgerDir", badgerDir,
		"dbNodes", len(s3Config.DB),
	)
	return be, nil
}

// launchQUICServers starts QUIC servers based on configuration and node selection
func launchQUICServers(s3Config *s3.Config, nodeID int) {
	if len(s3Config.Nodes) == 0 {
		slog.Warn("No nodes configured, skipping QUIC server launch")
		return
	}

	// If specific node requested, only launch that node
	if nodeID > 0 {
		for _, n := range s3Config.Nodes {
			if n.ID == nodeID {
				quicAddr := fmt.Sprintf("%s:%d", n.Host, n.Port)
				slog.Info("Launching QUIC server for node", "nodeID", nodeID, "path", n.Path, "addr", quicAddr)

				// Ensure node data directory exists
				if err := os.MkdirAll(n.Path, 0750); err != nil {
					slog.Error("Failed to create node directory", "path", n.Path, "error", err)
					os.Exit(1)
				}

				quicserver.New(n.Path, quicAddr)
				return
			}
		}
		slog.Error("Node ID not found in configuration", "nodeID", nodeID)
		os.Exit(1)
	}

	// Dev mode: check if all hosts are the same (allows running all nodes locally)
	if allHostsSame(s3Config.Nodes) {
		slog.Info("Dev mode: all nodes have same host, launching all QUIC servers as goroutines")
		for _, n := range s3Config.Nodes {
			quicAddr := fmt.Sprintf("%s:%d", n.Host, n.Port)
			slog.Info("Launching QUIC server", "nodeID", n.ID, "path", n.Path, "addr", quicAddr)

			// Ensure node data directory exists
			if err := os.MkdirAll(n.Path, 0750); err != nil {
				slog.Error("Failed to create node directory", "path", n.Path, "error", err)
				continue
			}

			quicserver.New(n.Path, quicAddr)
		}
	} else {
		slog.Error("Distributed mode requires -node flag when nodes have different hosts")
		slog.Error("Use: ./s3d -backend distributed -config cluster.toml -node <id>")
		slog.Error("Or use scripts/launch-cluster.sh to launch all nodes")
		os.Exit(1)
	}
}

// allHostsSame checks if all nodes have the same host address
func allHostsSame(nodes []s3.Nodes) bool {
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

// setupShutdown handles graceful shutdown on SIGINT/SIGTERM
func setupShutdown(cleanup func()) {
	var once sync.Once
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		slog.Info("Received signal, shutting down", "signal", sig)
		once.Do(cleanup)
		os.Exit(0)
	}()
}
