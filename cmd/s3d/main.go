package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/distributed"
	"github.com/mulgadc/predastore/backend/filesystem"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/mulgadc/predastore/s3"
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
		be, err = createDistributedBackend(s3Config, *nodeID)
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

	// Setup HTTP routes with the backend
	app := s3Config.SetupRoutesWithBackend(be)

	// Start the S3 server
	slog.Info("Starting S3 server", "host", *host, "port", *port, "backend", *backendType)
	log.Fatal(app.ListenTLS(fmt.Sprintf("%s:%d", *host, *port), *tlsCert, *tlsKey))
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
func createDistributedBackend(s3Config *s3.Config, nodeID int) (backend.Backend, error) {
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

	// Determine badger directory
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
