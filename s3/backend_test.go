package s3

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/distributed"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/stretchr/testify/require"
)

// BackendType represents the type of backend to use for testing
type BackendType string

const (
	BackendFilesystem  BackendType = "filesystem"
	BackendDistributed BackendType = "distributed"
)

// TestBackend holds the configuration and app for a specific backend
type TestBackend struct {
	Type        BackendType
	Config      *Config
	App         *fiber.App
	Backend     backend.Backend
	QuicServers []*quicserver.QuicServer
	Cleanup     func()
}

// setupFilesystemBackend creates a filesystem backend for testing
func setupFilesystemBackend(t *testing.T) *TestBackend {
	t.Helper()

	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	require.NoError(t, err, "Should read config without error")

	be := s3.createFilesystemBackend()
	app := s3.SetupRoutesWithBackend(be)

	return &TestBackend{
		Type:    BackendFilesystem,
		Config:  s3,
		App:     app,
		Backend: be,
		Cleanup: func() {
			be.Close()
		},
	}
}

// setupDistributedBackend creates a distributed backend for testing with QUIC servers
func setupDistributedBackend(t *testing.T) *TestBackend {
	t.Helper()

	// Create temporary directories for badger and node data
	tmpDir, err := os.MkdirTemp("", "distributed-test-*")
	require.NoError(t, err, "Should create temp directory")

	nodeDataDir := filepath.Join(tmpDir, "nodes")
	badgerDir := filepath.Join(tmpDir, "db")
	require.NoError(t, os.MkdirAll(nodeDataDir, 0750))
	require.NoError(t, os.MkdirAll(badgerDir, 0750))

	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "cluster.toml"),
	})
	err = s3.ReadConfig()
	require.NoError(t, err, "Should read config without error")

	// Override badger dir with temp directory
	s3.BadgerDir = badgerDir

	// Create distributed backend with QUIC enabled
	nodeCount := len(s3.Nodes)
	if nodeCount == 0 {
		nodeCount = 5 // Default to 5 nodes if none configured
	}

	// Convert s3 bucket configs to distributed bucket configs
	buckets := make([]distributed.BucketConfig, 0, len(s3.Buckets))
	for _, b := range s3.Buckets {
		buckets = append(buckets, distributed.BucketConfig{
			Name:   b.Name,
			Region: b.Region,
			Type:   b.Type,
			Public: b.Public,
		})
	}

	// Use a unique port range based on test timestamp to avoid conflicts
	basePort := 10000 + (int(time.Now().UnixNano()/1000000) % 5000)

	config := &distributed.Config{
		BadgerDir:      badgerDir,
		DataDir:        nodeDataDir,
		DataShards:     s3.RS.Data,
		ParityShards:   s3.RS.Parity,
		PartitionCount: nodeCount,
		UseQUIC:        true,
		QuicBasePort:   basePort,
		Buckets:        buckets,
	}

	be, err := distributed.New(config)
	require.NoError(t, err, "Should create distributed backend")

	// Start QUIC servers for each node
	quicServers := make([]*quicserver.QuicServer, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeDir := filepath.Join(nodeDataDir, fmt.Sprintf("node-%d", i))
		require.NoError(t, os.MkdirAll(nodeDir, 0750))

		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)
		quicServers[i] = quicserver.New(nodeDir, addr)
	}

	// Allow QUIC servers time to start
	time.Sleep(100 * time.Millisecond)

	app := s3.SetupRoutesWithBackend(be)

	return &TestBackend{
		Type:        BackendDistributed,
		Config:      s3,
		App:         app,
		Backend:     be,
		QuicServers: quicServers,
		Cleanup: func() {
			// Close QUIC servers first
			for _, qs := range quicServers {
				if qs != nil {
					_ = qs.Close()
				}
			}
			be.Close()
			os.RemoveAll(tmpDir)
		},
	}
}

// RunWithBackends runs a test function against multiple backends
func RunWithBackends(t *testing.T, backends []BackendType, testFn func(t *testing.T, tb *TestBackend)) {
	for _, backendType := range backends {
		backendType := backendType // capture range variable
		t.Run(string(backendType), func(t *testing.T) {
			var tb *TestBackend
			switch backendType {
			case BackendFilesystem:
				tb = setupFilesystemBackend(t)
			case BackendDistributed:
				tb = setupDistributedBackend(t)
			default:
				t.Fatalf("Unknown backend type: %s", backendType)
			}
			defer tb.Cleanup()

			testFn(t, tb)
		})
	}
}

// AllBackends returns all available backend types for testing
func AllBackends() []BackendType {
	return []BackendType{BackendFilesystem, BackendDistributed}
}

// FilesystemOnly returns only the filesystem backend for testing
func FilesystemOnly() []BackendType {
	return []BackendType{BackendFilesystem}
}

// DistributedOnly returns only the distributed backend for testing
func DistributedOnly() []BackendType {
	return []BackendType{BackendDistributed}
}
