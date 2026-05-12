package s3

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/distributed"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/stretchr/testify/require"
)

// s3BackendPortCounter gives each setupDistributedBackend invocation a unique
// port range so concurrent or rapidly-sequenced test setups don't collide on
// QUIC (UDP) ports the OS hasn't released yet.
var s3BackendPortCounter atomic.Int32

// testBucket is the bucket name used by all unit and integration tests.
const testBucket = "test"

// Note: BackendType and BackendDistributed are defined in server.go

// newAuthTestConfig returns an inline Config for tests that only exercise
// auth middleware (no real backend needed). Avoids reading any TOML file.
func newAuthTestConfig() *Config {
	return &Config{
		Version: "1.0",
		Region:  "ap-southeast-2",
		Buckets: []S3_Buckets{
			{Name: "test-bucket01", Region: "ap-southeast-2", Type: "distributed", Public: true},
			{Name: "private", Region: "ap-southeast-2", Type: "distributed", Public: false},
			{Name: "local", Region: "ap-southeast-2", Type: "distributed", Public: false},
		},
		Auth: []AuthEntry{
			{
				AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
				SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				Policy: []PolicyRule{
					{Bucket: "private", Actions: []string{"s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListAllMyBuckets"}},
					{Bucket: "local", Actions: []string{"s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListAllMyBuckets"}},
				},
			},
		},
	}
}

// TestBackend holds the configuration and server for a specific backend
type TestBackend struct {
	Type        BackendType
	Config      *Config
	Server      *HTTP2Server
	Handler     http.Handler
	Backend     backend.Backend
	QuicServers []*quicserver.QuicServer
	Cleanup     func()
}

// setupDistributedBackend creates a distributed backend for testing with QUIC servers
func setupDistributedBackend(t *testing.T) *TestBackend {
	t.Helper()

	// Create temporary directories for badger and node data
	tmpDir := t.TempDir()

	nodeDataDir := filepath.Join(tmpDir, "nodes")
	badgerDir := filepath.Join(tmpDir, "db")
	require.NoError(t, os.MkdirAll(nodeDataDir, 0750))
	require.NoError(t, os.MkdirAll(badgerDir, 0750))

	s3 := New(&Config{
		ConfigPath: filepath.Join("..", "config", "7node.toml"),
	})
	err := s3.ReadConfig()
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

	basePort := 14001 + int(s3BackendPortCounter.Add(1)-1)*20

	config := &distributed.Config{
		BadgerDir:      badgerDir,
		DataDir:        nodeDataDir,
		DataShards:     s3.RS.Data,
		ParityShards:   s3.RS.Parity,
		PartitionCount: nodeCount,
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
		quicServers[i] = quicserver.New(nodeDir, addr, quicserver.WithMasterKey(storetest.TestKey()))
	}

	// Allow QUIC servers time to start
	time.Sleep(100 * time.Millisecond)

	// Create the shared test bucket so tests don't have to.
	_, err = be.CreateBucket(context.Background(), &backend.CreateBucketRequest{
		Bucket:    testBucket,
		Region:    s3.Region,
		OwnerID:   s3.Auth[0].AccessKeyID,
		AccountID: s3.Auth[0].AccountID,
	})
	require.NoError(t, err, "CreateBucket %q should succeed", testBucket)

	server := NewHTTP2ServerWithBackend(s3, be, NewConfigProvider(s3.Auth))

	return &TestBackend{
		Type:        BackendDistributed,
		Config:      s3,
		Server:      server,
		Handler:     server.GetHandler(),
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
		},
	}
}

// RunWithBackends runs a test function against multiple backends
func RunWithBackends(t *testing.T, backends []BackendType, testFn func(t *testing.T, tb *TestBackend)) {
	for _, backendType := range backends {
		t.Run(string(backendType), func(t *testing.T) {
			var tb *TestBackend
			switch backendType {
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
	return []BackendType{BackendDistributed}
}

// DistributedOnly returns only the distributed backend for testing
func DistributedOnly() []BackendType {
	return []BackendType{BackendDistributed}
}
