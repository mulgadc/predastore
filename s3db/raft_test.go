package s3db

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSingleNodeRaft tests basic operations on a single Raft node
func TestSingleNodeRaft(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raft-test-single-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := DefaultClusterConfig()
	config.NodeID = 1
	config.DataDir = tmpDir
	config.Bootstrap = true
	config.Nodes = []DBNodeConfig{
		{ID: 1, Host: "127.0.0.1", Port: 16661, RaftPort: 17661, Path: filepath.Join(tmpDir, "node-1")},
	}

	node, err := NewRaftNode(config)
	require.NoError(t, err)
	defer node.Close()

	// Wait for leader election
	err = node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Verify this node is the leader
	assert.True(t, node.IsLeader())

	// Test Put
	err = node.Put("test-table", "key1", []byte("value1"))
	require.NoError(t, err)

	// Test Get
	value, err := node.Get("test-table", "key1")
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), value)

	// Test overwrite
	err = node.Put("test-table", "key1", []byte("value1-updated"))
	require.NoError(t, err)

	value, err = node.Get("test-table", "key1")
	require.NoError(t, err)
	assert.Equal(t, []byte("value1-updated"), value)

	// Test multiple keys
	for i := range 10 {
		key := fmt.Sprintf("key-%d", i)
		val := fmt.Sprintf("value-%d", i)
		err = node.Put("test-table", key, []byte(val))
		require.NoError(t, err)
	}

	// Test Scan
	var keys []string
	err = node.Scan("test-table", "key-", func(key string, value []byte) error {
		keys = append(keys, key)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, keys, 10)

	// Test Delete
	err = node.Delete("test-table", "key-0")
	require.NoError(t, err)

	_, err = node.Get("test-table", "key-0")
	assert.Error(t, err) // Should fail - key deleted

	// Verify key-1 still exists
	value, err = node.Get("test-table", "key-1")
	require.NoError(t, err)
	assert.Equal(t, []byte("value-1"), value)
}

// TestMultipleTablesIsolation tests that tables are isolated from each other
func TestMultipleTablesIsolation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raft-test-tables-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := DefaultClusterConfig()
	config.NodeID = 1
	config.DataDir = tmpDir
	config.Bootstrap = true
	config.Nodes = []DBNodeConfig{
		{ID: 1, Host: "127.0.0.1", Port: 16662, RaftPort: 17662, Path: filepath.Join(tmpDir, "node-1")},
	}

	node, err := NewRaftNode(config)
	require.NoError(t, err)
	defer node.Close()

	err = node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Put same key in different tables
	err = node.Put("buckets", "my-bucket", []byte("bucket-metadata"))
	require.NoError(t, err)

	err = node.Put("objects", "my-bucket", []byte("object-metadata"))
	require.NoError(t, err)

	// Verify isolation
	val1, err := node.Get("buckets", "my-bucket")
	require.NoError(t, err)
	assert.Equal(t, []byte("bucket-metadata"), val1)

	val2, err := node.Get("objects", "my-bucket")
	require.NoError(t, err)
	assert.Equal(t, []byte("object-metadata"), val2)

	// Delete from one table shouldn't affect other
	err = node.Delete("buckets", "my-bucket")
	require.NoError(t, err)

	_, err = node.Get("buckets", "my-bucket")
	assert.Error(t, err) // Deleted

	val2, err = node.Get("objects", "my-bucket")
	require.NoError(t, err)
	assert.Equal(t, []byte("object-metadata"), val2) // Still exists
}

// TestThreeNodeCluster tests a 3-node Raft cluster
func TestThreeNodeCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping cluster test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "raft-test-cluster-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create 3-node cluster config
	nodes := []DBNodeConfig{
		{ID: 1, Host: "127.0.0.1", Port: 16671, RaftPort: 17671, Path: filepath.Join(tmpDir, "node-1")},
		{ID: 2, Host: "127.0.0.1", Port: 16672, RaftPort: 17672, Path: filepath.Join(tmpDir, "node-2")},
		{ID: 3, Host: "127.0.0.1", Port: 16673, RaftPort: 17673, Path: filepath.Join(tmpDir, "node-3")},
	}

	var raftNodes []*RaftNode

	// Start all nodes
	for i, nodeConfig := range nodes {
		config := DefaultClusterConfig()
		config.NodeID = nodeConfig.ID
		config.DataDir = tmpDir
		config.Bootstrap = (i == 0) // Only first node bootstraps
		config.Nodes = nodes

		node, err := NewRaftNode(config)
		require.NoError(t, err)
		raftNodes = append(raftNodes, node)
	}

	defer func() {
		for _, n := range raftNodes {
			n.Close()
		}
	}()

	// Wait for leader election
	time.Sleep(3 * time.Second)

	// Find the leader
	var leader *RaftNode
	for _, n := range raftNodes {
		if n.IsLeader() {
			leader = n
			break
		}
	}
	require.NotNil(t, leader, "No leader elected")

	t.Logf("Leader: node %s", leader.LeaderID())

	// Write to leader
	err = leader.Put("test", "cluster-key", []byte("cluster-value"))
	require.NoError(t, err)

	// Wait for replication
	time.Sleep(500 * time.Millisecond)

	// Read from all nodes (should be replicated)
	for i, n := range raftNodes {
		value, err := n.Get("test", "cluster-key")
		require.NoError(t, err, "Node %d failed to read", i+1)
		assert.Equal(t, []byte("cluster-value"), value, "Node %d has wrong value", i+1)
	}
}

// TestLeaderFailover tests that cluster continues after leader failure
func TestLeaderFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping failover test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "raft-test-failover-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create 3-node cluster config
	nodes := []DBNodeConfig{
		{ID: 1, Host: "127.0.0.1", Port: 16681, RaftPort: 17681, Path: filepath.Join(tmpDir, "node-1")},
		{ID: 2, Host: "127.0.0.1", Port: 16682, RaftPort: 17682, Path: filepath.Join(tmpDir, "node-2")},
		{ID: 3, Host: "127.0.0.1", Port: 16683, RaftPort: 17683, Path: filepath.Join(tmpDir, "node-3")},
	}

	var raftNodes []*RaftNode
	var mu sync.Mutex

	// Start all nodes
	for i, nodeConfig := range nodes {
		config := DefaultClusterConfig()
		config.NodeID = nodeConfig.ID
		config.DataDir = tmpDir
		config.Bootstrap = (i == 0)
		config.Nodes = nodes
		// Faster timeouts for testing
		config.HeartbeatTimeout = 200 * time.Millisecond
		config.ElectionTimeout = 200 * time.Millisecond
		config.LeaderLeaseTimeout = 100 * time.Millisecond

		node, err := NewRaftNode(config)
		require.NoError(t, err)
		raftNodes = append(raftNodes, node)
	}

	defer func() {
		mu.Lock()
		for _, n := range raftNodes {
			if n != nil {
				n.Close()
			}
		}
		mu.Unlock()
	}()

	// Wait for leader election
	time.Sleep(2 * time.Second)

	// Find and write to leader
	var leaderIdx int
	for i, n := range raftNodes {
		if n.IsLeader() {
			leaderIdx = i
			break
		}
	}

	leader := raftNodes[leaderIdx]
	err = leader.Put("test", "failover-key", []byte("before-failover"))
	require.NoError(t, err)

	// Wait for replication
	time.Sleep(500 * time.Millisecond)

	// Kill the leader
	t.Logf("Killing leader (node %d)", leaderIdx+1)
	mu.Lock()
	leader.Close()
	raftNodes[leaderIdx] = nil
	mu.Unlock()

	// Wait for new leader election
	time.Sleep(3 * time.Second)

	// Find new leader
	var newLeader *RaftNode
	mu.Lock()
	for _, n := range raftNodes {
		if n != nil && n.IsLeader() {
			newLeader = n
			break
		}
	}
	mu.Unlock()
	require.NotNil(t, newLeader, "No new leader elected after failover")

	t.Logf("New leader elected")

	// Verify data persisted
	value, err := newLeader.Get("test", "failover-key")
	require.NoError(t, err)
	assert.Equal(t, []byte("before-failover"), value)

	// Write to new leader
	err = newLeader.Put("test", "failover-key-2", []byte("after-failover"))
	require.NoError(t, err)

	// Verify write succeeded
	value, err = newLeader.Get("test", "failover-key-2")
	require.NoError(t, err)
	assert.Equal(t, []byte("after-failover"), value)
}

// TestConcurrentWrites tests concurrent writes to the cluster
func TestConcurrentWrites(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raft-test-concurrent-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := DefaultClusterConfig()
	config.NodeID = 1
	config.DataDir = tmpDir
	config.Bootstrap = true
	config.Nodes = []DBNodeConfig{
		{ID: 1, Host: "127.0.0.1", Port: 16691, RaftPort: 17691, Path: filepath.Join(tmpDir, "node-1")},
	}

	node, err := NewRaftNode(config)
	require.NoError(t, err)
	defer node.Close()

	err = node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Concurrent writes
	numWriters := 10
	numWrites := 100
	var wg sync.WaitGroup
	errors := make(chan error, numWriters*numWrites)

	for w := range numWriters {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := range numWrites {
				key := fmt.Sprintf("writer-%d-key-%d", writerID, i)
				value := fmt.Sprintf("writer-%d-value-%d", writerID, i)
				if err := node.Put("concurrent", key, []byte(value)); err != nil {
					errors <- fmt.Errorf("writer %d write %d failed: %w", writerID, i, err)
				}
			}
		}(w)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Error(err)
	}

	// Verify all writes
	for w := range numWriters {
		for i := range numWrites {
			key := fmt.Sprintf("writer-%d-key-%d", w, i)
			expectedValue := fmt.Sprintf("writer-%d-value-%d", w, i)
			value, err := node.Get("concurrent", key)
			require.NoError(t, err, "Failed to get %s", key)
			assert.Equal(t, []byte(expectedValue), value)
		}
	}

	t.Logf("Successfully wrote and verified %d keys", numWriters*numWrites)
}

// TestLargeValues tests storing large values
func TestLargeValues(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "raft-test-large-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	config := DefaultClusterConfig()
	config.NodeID = 1
	config.DataDir = tmpDir
	config.Bootstrap = true
	config.Nodes = []DBNodeConfig{
		{ID: 1, Host: "127.0.0.1", Port: 16692, RaftPort: 17692, Path: filepath.Join(tmpDir, "node-1")},
	}

	node, err := NewRaftNode(config)
	require.NoError(t, err)
	defer node.Close()

	err = node.WaitForLeader(5 * time.Second)
	require.NoError(t, err)

	// Test various sizes
	sizes := []int{1024, 10 * 1024, 100 * 1024, 1024 * 1024} // 1KB, 10KB, 100KB, 1MB

	for _, size := range sizes {
		t.Run(fmt.Sprintf("%dB", size), func(t *testing.T) {
			// Generate test data
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			key := fmt.Sprintf("large-%d", size)
			err := node.Put("large-test", key, data)
			require.NoError(t, err)

			value, err := node.Get("large-test", key)
			require.NoError(t, err)
			assert.Equal(t, data, value)
		})
	}
}
