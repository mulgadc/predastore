package blob_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/blob/engine"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	statHost       = "stat-test-host"
	statServerPort = 6457
	statClientPort = 6458
)

// failingLookupStore is a store whose index cannot be read, as a failing disk
// or a corrupt index row leaves it.
type failingLookupStore struct {
	*slowCommitStore

	err error
}

func (s failingLookupStore) Lookup([32]byte, uint32) (engine.Reader, error) { return nil, s.err }

// startStoreNode runs a blob node over the given store and returns a client
// addressed to it.
func startStoreNode(t *testing.T, store blob.Store) *blob.Client {
	t.Helper()

	clusterCfg := &config.Config{
		Hosts: []config.Host{{
			ID:   1,
			Addr: statHost,
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleBlob, Port: statServerPort},
				{ID: 2, Role: config.RoleBlob, Port: statClientPort},
			},
		}},
	}

	serverTr := transport.NewPipeTransport(statHost, statServerPort)
	t.Cleanup(func() { serverTr.Close() })
	ln, err := serverTr.Listen()
	require.NoError(t, err)

	srv, err := blob.New(blob.Config{
		NodeID:    1,
		DataDir:   t.TempDir(),
		Store:     store,
		Listeners: []transport.Listener{ln},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	clientTr := transport.NewPipeTransport(statHost, statClientPort)
	t.Cleanup(func() { clientTr.Close() })
	clientRes, err := rpc.NewResolver(clusterCfg, 2, clientTr)
	require.NoError(t, err)

	client, err := blob.NewClient(blob.ClientConfig{
		Client:          rpc.NewClient(rpc.NewConnPool(2, clientRes)),
		EnvelopeTimeout: 5 * time.Second,
		IdleTimeout:     5 * time.Second,
	})
	require.NoError(t, err)

	return client
}

// An absent shard is the one answer Stat reports as not-found, and a published
// one reports the generation it was written under.
func TestBlobStatReportsAnAbsentShardAsNotFound(t *testing.T) {
	c := startBlobNode(t)
	req := blob.StatRequest{Key: epochKey(), Index: epochTestShardIndex}

	_, err := c.Stat(context.Background(), epochServerNode, req)
	require.ErrorIs(t, err, blob.ErrNotFound)

	put(t, c, 7, []byte("published shard"))
	require.NoError(t, commit(t, c, 7))

	held, err := c.Stat(context.Background(), epochServerNode, req)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), held.Epoch)
}

// A store error must reach the caller as an error. Reported as not-found, it
// reads as a missing shard and repair rebuilds over data that may be intact.
func TestBlobStatSurfacesAStoreError(t *testing.T) {
	c := startStoreNode(t, failingLookupStore{
		slowCommitStore: newSlowCommitStore(0, 0),
		err:             errors.New("get extent: input/output error"),
	})

	_, err := c.Stat(context.Background(), 1, blob.StatRequest{Key: epochKey()})
	require.Error(t, err)
	assert.NotErrorIs(t, err, blob.ErrNotFound)
	assert.Contains(t, err.Error(), "input/output error")
}
