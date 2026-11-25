package blob_test

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	compatHost       = "compat-test-host"
	compatServerPort = 6481
	compatClientPort = 6482
)

// startPriorNode runs a node that answers only the opcodes a node from before
// held writes knows, and answers a put the way that node did: it ignores Hold,
// reads the body and replies once. It reports each put's body byte count.
func startPriorNode(t *testing.T, envelopeTimeout time.Duration) (*blob.Client, <-chan int64) {
	t.Helper()

	clusterCfg := &config.Config{
		Hosts: []config.Host{{
			ID:   1,
			Addr: compatHost,
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleBlob, Port: compatServerPort},
				{ID: 2, Role: config.RoleBlob, Port: compatClientPort},
			},
		}},
	}

	bodies := make(chan int64, 4)
	mux := rpc.NewMux()
	rpc.RegisterHandler(mux, blob.OpPut, func(_ context.Context, h blob.Request, stream transport.Stream) error {
		n, err := io.Copy(io.Discard, io.LimitReader(stream, h.Size))
		bodies <- n
		if err != nil {
			return err
		}
		return json.NewEncoder(stream).Encode(&blob.Response{Size: h.Size, Epoch: h.Epoch})
	})

	serverTr := transport.NewPipeTransport(compatHost, compatServerPort)
	t.Cleanup(func() { serverTr.Close() })
	ln, err := serverTr.Listen()
	require.NoError(t, err)
	srv, err := rpc.NewServer(mux, []transport.Listener{ln}, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	clientTr := transport.NewPipeTransport(compatHost, compatClientPort)
	t.Cleanup(func() { clientTr.Close() })
	clientRes, err := rpc.NewResolver(clusterCfg, 2, clientTr)
	require.NoError(t, err)
	client, err := blob.NewClient(blob.ClientConfig{
		Client:          rpc.NewClient(rpc.NewConnPool(2, clientRes)),
		EnvelopeTimeout: envelopeTimeout,
		IdleTimeout:     5 * time.Second,
	})
	require.NoError(t, err)

	return client, bodies
}

// A node from before held writes never answers Registered, so the client
// gives up on the hold and the node receives no body byte.
func TestHeldPutToAPriorNodeSendsNoBody(t *testing.T) {
	c, bodies := startPriorNode(t, 200*time.Millisecond)

	_, err := c.OpenHeld(context.Background(), 1, heldReq(1, []byte("never sent")))
	require.ErrorIs(t, err, blob.ErrHoldUnsupported)

	select {
	case n := <-bodies:
		assert.Zero(t, n, "a held put that was not registered must send no body")
	case <-time.After(5 * time.Second):
		t.Fatal("the prior node's put never ended")
	}
}

// A node from before DeleteEpoch has no handler for it and closes the stream
// unanswered, which the client reports as an error rather than a delete.
func TestDeleteEpochToAPriorNodeFails(t *testing.T) {
	c, _ := startPriorNode(t, time.Second)

	resp, err := c.DeleteEpoch(context.Background(), 1, shardAt(1))
	require.Error(t, err, "a node without the opcode must not report a delete")
	assert.Nil(t, resp)
}
