package blob_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/blob/hostile"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Short enough that a bound which fails to fire shows up as a hung test
// rather than a slow one.
const (
	testEnvelopeTimeout = 200 * time.Millisecond
	testIdleTimeout     = 200 * time.Millisecond
	// The margin a bounded operation is allowed over its own timeout. A
	// caller with no bound at all blows through this by orders of magnitude.
	testBoundMargin = 3 * time.Second
)

const (
	hostileHost = "hostile-test-host"
	serverPort  = 6301
	clientPort  = 6302
	serverNode  = config.NodeID(1)
)

func hostileCfg() *config.Config {
	return &config.Config{
		Hosts: []config.Host{{
			ID:   1,
			Addr: hostileHost,
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleBlob, Port: serverPort},
				{ID: 2, Role: config.RoleBlob, Port: clientPort},
			},
		}},
	}
}

// startHostile runs a blob node with the given fault over a pipe transport
// and returns a client addressed to it.
func startHostile(t *testing.T, cfg hostile.Config) (*blob.Client, *hostile.Server) {
	t.Helper()
	clusterCfg := hostileCfg()

	serverTr := transport.NewPipeTransport(hostileHost, serverPort)
	t.Cleanup(func() { serverTr.Close() })
	ln, err := serverTr.Listen()
	require.NoError(t, err)

	serverRes, err := rpc.NewResolver(clusterCfg, 1, serverTr)
	require.NoError(t, err)
	serverPool := rpc.NewConnPool(1, serverRes)

	node := hostile.New(cfg)
	srv, err := rpc.NewServer(node.Mux(), []transport.Listener{ln}, serverPool,
		rpc.WithDrainTimeout(2*time.Second))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		// Release parked handlers first, or the drain waits them out.
		node.Close()
		cancel()
		<-done
	})

	clientTr := transport.NewPipeTransport(hostileHost, clientPort)
	t.Cleanup(func() { clientTr.Close() })
	clientRes, err := rpc.NewResolver(clusterCfg, 2, clientTr)
	require.NoError(t, err)

	client, err := blob.NewClient(blob.ClientConfig{
		Client:          rpc.NewClient(rpc.NewConnPool(2, clientRes)),
		EnvelopeTimeout: testEnvelopeTimeout,
		IdleTimeout:     testIdleTimeout,
	})
	require.NoError(t, err)
	return client, node
}

func getRequest() blob.GetRequest {
	return blob.GetRequest{Key: [32]byte{1}, Index: 0, RangeStart: -1, RangeEnd: -1}
}

// withinBound runs fn and fails if it did not return promptly. This is the
// assertion that goes red against an unbounded client: it does not merely
// check the error, it checks that the caller got control back at all.
func withinBound(t *testing.T, what string, fn func() error) error {
	t.Helper()
	type result struct {
		err     error
		elapsed time.Duration
	}
	ch := make(chan result, 1)
	start := time.Now()
	go func() {
		err := fn()
		ch <- result{err: err, elapsed: time.Since(start)}
	}()

	select {
	case r := <-ch:
		assert.Lessf(t, r.elapsed, testBoundMargin,
			"%s returned in %v, which is past its bound", what, r.elapsed)
		return r.err
	case <-time.After(testBoundMargin):
		t.Fatalf("%s did not return within %v: the operation is unbounded", what, testBoundMargin)
		return nil
	}
}

func TestBlobClientBounds(t *testing.T) {
	t.Run("get against a node that never answers gives up", func(t *testing.T) {
		client, node := startHostile(t, hostile.Config{Fault: hostile.FaultStall})

		err := withinBound(t, "Get", func() error {
			_, err := client.Get(context.Background(), serverNode, getRequest())
			return err
		})

		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Positive(t, node.Calls(), "the node should have been reached")
	})

	t.Run("put against a node that never answers gives up", func(t *testing.T) {
		client, _ := startHostile(t, hostile.Config{Fault: hostile.FaultStall})

		err := withinBound(t, "Put", func() error {
			body := make([]byte, 64)
			_, err := client.Put(context.Background(), serverNode,
				blob.PutRequest{Key: [32]byte{1}, Index: 0, Size: int64(len(body))},
				newRepeatReader(body))
			return err
		})
		require.Error(t, err)
	})

	t.Run("delete against a node that never answers gives up", func(t *testing.T) {
		client, _ := startHostile(t, hostile.Config{Fault: hostile.FaultStall})

		err := withinBound(t, "Delete", func() error {
			_, err := client.Delete(context.Background(), serverNode,
				blob.DeleteRequest{Key: [32]byte{1}, Index: 0})
			return err
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	// The envelope cap has already been released by the time the body is
	// read, so only the idle guard can catch this one.
	t.Run("body that stalls mid-transfer gives up", func(t *testing.T) {
		client, _ := startHostile(t, hostile.Config{
			Fault:  hostile.FaultStallAfterEnvelope,
			Values: map[uint32][]byte{0: make([]byte, 4096)},
		})

		err := withinBound(t, "body read", func() error {
			body, err := client.Get(context.Background(), serverNode, getRequest())
			if err != nil {
				return err
			}
			defer body.Close()
			_, err = io.ReadAll(body)
			return err
		})
		require.Error(t, err)
	})

	t.Run("caller cancellation aborts a body in flight", func(t *testing.T) {
		client, _ := startHostile(t, hostile.Config{
			Fault:  hostile.FaultStallAfterEnvelope,
			Values: map[uint32][]byte{0: make([]byte, 4096)},
		})

		ctx, cancel := context.WithCancel(context.Background())
		body, err := client.Get(ctx, serverNode, getRequest())
		require.NoError(t, err)
		defer body.Close()

		time.AfterFunc(50*time.Millisecond, cancel)
		err = withinBound(t, "cancelled body read", func() error {
			_, err := io.ReadAll(body)
			return err
		})
		require.Error(t, err)
	})
}

// A short shard fed into Reed-Solomon reconstructs a plausible wrong object,
// so a peer that under-delivers must be reported, never quietly accepted.
func TestBlobClientRejectsShortBody(t *testing.T) {
	value := []byte("the quick brown fox jumps over the lazy dog")
	client, _ := startHostile(t, hostile.Config{
		Fault:  hostile.FaultTruncate,
		Values: map[uint32][]byte{0: value},
	})

	body, err := client.Get(context.Background(), serverNode, getRequest())
	require.NoError(t, err)
	defer body.Close()

	got, err := io.ReadAll(body)
	require.Error(t, err, "a truncated body must not read back as a complete value")
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.Less(t, len(got), len(value))
}

// An envelope with no terminator must not be buffered without limit.
func TestBlobClientCapsEnvelope(t *testing.T) {
	client, _ := startHostile(t, hostile.Config{Fault: hostile.FaultEnvelopeGarbage})

	err := withinBound(t, "Get with unterminated envelope", func() error {
		_, err := client.Get(context.Background(), serverNode, getRequest())
		return err
	})
	require.Error(t, err)
}

// The guards must not cost correctness on the path that works.
func TestBlobClientHonestRoundTrip(t *testing.T) {
	value := []byte("shard bytes that must survive the guards intact")
	client, _ := startHostile(t, hostile.Config{
		Fault:  hostile.FaultNone,
		Values: map[uint32][]byte{0: value},
	})

	body, err := client.Get(context.Background(), serverNode, getRequest())
	require.NoError(t, err)
	defer body.Close()

	got, err := io.ReadAll(body)
	require.NoError(t, err)
	assert.Equal(t, value, got)
}

// A transfer that keeps making progress must never be cut off, however long
// it runs in total. This is what separates an idle bound from a total one.
func TestBlobClientAllowsSlowButProgressingBody(t *testing.T) {
	value := make([]byte, 200)
	for i := range value {
		value[i] = byte(i)
	}
	client, _ := startHostile(t, hostile.Config{
		Fault:        hostile.FaultSlowDrip,
		Values:       map[uint32][]byte{0: value},
		DripInterval: 5 * time.Millisecond,
	})

	body, err := client.Get(context.Background(), serverNode, getRequest())
	require.NoError(t, err)
	defer body.Close()

	// One second in total, far past the 200ms idle bound, but never idle.
	got, err := io.ReadAll(body)
	require.NoError(t, err, "a progressing transfer must not trip the idle guard")
	assert.Equal(t, value, got)
}

// repeatReader yields the same bytes forever, so a put can push more than the
// peer's flow-control window without allocating it all.
type repeatReader struct {
	buf []byte
	pos int
}

func newRepeatReader(buf []byte) *repeatReader { return &repeatReader{buf: buf} }

func (r *repeatReader) Read(p []byte) (int, error) {
	n := copy(p, r.buf[r.pos%len(r.buf):])
	r.pos += n
	return n, nil
}

var _ io.Reader = (*repeatReader)(nil)
