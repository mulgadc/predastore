package blob_test

import (
	"bytes"
	"context"
	"io"
	"sync"
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

// slowCommitStore is a healthy store on a drive whose completion interrupts
// are late. Every write lands; only the fsync in Close is delayed, which is
// what "I/O tag N QID nn timeout, completion polled" describes.
type slowCommitStore struct {
	commitDelay time.Duration
	// readDelay stalls the server before it drains the body, which is what a
	// mid-transfer device stall looks like from the other end of the stream.
	readDelay time.Duration

	mu        sync.Mutex
	prepared  map[uint32]preparedValue
	published map[uint32]preparedValue
}

type preparedValue struct {
	data  []byte
	epoch uint64
}

func newSlowCommitStore(commitDelay, readDelay time.Duration) *slowCommitStore {
	return &slowCommitStore{
		commitDelay: commitDelay,
		readDelay:   readDelay,
		prepared:    map[uint32]preparedValue{},
		published:   map[uint32]preparedValue{},
	}
}

func (s *slowCommitStore) Append(_ [32]byte, index uint32, _ int64, epoch uint64) (engine.Writer, error) {
	return &slowCommitWriter{store: s, index: index, epoch: epoch}, nil
}

func (s *slowCommitStore) Lookup(_ [32]byte, index uint32) (engine.Reader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.published[index]
	if !ok {
		return nil, engine.ErrKeyNotFound
	}

	return &sectionReader{Reader: bytes.NewReader(v.data), size: int64(len(v.data)), epoch: v.epoch}, nil
}

// Commit publishes a prepared value. It is idempotent against the epoch so a
// retry of one already applied succeeds rather than reporting a lost write,
// and publishing is forward-only: losing to a higher epoch reports
// published=false rather than an error, which is last-write-wins.
func (s *slowCommitStore) Commit(_ [32]byte, index uint32, epoch uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.published[index]; ok {
		if v.epoch == epoch {
			return true, nil
		}
		if v.epoch > epoch {
			return false, nil
		}
	}
	v, ok := s.prepared[index]
	if !ok || v.epoch != epoch {
		return false, engine.ErrNotPrepared
	}
	s.published[index] = v
	delete(s.prepared, index)

	return true, nil
}

// LookupAt serves a named generation, which the read path uses to reach a
// shard the placement record still points at.
func (s *slowCommitStore) LookupAt(_ [32]byte, index uint32, epoch uint64) (engine.Reader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, set := range []map[uint32]preparedValue{s.published, s.prepared} {
		if v, ok := set[index]; ok && v.epoch == epoch {
			return &sectionReader{Reader: bytes.NewReader(v.data), size: int64(len(v.data)), epoch: v.epoch}, nil
		}
	}

	return nil, engine.ErrKeyNotFound
}

func (s *slowCommitStore) Abort(_ [32]byte, index uint32, epoch uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.prepared[index]; ok && v.epoch == epoch {
		delete(s.prepared, index)
	}

	return nil
}

func (s *slowCommitStore) ReleaseGeneration(_ [32]byte, _ uint32, _ uint64) error { return nil }
func (s *slowCommitStore) Delete(_ [32]byte, _ uint32) (bool, error)              { return false, nil }
func (s *slowCommitStore) NearFull() bool                                         { return false }
func (s *slowCommitStore) Close() error                                           { return nil }

// Prepared is what reached the platter, whatever the caller was told.
func (s *slowCommitStore) Prepared(index uint32) (preparedValue, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.prepared[index]

	return v, ok
}

type sectionReader struct {
	*bytes.Reader

	size  int64
	epoch uint64
}

func (r *sectionReader) Size() int64   { return r.size }
func (r *sectionReader) Epoch() uint64 { return r.epoch }
func (r *sectionReader) Close() error  { return nil }

type slowCommitWriter struct {
	store *slowCommitStore
	index uint32
	epoch uint64
	buf   bytes.Buffer
}

func (w *slowCommitWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *slowCommitWriter) ReadFrom(r io.Reader) (int64, error) {
	time.Sleep(w.store.readDelay)

	return w.buf.ReadFrom(r)
}

// Close is the prepare: two fsyncs in the real engine, and the only thing the
// stalling drive delays. It still succeeds.
func (w *slowCommitWriter) Close() error {
	time.Sleep(w.store.commitDelay)
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	w.store.prepared[w.index] = preparedValue{
		data:  append([]byte(nil), w.buf.Bytes()...),
		epoch: w.epoch,
	}

	return nil
}

const (
	slowServerPort = 6311
	slowClientPort = 6312
	slowEpoch      = uint64(0x1234)
)

// startSlowCommitNode runs a real blob.Server over a store whose commit is
// slow, and returns a client pointed at it plus the store to inspect.
// envelopeTimeout is deliberately separate from commitTimeout in every case
// here: setting it short is what proves which bound the put is actually under.
func startSlowCommitNode(
	t *testing.T, commitDelay, readDelay, envelopeTimeout, commitTimeout, idleTimeout time.Duration,
) (*blob.Client, *slowCommitStore) {
	t.Helper()

	clusterCfg := &config.Config{
		Hosts: []config.Host{{
			ID:   1,
			Addr: hostileHost,
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleBlob, Port: slowServerPort},
				{ID: 2, Role: config.RoleBlob, Port: slowClientPort},
			},
		}},
	}

	serverTr := transport.NewPipeTransport(hostileHost, slowServerPort)
	t.Cleanup(func() { serverTr.Close() })
	ln, err := serverTr.Listen()
	require.NoError(t, err)

	store := newSlowCommitStore(commitDelay, readDelay)
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

	clientTr := transport.NewPipeTransport(hostileHost, slowClientPort)
	t.Cleanup(func() { clientTr.Close() })
	clientRes, err := rpc.NewResolver(clusterCfg, 2, clientTr)
	require.NoError(t, err)

	client, err := blob.NewClient(blob.ClientConfig{
		Client:          rpc.NewClient(rpc.NewConnPool(2, clientRes)),
		EnvelopeTimeout: envelopeTimeout,
		CommitTimeout:   commitTimeout,
		IdleTimeout:     idleTimeout,
	})
	require.NoError(t, err)

	return client, store
}

// A blob node on a drive that stalls its commit used to fail every put,
// because the response envelope was capped at EnvelopeTimeout and handlePut
// does not send it until the shard is fsynced. The cap was on the disk.
// CommitTimeout is the bound that belongs there, and it is set above the
// kernel's 30s io_timeout for that reason.
func TestPutWaitsOutASlowCommit(t *testing.T) {
	const (
		// Shorter than the commit it has to survive: if the put is bounded by
		// this, as it was, the write fails on a drive that is merely slow.
		envelopeTimeout = 300 * time.Millisecond
		commitTimeout   = 5 * time.Second
		idleTimeout     = 30 * time.Second
		commitDelay     = 750 * time.Millisecond
	)

	client, store := startSlowCommitNode(t, commitDelay, 0, envelopeTimeout, commitTimeout, idleTimeout)
	payload := bytes.Repeat([]byte("x"), 4096)

	start := time.Now()
	resp, err := client.Put(context.Background(), serverNode,
		blob.PutRequest{Key: [32]byte{1}, Index: 0, Size: int64(len(payload)), Epoch: slowEpoch},
		bytes.NewReader(payload))
	elapsed := time.Since(start)

	require.NoError(t, err, "a commit slower than a round trip is not a failed write")
	assert.Equal(t, int64(len(payload)), resp.Size)
	assert.GreaterOrEqual(t, elapsed, commitDelay,
		"the put should have waited for the commit rather than racing it")

	// What the caller was told matches what is on the platter.
	prepared, ok := store.Prepared(0)
	require.True(t, ok, "the shard should be prepared")
	assert.Equal(t, payload, prepared.data)
	assert.Equal(t, slowEpoch, prepared.epoch)
}

// TestPutReportsAnAbandonedCommitAsUnknown covers the residual case: a commit
// slower than even the commit bound. The shard may still land, so the put says
// the outcome is unknown rather than reporting a refusal it never received.
// The gate settles it by committing, which is what stops a slow drive being
// scored as lost redundancy.
func TestPutReportsAnAbandonedCommitAsUnknown(t *testing.T) {
	const (
		envelopeTimeout = 10 * time.Second
		commitTimeout   = 300 * time.Millisecond
		idleTimeout     = 30 * time.Second
		commitDelay     = 900 * time.Millisecond
	)

	client, store := startSlowCommitNode(t, commitDelay, 0, envelopeTimeout, commitTimeout, idleTimeout)
	payload := bytes.Repeat([]byte("x"), 4096)

	_, err := client.Put(context.Background(), serverNode,
		blob.PutRequest{Key: [32]byte{2}, Index: 1, Size: int64(len(payload)), Epoch: slowEpoch},
		bytes.NewReader(payload))

	require.Error(t, err)
	assert.ErrorIs(t, err, blob.ErrCommitUnknown,
		"a node that took the body and went quiet has not refused the write")
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// The write the caller was told nothing about lands anyway, and the commit
	// the gate issues after publishing the record finds it there.
	assert.Eventually(t, func() bool {
		_, ok := store.Prepared(1)

		return ok
	}, 5*time.Second, 20*time.Millisecond, "the abandoned put should still have landed")

	overtaken, err := client.Commit(context.Background(), serverNode,
		blob.CommitRequest{Key: [32]byte{2}, Index: 1, Epoch: slowEpoch})
	require.NoError(t, err,
		"committing an abandoned put is how the gate learns it was only slow")
	assert.False(t, overtaken, "nothing newer raced it, so this epoch is the live one")
}

// TestPutBodyStallTripsTheIdleGuard is the other half of the same fault: when
// the drive stalls while the body is still arriving, the server stops reading
// the stream and the write guard aborts it. That is a transfer making no
// progress rather than a slow commit, so it must stay bounded by the idle
// timeout and must not be waited out.
func TestPutBodyStallTripsTheIdleGuard(t *testing.T) {
	const (
		envelopeTimeout = 10 * time.Second
		commitTimeout   = 45 * time.Second
		idleTimeout     = 300 * time.Millisecond
		readDelay       = 1 * time.Second
	)

	client, _ := startSlowCommitNode(t, 0, readDelay, envelopeTimeout, commitTimeout, idleTimeout)

	// Larger than the stream's flow-control window, so a server that is not
	// draining blocks the client's writes rather than absorbing them.
	payload := bytes.Repeat([]byte("x"), 8<<20)

	start := time.Now()
	_, err := client.Put(context.Background(), serverNode,
		blob.PutRequest{Key: [32]byte{3}, Index: 2, Size: int64(len(payload)), Epoch: slowEpoch},
		bytes.NewReader(payload))
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.ErrorIs(t, err, transport.ErrIdleTimeout)
	assert.NotErrorIs(t, err, blob.ErrCommitUnknown,
		"a body that never arrived cannot have prepared a shard")
	assert.Less(t, elapsed, readDelay, "the guard should fire at the idle timeout")
}
