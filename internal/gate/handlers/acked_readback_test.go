package handlers

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/storetest"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errInjected stands in for the transport failures a stalled or restarting
// peer produces: the connection closes and the caller learns nothing more.
var errInjected = errors.New("injected: connection closed")

// shardFaults names, by shard index, what goes wrong for the next write.
type shardFaults struct {
	// failPut drains the body then fails, so the write path sees a shard that
	// failed after it was sent, which hinted handoff does not cover.
	failPut []uint32
	// dropCommit fails the commit without it reaching the node.
	dropCommit []uint32
	// phantom never prepares the shard: a concurrent writer publishes a newer
	// generation on that node instead, and the put's outcome reads as unknown.
	phantom []uint32
}

// faultyBlob forwards to real blob nodes, except where the armed faults say.
type faultyBlob struct {
	BlobClient

	epochs *EpochMinter
	mu     sync.Mutex
	armed  shardFaults

	// released and committed record what the gate asked of the nodes, by
	// epoch, so a test can say which generation a write released.
	released  []blob.ReleaseRequest
	committed map[uint64]int
}

func (f *faultyBlob) Release(ctx context.Context, node config.NodeID, req blob.ReleaseRequest) error {
	f.mu.Lock()
	f.released = append(f.released, req)
	f.mu.Unlock()
	return f.BlobClient.Release(ctx, node, req)
}

// takeReleases returns and clears the releases recorded so far.
func (f *faultyBlob) takeReleases() []blob.ReleaseRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.released
	f.released = nil
	return out
}

// lastCommitEpoch is the epoch of the most recent commit the gate sent.
func (f *faultyBlob) lastCommitEpoch() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var last uint64
	for e := range f.committed {
		last = max(last, e)
	}
	return last
}

func (f *faultyBlob) arm(s shardFaults) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armed = s
}

func (f *faultyBlob) faults() shardFaults {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.armed
}

func (f *faultyBlob) Put(
	ctx context.Context, node config.NodeID, req blob.PutRequest, body io.Reader,
) (*blob.PutResponse, error) {
	armed := f.faults()
	switch {
	case slices.Contains(armed.failPut, req.Index):
		_, _ = io.Copy(io.Discard, body)
		return nil, errInjected
	case slices.Contains(armed.phantom, req.Index):
		_, _ = io.Copy(io.Discard, body)
		newer, err := f.epochs.Next()
		if err != nil {
			return nil, err
		}
		other := bytes.Repeat([]byte{0xEE}, int(req.Size))
		rival := req
		rival.Epoch = newer
		if _, err := f.BlobClient.Put(ctx, node, rival, bytes.NewReader(other)); err != nil {
			return nil, err
		}
		if _, err := f.BlobClient.Commit(ctx, node, blob.CommitRequest{Key: req.Key, Index: req.Index, Epoch: newer}); err != nil {
			return nil, err
		}
		return nil, blob.ErrCommitUnknown
	}
	return f.BlobClient.Put(ctx, node, req, body)
}

func (f *faultyBlob) Commit(ctx context.Context, node config.NodeID, req blob.CommitRequest) (bool, error) {
	f.mu.Lock()
	if f.committed == nil {
		f.committed = make(map[uint64]int)
	}
	f.committed[req.Epoch]++
	f.mu.Unlock()
	if slices.Contains(f.faults().dropCommit, req.Index) {
		return false, errInjected
	}
	return f.BlobClient.Commit(ctx, node, req)
}

// realStoreGate is the gate's object handlers over real blob nodes, each on its
// own engine store, reached through the real blob client over the pipe
// transport. Only metadata is faked: meta quorum is not what these tests ask.
type realStoreGate struct {
	mc    *fakeMeta
	bc    *faultyBlob
	ring  *placement.Ring
	cfg   Config
	cache *BucketCache
}

// rsProfile is an erasure code and the cluster it runs on. Each has one node
// more than its stripe, as production does, so hinted handoff has a target.
type rsProfile struct{ data, parity, nodes int }

var rsProfiles = []rsProfile{{2, 1, 4}, {3, 2, 6}}

func (p rsProfile) String() string { return fmt.Sprintf("rs-%d-%d", p.data, p.parity) }

// pipeHostSeq keeps every gate on a pipe host of its own. A benchmark runs
// its setup once per calibration round, so a name alone would repeat.
var pipeHostSeq atomic.Uint64

func newRealStoreGate(t testing.TB, p rsProfile) *realStoreGate {
	t.Helper()

	host := fmt.Sprintf("acked-%d-%s", pipeHostSeq.Add(1),
		strings.NewReplacer("/", "-", " ", "-").Replace(t.Name()))

	const clientNode, basePort = config.NodeID(99), 7400
	ids := make([]config.NodeID, p.nodes)
	nodes := make([]config.Node, 0, p.nodes+1)
	for i := range ids {
		ids[i] = config.NodeID(i + 1)
		nodes = append(nodes, config.Node{ID: ids[i], Role: config.RoleBlob, Port: basePort + i + 1})
	}
	nodes = append(nodes, config.Node{ID: clientNode, Role: config.RoleBlob, Port: basePort + int(clientNode)})
	clusterCfg := &config.Config{Hosts: []config.Host{{ID: 1, Addr: host, Nodes: nodes}}}

	for i, id := range ids {
		tr := transport.NewPipeTransport(host, basePort+i+1)
		t.Cleanup(func() { tr.Close() })
		ln, err := tr.Listen()
		require.NoError(t, err)

		srv, err := blob.New(blob.Config{
			NodeID:    id,
			DataDir:   t.TempDir(),
			AEAD:      storetest.TestAEAD(),
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
	}

	clientTr := transport.NewPipeTransport(host, basePort+int(clientNode))
	t.Cleanup(func() { clientTr.Close() })
	res, err := rpc.NewResolver(clusterCfg, clientNode, clientTr)
	require.NoError(t, err)
	pool := rpc.NewConnPool(clientNode, res)
	t.Cleanup(func() { _ = pool.Close() })
	client, err := blob.NewClient(blob.ClientConfig{
		Client:          rpc.NewClient(pool),
		EnvelopeTimeout: 5 * time.Second,
		IdleTimeout:     5 * time.Second,
	})
	require.NoError(t, err)

	epochs := mustEpochs(clientNode)
	// The production profile: degraded writes and hinted handoff on.
	return &realStoreGate{
		mc:   newFakeMeta(),
		bc:   &faultyBlob{BlobClient: client, epochs: epochs},
		ring: placement.NewRing(ids),
		cfg: Config{
			Region: "ap-southeast-2", DataShards: p.data, ParityShards: p.parity,
			DegradedWrites: true, HintedHandoff: true,
			Epochs: epochs,
		},
		cache: NewBucketCache([]BucketConfig{{Name: "bkt"}}),
	}
}

func (g *realStoreGate) serve(h http.Handler, method, target, key string, body []byte, header http.Header) *httptest.ResponseRecorder {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rd).WithContext(objectCtx("bkt", key))
	req.ContentLength = int64(len(body))
	maps.Copy(req.Header, header)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func (g *realStoreGate) put(key string, body []byte) *httptest.ResponseRecorder {
	return g.serve(PutObject(g.mc, g.bc, g.ring, g.cache, g.cfg), http.MethodPut, "/bkt/"+key, key, body, nil)
}

func (g *realStoreGate) get(key string) *httptest.ResponseRecorder {
	return g.serve(GetObject(g.mc, g.bc, g.ring, g.cache, g.cfg), http.MethodGet, "/bkt/"+key, key, nil, nil)
}

func (g *realStoreGate) copy(src, dst string) *httptest.ResponseRecorder {
	return g.serve(CopyObject(g.mc, g.bc, g.ring, g.cache, g.cfg), http.MethodPut, "/bkt/"+dst, dst, nil,
		http.Header{"X-Amz-Copy-Source": {"/bkt/" + src}})
}

func (g *realStoreGate) createUpload(t testing.TB, key string) string {
	t.Helper()
	rr := g.serve(CreateMultipartUpload(g.mc, g.cache), http.MethodPost, "/bkt/"+key+"?uploads", key, nil, nil)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var created InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &created))
	return created.UploadId
}

func (g *realStoreGate) uploadPart(key, upload string, n int, body []byte) *httptest.ResponseRecorder {
	return g.serve(UploadPart(g.mc, g.bc, g.ring, g.cache, g.cfg), http.MethodPut,
		fmt.Sprintf("/bkt/%s?partNumber=%d&uploadId=%s", key, n, url.QueryEscape(upload)), key, body, nil)
}

func (g *realStoreGate) uploadPartCopy(key, upload string, n int, src string) *httptest.ResponseRecorder {
	return g.serve(UploadPartCopy(g.mc, g.bc, g.ring, g.cache, g.cfg), http.MethodPut,
		fmt.Sprintf("/bkt/%s?partNumber=%d&uploadId=%s", key, n, url.QueryEscape(upload)), key, nil,
		http.Header{"X-Amz-Copy-Source": {"/bkt/" + src}})
}

func (g *realStoreGate) listParts(t testing.TB, key, upload string) ListPartsResult {
	t.Helper()
	rr := g.serve(ListParts(g.mc, g.cache), http.MethodGet,
		fmt.Sprintf("/bkt/%s?uploadId=%s", key, url.QueryEscape(upload)), key, nil, nil)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var out ListPartsResult
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &out))
	return out
}

func (g *realStoreGate) complete(t testing.TB, key, upload string, parts ...MultipartUploadPart) *httptest.ResponseRecorder {
	t.Helper()
	body, err := xml.Marshal(CompleteMultipartUploadRequest{Parts: parts})
	require.NoError(t, err)
	return g.serve(CompleteMultipartUpload(g.mc, g.bc, g.ring, g.cache, g.cfg), http.MethodPost,
		fmt.Sprintf("/bkt/%s?uploadId=%s", key, url.QueryEscape(upload)), key, body, nil)
}

// succeeded reports whether a write answered as a success, which for copy and
// completion means the body too: AWS can answer 200 with an error document.
func succeeded(rr *httptest.ResponseRecorder) bool {
	return rr.Code == http.StatusOK && !strings.Contains(rr.Body.String(), "<Error>")
}

// requireRefused holds a write below its floor to the promised failure: HTTP
// 500 carrying an S3 InternalError, sent before any success header.
func requireRefused(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, http.StatusInternalServerError, rr.Code, "a write below its floor must fail with 500: %s", rr.Body.String())
	assert.Equal(t, "InternalError", s3ErrorCode(t, rr))
	assert.Empty(t, rr.Header().Get("ETag"), "a refused write must not carry a success header")
}

// successBody checks that a success decodes as the operation's own result.
type successBody func(t *testing.T, rr *httptest.ResponseRecorder)

func copyResult(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	var out CopyObjectResult
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &out), rr.Body.String())
	assert.NotEmpty(t, out.ETag)
}

func completeResult(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	var out CompleteMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &out), rr.Body.String())
	assert.NotEmpty(t, out.ETag)
}

// faultCase is a fault and whether it leaves the write at or above its floor.
type faultCase struct {
	name     string
	faults   shardFaults
	atFloor  bool
	profiles []rsProfile
}

// The cases cover each side of the floor at both codes. RS(2,1) needs two
// committed shards and RS(3,2) needs three, so the same fault shape lands on
// different sides of the floor for each.
var faultCases = []faultCase{
	{
		name:     "one shard lost, the rest commit",
		faults:   shardFaults{failPut: []uint32{1}},
		atFloor:  true,
		profiles: rsProfiles,
	},
	{
		name:     "two shards lost, the rest commit",
		faults:   shardFaults{failPut: []uint32{1, 3}},
		atFloor:  true,
		profiles: []rsProfile{{3, 2, 6}},
	},
	{
		name:     "one shard lost and one commit dropped",
		faults:   shardFaults{failPut: []uint32{1}, dropCommit: []uint32{0}},
		atFloor:  false,
		profiles: []rsProfile{{2, 1, 4}},
	},
	{
		name:     "one shard lost and one commit dropped, above a floor of three",
		faults:   shardFaults{failPut: []uint32{1}, dropCommit: []uint32{0}},
		atFloor:  true,
		profiles: []rsProfile{{3, 2, 6}},
	},
	{
		name:     "one shard lost and two commits dropped",
		faults:   shardFaults{failPut: []uint32{1}, dropCommit: []uint32{0, 2}},
		atFloor:  false,
		profiles: []rsProfile{{3, 2, 6}},
	},
	{
		name:     "a position that never prepared, behind a newer generation",
		faults:   shardFaults{phantom: []uint32{1}, dropCommit: []uint32{0}},
		atFloor:  false,
		profiles: []rsProfile{{2, 1, 4}},
	},
	{
		name:     "a position that never prepared, behind a newer generation, at three",
		faults:   shardFaults{phantom: []uint32{1}, dropCommit: []uint32{0, 2}},
		atFloor:  false,
		profiles: []rsProfile{{3, 2, 6}},
	},
}

func eachFault(t *testing.T, fn func(t *testing.T, p rsProfile, c faultCase)) {
	for _, c := range faultCases {
		for _, p := range c.profiles {
			t.Run(p.String()+"/"+c.name, func(t *testing.T) { fn(t, p, c) })
		}
	}
}

// requireOutcome is the contract every write here is held to: a success reads
// back as the new bytes, and a failure leaves the key as it was, or absent.
// Below the floor the write must fail; at or above it, it must succeed.
func requireOutcome(
	t *testing.T, g *realStoreGate, c faultCase, write *httptest.ResponseRecorder, key string, before, after []byte,
	body successBody,
) {
	t.Helper()
	got := g.get(key)
	if c.atFloor {
		require.True(t, succeeded(write), "a write at its floor must succeed: %d %s", write.Code, write.Body.String())
		if body != nil {
			body(t, write)
		}
	} else {
		requireRefused(t, write)
	}
	switch {
	case succeeded(write):
		require.Equal(t, http.StatusOK, got.Code, "an acknowledged write did not read back: %s", got.Body.String())
		assert.True(t, bytes.Equal(after, got.Body.Bytes()), "an acknowledged write read back other bytes")
	case before == nil:
		assert.Equal(t, http.StatusNotFound, got.Code, "a failed first write must leave the key absent: %s", got.Body.String())
	default:
		require.Equal(t, http.StatusOK, got.Code, "a failed write left the key unreadable: %s", got.Body.String())
		assert.True(t, bytes.Equal(before, got.Body.Bytes()), "a failed write must leave the previous version")
	}
}

// The prod failure of 2026-11-23 was a new key: a viperblock chunk.
func TestAnAcknowledgedNewKeyReadsBack(t *testing.T) {
	eachFault(t, func(t *testing.T, p rsProfile, c faultCase) {
		g := newRealStoreGate(t, p)
		body := bytes.Repeat([]byte("c"), 1<<20)

		g.bc.arm(c.faults)
		put := g.put("fresh", body)
		g.bc.arm(shardFaults{})

		requireOutcome(t, g, c, put, "fresh", nil, body, nil)
	})
}

func TestAnAcknowledgedOverwriteReadsBack(t *testing.T) {
	eachFault(t, func(t *testing.T, p rsProfile, c faultCase) {
		g := newRealStoreGate(t, p)
		before := bytes.Repeat([]byte("v1"), 1<<19)
		after := bytes.Repeat([]byte("v2"), 1<<19)
		require.Equal(t, http.StatusOK, g.put("chunk", before).Code)

		g.bc.arm(c.faults)
		put := g.put("chunk", after)
		g.bc.arm(shardFaults{})

		requireOutcome(t, g, c, put, "chunk", before, after, nil)
	})
}

// A failed write may leave a committed shard under an epoch no record names.
// It must not disturb the writes that follow.
func TestAWriteAfterAFailedOneReadsBack(t *testing.T) {
	eachFault(t, func(t *testing.T, p rsProfile, c faultCase) {
		g := newRealStoreGate(t, p)
		g.bc.arm(c.faults)
		_ = g.put("k", bytes.Repeat([]byte("a"), 1<<18))
		g.bc.arm(shardFaults{})

		final := bytes.Repeat([]byte("z"), 1<<18)
		require.Equal(t, http.StatusOK, g.put("k", final).Code)
		got := g.get("k")
		require.Equal(t, http.StatusOK, got.Code, got.Body.String())
		assert.True(t, bytes.Equal(final, got.Body.Bytes()))
	})
}

func TestAnAcknowledgedCopyReadsBack(t *testing.T) {
	eachFault(t, func(t *testing.T, p rsProfile, c faultCase) {
		g := newRealStoreGate(t, p)
		before := bytes.Repeat([]byte("d1"), 1<<18)
		src := bytes.Repeat([]byte("s"), 1<<19)
		require.Equal(t, http.StatusOK, g.put("src", src).Code)
		require.Equal(t, http.StatusOK, g.put("dst", before).Code)

		g.bc.arm(c.faults)
		cp := g.copy("src", "dst")
		g.bc.arm(shardFaults{})

		requireOutcome(t, g, c, cp, "dst", before, src, copyResult)
	})
}

func TestAnAcknowledgedCompletionReadsBack(t *testing.T) {
	eachFault(t, func(t *testing.T, p rsProfile, c faultCase) {
		g := newRealStoreGate(t, p)
		part := bytes.Repeat([]byte("p"), 1<<19)
		upload := g.createUpload(t, "mp")
		up := g.uploadPart("mp", upload, 1, part)
		require.True(t, succeeded(up), up.Body.String())

		g.bc.arm(c.faults)
		done := g.complete(t, "mp", upload, MultipartUploadPart{PartNumber: 1, ETag: up.Header().Get("ETag")})
		g.bc.arm(shardFaults{})

		requireOutcome(t, g, c, done, "mp", nil, part, completeResult)
	})
}

// A part replaced under a fault below the floor must leave the upload on the
// part it had: ListParts names the old ETag, and completing with it serves the
// old bytes.
func TestAFailedPartReplacementKeepsTheOldPart(t *testing.T) {
	for _, via := range []string{"UploadPart", "UploadPartCopy"} {
		eachFault(t, func(t *testing.T, p rsProfile, c faultCase) {
			if c.atFloor {
				t.Skip("a write at its floor replaces the part, which the success cases cover")
			}
			t.Run(via, func(t *testing.T) {
				g := newRealStoreGate(t, p)
				old := bytes.Repeat([]byte("o"), 1<<19)
				replacement := bytes.Repeat([]byte("r"), 1<<19)
				require.Equal(t, http.StatusOK, g.put("src", replacement).Code)

				upload := g.createUpload(t, "mp")
				up := g.uploadPart("mp", upload, 1, old)
				require.True(t, succeeded(up), up.Body.String())
				oldETag := up.Header().Get("ETag")

				g.bc.arm(c.faults)
				var rr *httptest.ResponseRecorder
				if via == "UploadPart" {
					rr = g.uploadPart("mp", upload, 1, replacement)
				} else {
					rr = g.uploadPartCopy("mp", upload, 1, "src")
				}
				g.bc.arm(shardFaults{})
				requireRefused(t, rr)

				listed := g.listParts(t, "mp", upload)
				require.Len(t, listed.Parts, 1)
				assert.Equal(t, oldETag, listed.Parts[0].ETag, "ListParts must still name the old part")

				done := g.complete(t, "mp", upload, MultipartUploadPart{PartNumber: 1, ETag: oldETag})
				require.True(t, succeeded(done), done.Body.String())
				got := g.get("mp")
				require.Equal(t, http.StatusOK, got.Code, got.Body.String())
				assert.True(t, bytes.Equal(old, got.Body.Bytes()), "completing with the old ETag must serve the old part")
			})
		})
	}
}

// An empty object stores no shards, so the floor does not apply to it.
func TestEmptyWritesNeedNoShardFloor(t *testing.T) {
	for _, p := range rsProfiles {
		t.Run(p.String(), func(t *testing.T) {
			g := newRealStoreGate(t, p)
			require.True(t, succeeded(g.put("empty", []byte{})))
			require.True(t, succeeded(g.copy("empty", "empty-copy")))
			got := g.get("empty-copy")
			require.Equal(t, http.StatusOK, got.Code, got.Body.String())
			assert.Empty(t, got.Body.Bytes())
		})
	}
}

// With degraded writes off the floor is the whole stripe, so one commit lost
// fails the write even though every shard landed.
func TestWithoutDegradedWritesEveryShardMustCommit(t *testing.T) {
	for _, p := range rsProfiles {
		t.Run(p.String(), func(t *testing.T) {
			g := newRealStoreGate(t, p)
			g.cfg.DegradedWrites = false
			before := bytes.Repeat([]byte("v1"), 1<<18)
			require.Equal(t, http.StatusOK, g.put("k", before).Code)

			g.bc.arm(shardFaults{dropCommit: []uint32{0}})
			put := g.put("k", bytes.Repeat([]byte("v2"), 1<<18))
			g.bc.arm(shardFaults{})

			requireRefused(t, put)
			got := g.get("k")
			require.Equal(t, http.StatusOK, got.Code, got.Body.String())
			assert.True(t, bytes.Equal(before, got.Body.Bytes()), "a failed write must leave the previous version")
		})
	}
}

// A failed overwrite must leave the generation it would have replaced
// unreleased, which is what keeps it from the sweep (engine:
// TestTheSweepKeepsAGenerationNobodyReleased), and the next successful write
// must release exactly that generation on every holder, which is what lets
// the sweep reclaim it (engine: TestAReleasedGenerationSurvivesTheReaderGraceWindow).
func TestAFailedOverwriteReleasesNothingAndTheNextWriteReleasesTheSeed(t *testing.T) {
	eachFault(t, func(t *testing.T, p rsProfile, c faultCase) {
		if c.atFloor {
			t.Skip("a write at its floor succeeds and releases the seed itself")
		}
		g := newRealStoreGate(t, p)
		seed := bytes.Repeat([]byte("s"), 1<<18)
		require.Equal(t, http.StatusOK, g.put("k", seed).Code)
		seedEpoch := g.bc.lastCommitEpoch()
		_ = g.bc.takeReleases()

		g.bc.arm(c.faults)
		failed := g.put("k", bytes.Repeat([]byte("f"), 1<<18))
		g.bc.arm(shardFaults{})
		requireRefused(t, failed)
		assert.Empty(t, g.bc.takeReleases(), "a failed overwrite released a generation")

		got := g.get("k")
		require.Equal(t, http.StatusOK, got.Code, got.Body.String())
		assert.True(t, bytes.Equal(seed, got.Body.Bytes()), "a failed overwrite must leave the seed")

		final := bytes.Repeat([]byte("z"), 1<<18)
		require.Equal(t, http.StatusOK, g.put("k", final).Code)
		released := g.bc.takeReleases()
		require.NotEmpty(t, released, "the write after a failed one released nothing")
		positions := map[uint32]bool{}
		for _, r := range released {
			assert.Equal(t, seedEpoch, r.Epoch, "a write must release the generation its record displaced, not the failed one")
			positions[r.Index] = true
		}
		assert.Len(t, positions, p.data+p.parity, "the seed must be released at every position")
	})
}

// The degraded signal counts what committed, not what landed: a shard that
// landed and lost its commit is as absent from the stripe as one never sent.
func TestTheDegradedHeaderCountsUncommittedShards(t *testing.T) {
	cases := []struct {
		p      rsProfile
		faults shardFaults
		short  string
	}{
		{rsProfile{2, 1, 4}, shardFaults{dropCommit: []uint32{0}}, "1"},
		{rsProfile{3, 2, 6}, shardFaults{dropCommit: []uint32{0}}, "1"},
		{rsProfile{3, 2, 6}, shardFaults{failPut: []uint32{1}, dropCommit: []uint32{0}}, "2"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/short-%s", tc.p, tc.short), func(t *testing.T) {
			g := newRealStoreGate(t, tc.p)
			g.bc.arm(tc.faults)
			put := g.put("k", bytes.Repeat([]byte("d"), 1<<18))
			g.bc.arm(shardFaults{})
			require.Equal(t, http.StatusOK, put.Code, put.Body.String())
			assert.Equal(t, tc.short, put.Header().Get(degradedWriteHeader))
		})
	}
}
