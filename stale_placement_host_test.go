package predastore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/dgraph-io/badger/v4"
	"github.com/mulgadc/predastore"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/blob/engine"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/require"
)

// The shape prod runs: three shards per stripe over three blob nodes, a floor
// of two for any read. Retention is seconds rather than minutes so the far
// side of the window is reachable without waiting it out.
const (
	staleAccount = "777788889999"
	staleKey     = "AKIASTALEPLACEMENTAAA"
	staleSecret  = "stale-placement-test-secret-aaaaaaaaaaaa"
	staleBucket  = "stale-placement-bucket"
	staleObject  = "zone-file"
	staleType    = "text/dns"

	staleRetention = 2 * time.Second
)

// orphanEpoch is larger than any epoch the gate mints, so committing it
// demotes whatever generation the placement record names.
const orphanEpoch uint64 = 0x7fffffffffffff00

// gateNodeID sources the resolver: any node in the configuration will do, and
// the gate is the one that dials without being dialed.
const gateNodeID config.NodeID = 1

func startStaleHost(t *testing.T) (*s3Client, *config.Config) {
	t.Helper()
	client, cfg := staleHostConfig(t)
	runHost(t, cfg, newMasterKey(t))
	awaitProbe(t, "http://127.0.0.1:"+strconv.Itoa(cfg.Hosts[0].AdminPort)+"/readyz", http.StatusOK)
	awaitGate(t, client.http, client.base+"/")
	return client, cfg
}

func staleHostConfig(t *testing.T) (*s3Client, *config.Config) {
	t.Helper()
	certPath, keyPath, _ := testcerts.Generate(t)
	adminPort, gatePort := freePort(t), freePort(t)

	cfg := &config.Config{
		Version: config.Version,
		Region:  "ap-southeast-2",
		RS:      config.RS{Data: 2, Parity: 1},
		Compaction: config.Compaction{
			IntervalSeconds:       1,
			RetainedMaxAgeSeconds: int(staleRetention / time.Second),
		},
		Hosts: []config.Host{{
			ID:        1,
			Addr:      "127.0.0.1",
			DataDir:   t.TempDir(),
			TLSCert:   certPath,
			TLSKey:    keyPath,
			AdminPort: adminPort,
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleGate, Port: gatePort, BindAddr: "127.0.0.1"},
				{ID: 2, Role: config.RoleMeta, Port: freePort(t)},
				{ID: 3, Role: config.RoleBlob, Port: freePort(t)},
				{ID: 4, Role: config.RoleBlob, Port: freePort(t)},
				{ID: 5, Role: config.RoleBlob, Port: freePort(t)},
			},
		}},
		Auth: []config.AuthEntry{
			{AccessKeyID: staleKey, SecretAccessKey: staleSecret, AccountID: staleAccount},
		},
	}
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}}
	base := "https://127.0.0.1:" + strconv.Itoa(gatePort)
	return &s3Client{t: t, base: base, region: cfg.Region,
		accessKey: staleKey, secretKey: staleSecret, http: client}, cfg
}

func TestHostReplaysRaftNamedPlacementBeforeServingShards(t *testing.T) {
	testHostReplaysRaftNamedPlacement(t, false)
}

func TestHostReplaysPlacementPublishedAfterRestart(t *testing.T) {
	testHostReplaysRaftNamedPlacement(t, true)
}

func testHostReplaysRaftNamedPlacement(t *testing.T, latePlacement bool) {
	t.Helper()
	client, cfg := staleHostConfig(t)
	require.NoError(t, cfg.Validate())
	key := newMasterKey(t)
	start := func() func() {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			done <- predastore.Run(ctx, predastore.Options{Config: cfg, HostID: 1, MasterKey: key})
		}()
		stop := sync.OnceFunc(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				t.Error("host did not stop")
			}
		})
		t.Cleanup(stop)
		awaitProbe(t, "http://127.0.0.1:"+strconv.Itoa(cfg.Hosts[0].AdminPort)+"/readyz", http.StatusOK)
		return stop
	}
	stop := start()
	status, body := client.send(http.MethodPut, "/"+staleBucket, "", nil)
	require.Equal(t, http.StatusOK, status, "%s", body)
	want := bytes.Repeat([]byte("acknowledged payload"), 1024)
	objectPath := "/" + staleBucket + "/" + staleObject
	status, body = client.send(http.MethodPut, objectPath, "", want)
	require.Equal(t, http.StatusOK, status, "%s", body)

	hash := model.ObjectHash(staleBucket, staleObject)
	var placement handlers.ObjectToShardNodes
	placementKey := handlers.TableKey(model.TableObjects, string(hash[:]))
	var placementValue []byte
	withHostClients(t, cfg, func(mc *meta.Client, _ *blob.Client, _ []config.NodeID) {
		raw, err := mc.AuthorityGet(t.Context(), placementKey)
		require.NoError(t, err)
		placementValue = raw
		placement, err = handlers.DecodePlacement(raw)
		require.NoError(t, err)
		if latePlacement {
			require.NoError(t, mc.Delete(t.Context(), placementKey))
		}
	})
	stop()
	client.http.CloseIdleConnections()

	host := cfg.Hosts[0]
	holders := placement.AllNodes()
	require.Len(t, holders, 3)
	for index, id := range holders {
		for _, node := range host.Nodes {
			if node.ID != id {
				continue
			}
			db, err := badger.Open(badger.DefaultOptions(filepath.Join(config.NodeDataDir(host, node), "db")))
			require.NoError(t, err)
			require.NoError(t, db.Update(func(txn *badger.Txn) error {
				row := engine.MakeKey(hash, uint32(index))
				if _, err := txn.Get(row); err != nil {
					return err
				}
				return txn.Delete(row)
			}))
			require.NoError(t, db.Close())
		}
	}

	start()
	if latePlacement {
		withHostClients(t, cfg, func(mc *meta.Client, _ *blob.Client, _ []config.NodeID) {
			require.NoError(t, mc.Put(t.Context(), placementKey, placementValue))
		})
	}
	status, body = client.send(http.MethodGet, objectPath, "", nil)
	require.Equal(t, http.StatusOK, status, "%s", body)
	require.Equal(t, want, body)
}

// sendTyped signs and issues one request, optionally declaring a content type,
// and returns the response headers the caller compares a readback against.
func (c *s3Client) sendTyped(method, path, contentType string, body []byte) (int, []byte, http.Header) {
	c.t.Helper()
	req, err := http.NewRequestWithContext(c.t.Context(), method, c.base+path, bytes.NewReader(body))
	require.NoError(c.t, err)

	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", hash)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	require.NoError(c.t, signer.SignHTTP(c.t.Context(),
		aws.Credentials{AccessKeyID: c.accessKey, SecretAccessKey: c.secretKey},
		req, hash, "s3", c.region, time.Now().UTC()))

	resp, err := c.http.Do(req)
	require.NoError(c.t, err)
	defer func() { require.NoError(c.t, resp.Body.Close()) }()

	got, err := io.ReadAll(resp.Body)
	require.NoError(c.t, err)
	return resp.StatusCode, got, resp.Header
}

// withHostBlob runs fn with a blob client that reaches every blob node of a
// single-host cluster.
func withHostBlob(t *testing.T, cfg *config.Config, fn func(bc *blob.Client, blobIDs []config.NodeID)) {
	t.Helper()
	withHostClients(t, cfg, func(_ *meta.Client, bc *blob.Client, blobIDs []config.NodeID) { fn(bc, blobIDs) })
}

// withHostClients runs fn with a meta client and a blob client that reach the
// nodes of a single-host cluster, as the gate's own clients do.
func withHostClients(t *testing.T, cfg *config.Config, fn func(mc *meta.Client, bc *blob.Client, blobIDs []config.NodeID)) {
	t.Helper()

	// A single-host cluster starts no QUIC listener, so its nodes are reachable
	// only over the in-process pipe the colocated nodes already use. Port 0
	// dials without binding a name of its own.
	pipe := transport.NewPipeTransport(cfg.Hosts[0].Addr, 0)
	defer func() { require.NoError(t, pipe.Close()) }()

	res, err := rpc.NewResolver(cfg, gateNodeID, pipe)
	require.NoError(t, err)

	var blobIDs, metaIDs []config.NodeID
	for _, n := range cfg.Hosts[0].Nodes {
		switch n.Role {
		case config.RoleBlob:
			blobIDs = append(blobIDs, n.ID)
		case config.RoleMeta:
			metaIDs = append(metaIDs, n.ID)
		default:
		}
	}
	connPool := rpc.NewConnPool(blobIDs[0], res)
	defer connPool.Close()
	cli := rpc.NewClient(connPool)

	mc, err := meta.NewClient(meta.ClientConfig{Client: cli, Replicas: metaIDs})
	require.NoError(t, err)
	bc, err := blob.NewClient(blob.ClientConfig{Client: cli})
	require.NoError(t, err)

	fn(mc, bc, blobIDs)
}

// commitOrphanGeneration writes and commits a newer generation of every shard
// of one object on every blob node, and never publishes a placement record.
// That is what a writer that dies in the window between the two leaves behind.
func commitOrphanGeneration(t *testing.T, cfg *config.Config, hash [32]byte, shards int) {
	t.Helper()

	withHostBlob(t, cfg, func(bc *blob.Client, blobIDs []config.NodeID) {
		body := []byte("a generation no placement record will ever name")
		for _, id := range blobIDs {
			for idx := range shards {
				req := blob.PutRequest{Key: hash, Index: uint32(idx), Size: int64(len(body)), Epoch: orphanEpoch}
				_, err := bc.Put(t.Context(), id, req, bytes.NewReader(body))
				require.NoError(t, err, "prepare orphan shard %d on node %d", idx, id)

				_, err = bc.Commit(t.Context(), id,
					blob.CommitRequest{Key: hash, Index: uint32(idx), Epoch: orphanEpoch})
				require.NoError(t, err, "commit orphan shard %d on node %d", idx, id)
			}
		}
	})
}

// An acknowledged object must stay readable when a later writer commits its
// shards and dies before publishing. The commit demotes the generation the
// record still names, and reclaiming it on age alone destroys an object the
// client was told it had stored.
//
// Verifies: ADR-0003:S7.
func TestS7_AnOrphanedCommitDoesNotDestroyAnAcknowledgedObject(t *testing.T) {
	c, cfg := startStaleHost(t)

	status, body := c.send(http.MethodPut, "/"+staleBucket, "", nil)
	require.Equal(t, http.StatusOK, status, "CreateBucket: %s", body)

	want := []byte("$ORIGIN spx3.net.\n@ IN SOA ns1 hostmaster 1 7200 3600 1209600 3600\n")
	status, body, putHeaders := c.sendTyped(http.MethodPut, "/"+staleBucket+"/"+staleObject, staleType, want)
	require.Equal(t, http.StatusOK, status, "PutObject: %s", body)
	etag := putHeaders.Get("ETag")
	require.NotEmpty(t, etag, "PutObject returned no ETag to compare against")

	status, got, before := c.sendTyped(http.MethodGet, "/"+staleBucket+"/"+staleObject, "", nil)
	require.Equal(t, http.StatusOK, status, "GetObject before the orphan commit: %s", got)
	require.Equal(t, want, got)

	hash := model.ObjectHash(staleBucket, staleObject)
	commitOrphanGeneration(t, cfg, hash, cfg.RS.Data+cfg.RS.Parity)

	// Long enough to span many sweep intervals and several retention windows,
	// which is what destroyed this object before a commit stopped starting a
	// clock. The sweep's own accounting is asserted in the engine tests, which
	// call it directly; nothing here can observe it.
	deadline := time.Now().Add(3 * staleRetention)
	for time.Now().Before(deadline) {
		status, got, after := c.sendTyped(http.MethodGet, "/"+staleBucket+"/"+staleObject, "", nil)
		require.Equal(t, http.StatusOK, status,
			"an acknowledged object stopped reading back after an orphaned commit: %s", got)
		require.Equal(t, want, got, "the object read back as another generation's body")

		// The orphaned generation holds a different body, so metadata that
		// still matches is what says the read was served from the right one.
		require.Equal(t, etag, after.Get("ETag"), "ETag changed after the orphaned commit")
		require.Equal(t, before.Get("Content-Length"), after.Get("Content-Length"),
			"Content-Length changed after the orphaned commit")
		require.Equal(t, staleType, after.Get("Content-Type"),
			"Content-Type changed after the orphaned commit")

		time.Sleep(200 * time.Millisecond)
	}
}

// Holding a generation until it is released must not stop an ordinary
// overwrite from replacing it. This covers publication and readback over S3:
// a release that fired too early takes the new generation with it. Whether the
// displaced one is reclaimed is asserted in the engine tests.
func TestAPublishedOverwriteReplacesTheGenerationItDisplaced(t *testing.T) {
	c, _ := startStaleHost(t)

	status, body := c.send(http.MethodPut, "/"+staleBucket, "", nil)
	require.Equal(t, http.StatusOK, status, "CreateBucket: %s", body)

	first := []byte("$ORIGIN spx3.net.\n@ IN SOA ns1 hostmaster 1 7200 3600 1209600 3600\n")
	status, body, _ = c.sendTyped(http.MethodPut, "/"+staleBucket+"/"+staleObject, staleType, first)
	require.Equal(t, http.StatusOK, status, "PutObject: %s", body)

	second := []byte("$ORIGIN spx3.net.\n@ IN SOA ns1 hostmaster 2 7200 3600 1209600 3600\n")
	status, body, putHeaders := c.sendTyped(http.MethodPut, "/"+staleBucket+"/"+staleObject, staleType, second)
	require.Equal(t, http.StatusOK, status, "PutObject overwrite: %s", body)
	etag := putHeaders.Get("ETag")
	require.NotEmpty(t, etag, "the overwrite returned no ETag to compare against")

	// Spans the retention window the release starts and many sweep intervals,
	// so the reclaim of the displaced generation happens inside the loop.
	deadline := time.Now().Add(3 * staleRetention)
	for time.Now().Before(deadline) {
		status, got, headers := c.sendTyped(http.MethodGet, "/"+staleBucket+"/"+staleObject, "", nil)
		require.Equal(t, http.StatusOK, status, "GetObject after the overwrite: %s", got)
		require.Equal(t, second, got, "the overwrite did not replace the generation it displaced")
		require.Equal(t, etag, headers.Get("ETag"), "ETag changed after the overwrite")
		require.Equal(t, staleType, headers.Get("Content-Type"), "Content-Type changed after the overwrite")

		time.Sleep(200 * time.Millisecond)
	}
}

// racedPlacementMeta lets a delete read a placement that names another
// generation. The first read of placeKey publishes raced, reads it, and puts
// the acknowledged record back. A removal of placeKey fails, as a crash would.
type racedPlacementMeta struct {
	handlers.MetaClient

	placeKey      string
	acknowledged  []byte
	raced         []byte
	racing        atomic.Bool
	removeRefused atomic.Int32
}

var errDeleteStopped = errors.New("the delete stopped before its placement removal")

func (m *racedPlacementMeta) Get(ctx context.Context, key string) ([]byte, error) {
	if key != m.placeKey || !m.racing.CompareAndSwap(false, true) {
		return m.MetaClient.Get(ctx, key)
	}

	if err := m.Put(ctx, key, m.raced); err != nil {
		return nil, err
	}
	data, err := m.MetaClient.Get(ctx, key)
	if restoreErr := m.Put(ctx, key, m.acknowledged); restoreErr != nil {
		return nil, restoreErr
	}

	return data, err
}

func (m *racedPlacementMeta) Delete(ctx context.Context, key string) error {
	if key == m.placeKey {
		m.removeRefused.Add(1)
		return errDeleteStopped
	}

	return m.MetaClient.Delete(ctx, key)
}

// scopedDelete is one DeleteEpoch the inline delete sent, and its answer.
type scopedDelete struct {
	node    config.NodeID
	index   uint32
	epoch   uint64
	deleted bool
}

// recordingBlob records every DeleteEpoch the inline delete sends.
type recordingBlob struct {
	handlers.BlobClient

	mu      sync.Mutex
	deletes []scopedDelete
}

func (b *recordingBlob) DeleteEpoch(ctx context.Context, node config.NodeID, req blob.CommitRequest) (*blob.DeleteResponse, error) {
	resp, err := b.BlobClient.DeleteEpoch(ctx, node, req)
	if err == nil {
		b.mu.Lock()
		b.deletes = append(b.deletes, scopedDelete{node: node, index: req.Index, epoch: req.Epoch, deleted: resp.Deleted})
		b.mu.Unlock()
	}

	return resp, err
}

// The inline delete reads a placement naming the orphaned generation, then the
// acknowledged record is put back. The delete removes the orphan shards and stops
// before its placement removal, and a read must still serve the retained rows.
//
// Verifies: ADR-0003:S7.
func TestS7_AScopedDeleteThatStopsLeavesARetainedAcknowledgedObjectReadable(t *testing.T) {
	c, cfg := startStaleHost(t)

	status, body := c.send(http.MethodPut, "/"+staleBucket, "", nil)
	require.Equal(t, http.StatusOK, status, "CreateBucket: %s", body)

	want := []byte("$ORIGIN spx3.net.\n@ IN SOA ns1 hostmaster 1 7200 3600 1209600 3600\n")
	status, body, putHeaders := c.sendTyped(http.MethodPut, "/"+staleBucket+"/"+staleObject, staleType, want)
	require.Equal(t, http.StatusOK, status, "PutObject: %s", body)

	hash := model.ObjectHash(staleBucket, staleObject)
	commitOrphanGeneration(t, cfg, hash, cfg.RS.Data+cfg.RS.Parity)

	withHostClients(t, cfg, func(mc *meta.Client, bc *blob.Client, _ []config.NodeID) {
		placeKey := handlers.TableKey(model.TableObjects, string(hash[:]))
		acknowledged, err := mc.Get(t.Context(), placeKey)
		require.NoError(t, err)
		place, err := handlers.DecodePlacement(acknowledged)
		require.NoError(t, err)
		require.NotEqual(t, orphanEpoch, place.WriteEpoch, "setup: the record must name the acknowledged generation")
		orphanPlace := place
		orphanPlace.WriteEpoch = orphanEpoch
		raced, err := handlers.EncodePlacement(orphanPlace)
		require.NoError(t, err)

		rm := &racedPlacementMeta{MetaClient: mc, placeKey: placeKey, acknowledged: acknowledged, raced: raced}
		rb := &recordingBlob{BlobClient: bc}
		cache := handlers.NewBucketCache(nil)
		reqCtx := handlers.WithObject(handlers.WithBucket(t.Context(), model.Bucket{Name: staleBucket}),
			model.Object{Bucket: model.Bucket{Name: staleBucket}, Key: staleObject})
		req := httptest.NewRequest(http.MethodDelete, "/"+staleBucket+"/"+staleObject, nil).WithContext(reqCtx)
		rr := httptest.NewRecorder()
		handlers.DeleteObject(rm, rb, cache, handlers.Config{
			Region: cfg.Region, DataShards: cfg.RS.Data, ParityShards: cfg.RS.Parity,
		}).ServeHTTP(rr, req)
		require.Equal(t, http.StatusInternalServerError, rr.Code, "setup: the delete must stop at its placement removal: %s", rr.Body.String())
		require.Equal(t, int32(1), rm.removeRefused.Load(), "setup: the delete must reach its placement removal once")

		positions := place.AllNodes()
		require.Len(t, rb.deletes, len(positions), "the delete must send one scoped delete per position")
		for _, d := range rb.deletes {
			require.Equal(t, orphanEpoch, d.epoch, "the delete must name the generation its placement read named")
			require.True(t, d.deleted, "the orphan shard %d on node %d was not removed", d.index, d.node)
		}
		for idx, id := range positions {
			_, err := bc.Stat(t.Context(), id, blob.StatRequest{Key: hash, Index: uint32(idx)})
			require.ErrorIs(t, err, blob.ErrNotFound, "position %d on node %d still has a live row", idx, id)
		}

		current, err := mc.Get(t.Context(), placeKey)
		require.NoError(t, err)
		require.Equal(t, acknowledged, current, "setup: the record must name the acknowledged generation again")
	})

	status, got, after := c.sendTyped(http.MethodGet, "/"+staleBucket+"/"+staleObject, "", nil)
	require.Equal(t, http.StatusOK, status,
		"ADR-0003:S7: an acknowledged object held only as retained rows stopped reading back: %s", got)
	require.Equal(t, want, got, "the object read back as another generation's body")
	require.Equal(t, putHeaders.Get("ETag"), after.Get("ETag"))
}
