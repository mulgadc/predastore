package predastore_test

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/model"
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
	runHost(t, cfg, newMasterKey(t))

	awaitProbe(t, "http://127.0.0.1:"+strconv.Itoa(adminPort)+"/readyz", http.StatusOK)
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}}
	base := "https://127.0.0.1:" + strconv.Itoa(gatePort)
	awaitGate(t, client, base+"/")

	return &s3Client{t: t, base: base, region: cfg.Region,
		accessKey: staleKey, secretKey: staleSecret, http: client}, cfg
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

	// A single-host cluster starts no QUIC listener, so its nodes are reachable
	// only over the in-process pipe the colocated nodes already use. Port 0
	// dials without binding a name of its own.
	pipe := transport.NewPipeTransport(cfg.Hosts[0].Addr, 0)
	defer func() { require.NoError(t, pipe.Close()) }()

	res, err := rpc.NewResolver(cfg, gateNodeID, pipe)
	require.NoError(t, err)

	var blobIDs []config.NodeID
	for _, n := range cfg.Hosts[0].Nodes {
		if n.Role == config.RoleBlob {
			blobIDs = append(blobIDs, n.ID)
		}
	}
	connPool := rpc.NewConnPool(blobIDs[0], res)
	defer connPool.Close()

	bc, err := blob.NewClient(blob.ClientConfig{Client: rpc.NewClient(connPool)})
	require.NoError(t, err)

	fn(bc, blobIDs)
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

// A delete that read a placement naming the orphaned generation removes that
// generation's shards, then stops before its placement removal. The record
// still names the acknowledged generation, which now exists only as retained
// rows, and a read must still serve it.
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
	shards := cfg.RS.Data + cfg.RS.Parity
	commitOrphanGeneration(t, cfg, hash, shards)
	withHostBlob(t, cfg, func(bc *blob.Client, blobIDs []config.NodeID) {
		for _, id := range blobIDs {
			for idx := range shards {
				resp, err := bc.DeleteEpoch(t.Context(), id,
					blob.CommitRequest{Key: hash, Index: uint32(idx), Epoch: orphanEpoch})
				require.NoError(t, err, "scoped delete of orphan shard %d on node %d", idx, id)
				require.True(t, resp.Deleted, "the orphan shard %d on node %d was not removed", idx, id)

				_, err = bc.Stat(t.Context(), id, blob.StatRequest{Key: hash, Index: uint32(idx)})
				require.ErrorIs(t, err, blob.ErrNotFound, "position %d on node %d still has a live row", idx, id)
			}
		}
	})

	status, got, after := c.sendTyped(http.MethodGet, "/"+staleBucket+"/"+staleObject, "", nil)
	require.Equal(t, http.StatusOK, status,
		"ADR-0003:S7: an acknowledged object held only as retained rows stopped reading back: %s", got)
	require.Equal(t, want, got, "the object read back as another generation's body")
	require.Equal(t, putHeaders.Get("ETag"), after.Get("ETag"))
}
