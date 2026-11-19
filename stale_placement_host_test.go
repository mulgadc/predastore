package predastore_test

import (
	"bytes"
	"crypto/tls"
	"net/http"
	"strconv"
	"testing"
	"time"

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

// commitOrphanGeneration writes and commits a newer generation of every shard
// of one object on every blob node, and never publishes a placement record.
// That is what a writer that dies in the window between the two leaves behind.
func commitOrphanGeneration(t *testing.T, cfg *config.Config, hash [32]byte, shards int) {
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
	status, body = c.send(http.MethodPut, "/"+staleBucket+"/"+staleObject, "", want)
	require.Equal(t, http.StatusOK, status, "PutObject: %s", body)

	status, got := c.send(http.MethodGet, "/"+staleBucket+"/"+staleObject, "", nil)
	require.Equal(t, http.StatusOK, status, "GetObject before the orphan commit: %s", got)
	require.Equal(t, want, got)

	hash := model.ObjectHash(staleBucket, staleObject)
	commitOrphanGeneration(t, cfg, hash, cfg.RS.Data+cfg.RS.Parity)

	// Past the retention window, with several sweeps behind it. The object is
	// still the one the client stored, so every read has to return it.
	deadline := time.Now().Add(3 * staleRetention)
	for time.Now().Before(deadline) {
		status, got = c.send(http.MethodGet, "/"+staleBucket+"/"+staleObject, "", nil)
		require.Equal(t, http.StatusOK, status,
			"an acknowledged object stopped reading back after an orphaned commit: %s", got)
		require.Equal(t, want, got, "the object read back as another generation's body")
		time.Sleep(200 * time.Millisecond)
	}
}
