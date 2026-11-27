package predastore_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/predastore"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/gate/reaper"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/stretchr/testify/require"
)

const (
	restartAccount = "444455556666"
	restartKey     = "AKIARESTARTREPLAYAAA"
	restartSecret  = "restart-replay-test-secret-aaaaaaaaaaaaa"
	restartBucket  = "restart-replay-bucket"

	restartIntentAge = time.Hour

	// restartWriterNode mints the writer's epochs apart from the host's gate.
	restartWriterNode config.NodeID = 99
)

// restartHost is one single-host cluster that stops and starts again on the
// same data directory and master key. Its own reaper is off, so only the
// pass a test runs, on an injected clock, settles an intent.
type restartHost struct {
	t    *testing.T
	cfg  *config.Config
	key  *masterkey.Key
	stop func()
}

func newRestartHost(t *testing.T) *restartHost {
	t.Helper()
	certPath, keyPath, _ := testcerts.Generate(t)
	off := false
	cfg := &config.Config{
		Version: config.Version,
		Region:  "ap-southeast-2",
		RS:      config.RS{Data: 2, Parity: 1},
		Reaper:  config.Reaper{Enabled: &off},
		Hosts: []config.Host{{
			ID:        1,
			Addr:      "127.0.0.1",
			DataDir:   t.TempDir(),
			TLSCert:   certPath,
			TLSKey:    keyPath,
			AdminPort: freePort(t),
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleGate, Port: freePort(t), BindAddr: "127.0.0.1"},
				{ID: 2, Role: config.RoleMeta, Port: freePort(t)},
				{ID: 3, Role: config.RoleBlob, Port: freePort(t)},
				{ID: 4, Role: config.RoleBlob, Port: freePort(t)},
				{ID: 5, Role: config.RoleBlob, Port: freePort(t)},
			},
		}},
		Auth: []config.AuthEntry{
			{AccessKeyID: restartKey, SecretAccessKey: restartSecret, AccountID: restartAccount},
		},
	}
	return &restartHost{t: t, cfg: cfg, key: newMasterKey(t)}
}

// start runs the host until stop, and returns a signing client once the gate
// answers.
func (h *restartHost) start() *s3Client {
	h.t.Helper()
	require.Nil(h.t, h.stop, "setup: the host is already running")
	require.NoError(h.t, h.cfg.Validate())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- predastore.Run(ctx, predastore.Options{Config: h.cfg, HostID: 1, MasterKey: h.key})
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				h.t.Error("host did not stop")
			}
		})
	}
	h.t.Cleanup(stop)
	h.stop = stop

	host := h.cfg.Hosts[0]
	awaitProbe(h.t, "http://127.0.0.1:"+strconv.Itoa(host.AdminPort)+"/readyz", http.StatusOK)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	base := "https://127.0.0.1:" + strconv.Itoa(host.Nodes[0].Port)
	awaitGate(h.t, client, base+"/")

	return &s3Client{t: h.t, base: base, region: h.cfg.Region,
		accessKey: restartKey, secretKey: restartSecret, http: client}
}

func (h *restartHost) restart() *s3Client {
	h.t.Helper()
	h.stop()
	h.stop = nil
	return h.start()
}

// keptIntentMeta is the store as a writer that stops before it drops its
// settled intent sees it: every other call reaches the real replica.
type keptIntentMeta struct {
	*meta.Client

	refused atomic.Int32
}

func (m *keptIntentMeta) Delete(ctx context.Context, key string) error {
	if strings.HasPrefix(key, handlers.IntentPrefix) {
		m.refused.Add(1)
		return errStoppedWriter
	}
	return m.Client.Delete(ctx, key)
}

var _ handlers.MetaClient = (*keptIntentMeta)(nil)

var errStoppedWriter = errors.New("the writer stopped before it dropped its intent")

// conditionalCreate runs the gate's PutObject with If-None-Match: * against
// the host's meta and blob nodes. The create commits, and its intent stays
// in the store as committed. It answers the ETag the create returned.
func (h *restartHost) conditionalCreate(object string, body []byte) string {
	h.t.Helper()
	var etag string
	withHostClients(h.t, h.cfg, func(mc *meta.Client, bc *blob.Client, blobIDs []config.NodeID) {
		epochs, err := handlers.NewEpochMinter(restartWriterNode)
		require.NoError(h.t, err)
		wm := &keptIntentMeta{Client: mc}
		put := handlers.PutObject(wm, bc, placement.NewRing(blobIDs), handlers.NewBucketCache(nil), handlers.Config{
			Region: h.cfg.Region, DataShards: h.cfg.RS.Data, ParityShards: h.cfg.RS.Parity, Epochs: epochs,
		})

		reqCtx := handlers.WithObject(handlers.WithBucket(h.t.Context(), model.Bucket{Name: restartBucket}),
			model.Object{Bucket: model.Bucket{Name: restartBucket}, Key: object})
		req := httptest.NewRequest(http.MethodPut, "/"+restartBucket+"/"+object, bytes.NewReader(body)).WithContext(reqCtx)
		req.ContentLength = int64(len(body))
		req.Header.Set("If-None-Match", "*")
		rr := httptest.NewRecorder()
		put.ServeHTTP(rr, req)

		require.Equal(h.t, http.StatusOK, rr.Code, "conditional PutObject: %s", rr.Body.String())
		require.Equal(h.t, int32(1), wm.refused.Load(), "setup: the writer must stop at its intent delete")
		etag = rr.Header().Get("ETag")
		require.NotEmpty(h.t, etag)
	})
	return etag
}

// intents reads every intent row from the leader once it has applied its log.
func (h *restartHost) intents() []handlers.Intent {
	h.t.Helper()
	var out []handlers.Intent
	withHostClients(h.t, h.cfg, func(mc *meta.Client, _ *blob.Client, _ []config.NodeID) {
		items, err := mc.LeaderScanFrom(h.t.Context(), handlers.IntentPrefix, "", 0)
		require.NoError(h.t, err)
		for _, item := range items {
			in, err := handlers.DecodeIntent(item.Value)
			require.NoError(h.t, err)
			out = append(out, in)
		}
	})
	return out
}

func (h *restartHost) requireCommittedIntents(objects ...string) {
	h.t.Helper()
	got := h.intents()
	keys := make([]string, 0, len(got))
	for _, in := range got {
		require.Equal(h.t, handlers.IntentCommitted, in.State,
			"the intent of %q was re-evaluated after the conditional create committed", in.Key)
		keys = append(keys, in.Key)
	}
	require.ElementsMatch(h.t, objects, keys, "the intent rows after the restart")
}

// reapOnce runs one reaper pass with a clock past the intent age, so the
// pass would fence any intent still pending.
func (h *restartHost) reapOnce(settled int) {
	h.t.Helper()
	withHostClients(h.t, h.cfg, func(mc *meta.Client, bc *blob.Client, _ []config.NodeID) {
		aged := time.Now().Add(2 * restartIntentAge)
		svc, err := reaper.New(reaper.Config{
			Meta: mc, Blob: bc, Workers: 2, IntentMaxAge: restartIntentAge,
			Now: func() time.Time { return aged },
		})
		require.NoError(h.t, err)
		require.NoError(h.t, svc.Pass(h.t.Context()))
		st := svc.Stats()
		require.Equal(h.t, int64(settled), st.IntentsSettled, "the intents the pass settled")
		require.Zero(h.t, st.IntentsFailed)
	})
}

func (h *restartHost) appliedIndex() uint64 {
	h.t.Helper()
	var applied uint64
	withHostClients(h.t, h.cfg, func(mc *meta.Client, _ *blob.Client, _ []config.NodeID) {
		st, err := mc.Status(h.t.Context(), h.cfg.Hosts[0].Nodes[1].ID)
		require.NoError(h.t, err)
		applied, err = strconv.ParseUint(st.AppliedIndex, 10, 64)
		require.NoError(h.t, err)
	})
	return applied
}

// snapshotIndex is the newest index a finished meta snapshot on disk covers,
// or zero when there is none.
func (h *restartHost) snapshotIndex() uint64 {
	h.t.Helper()
	var newest uint64
	require.NoError(h.t, filepath.WalkDir(h.cfg.Hosts[0].DataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() != "meta.json" {
			return err
		}
		dir := filepath.Dir(path)
		if strings.HasSuffix(dir, ".tmp") || filepath.Base(filepath.Dir(dir)) != "snapshots" {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var m struct {
			Index uint64 `json:"Index"`
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
		newest = max(newest, m.Index)
		return nil
	}))
	return newest
}

func requireReadsBack(t *testing.T, c *s3Client, object string, want []byte, etag string) {
	t.Helper()
	status, got, headers := c.sendTyped(http.MethodGet, "/"+restartBucket+"/"+object, "", nil)
	require.Equal(t, http.StatusOK, status, "GET %s after the restart: %s", object, got)
	require.Equal(t, want, got, "GET %s read back another body", object)
	require.Equal(t, etag, headers.Get("ETag"), "GET %s read back another ETag", object)
}

func createRestartBucket(t *testing.T, c *s3Client) {
	t.Helper()
	status, body := c.send(http.MethodPut, "/"+restartBucket, "", nil)
	require.Equal(t, http.StatusOK, status, "CreateBucket: %s", body)
}

// A replica with no snapshot replays its whole log on restart. The
// conditional create keeps its result, its intent stays committed, and an
// aged reaper pass settles the intent and leaves the object readable.
// Verifies: ADR-0003:S7, ADR-0003:S11.
func TestS7_AConditionalCreateReadsBackAfterARestartThatReplaysTheLog(t *testing.T) {
	h := newRestartHost(t)
	c := h.start()
	createRestartBucket(t, c)

	want := []byte("conditional create replayed from the log\n")
	etag := h.conditionalCreate("replayed", want)
	h.requireCommittedIntents("replayed")

	c = h.restart()
	require.Zero(t, h.snapshotIndex(), "setup: the replica must restart with no snapshot")

	requireReadsBack(t, c, "replayed", want, etag)
	h.requireCommittedIntents("replayed")

	h.reapOnce(1)
	require.Empty(t, h.intents(), "the pass must drop the committed intent")
	requireReadsBack(t, c, "replayed", want, etag)
}

// A replica restores a snapshot that holds one conditional create, then
// replays the log after it, which holds a second. Both keep their result
// through the restore and the replay, and through an aged reaper pass.
// Verifies: ADR-0003:S7, ADR-0003:S11.
func TestS7_AConditionalCreateReadsBackAfterARestartThatRestoresASnapshot(t *testing.T) {
	h := newRestartHost(t)
	h.cfg.Meta = config.Meta{SnapshotIntervalSeconds: 1, SnapshotThreshold: 1}
	c := h.start()
	createRestartBucket(t, c)

	inSnapshot := []byte("conditional create held in the snapshot\n")
	etagSnap := h.conditionalCreate("in-snapshot", inSnapshot)
	applied := h.appliedIndex()
	require.Eventually(t, func() bool { return h.snapshotIndex() >= applied },
		20*time.Second, 20*time.Millisecond, "setup: the replica never snapshotted index %d", applied)

	// Default snapshot tuning: no snapshot is taken again during the test.
	h.cfg.Meta = config.Meta{}
	h.restart()
	afterSnapshot := []byte("conditional create replayed after the snapshot\n")
	etagLog := h.conditionalCreate("after-snapshot", afterSnapshot)
	require.Less(t, h.snapshotIndex(), h.appliedIndex(), "setup: the second create must be after the snapshot")

	c = h.restart()
	requireReadsBack(t, c, "in-snapshot", inSnapshot, etagSnap)
	requireReadsBack(t, c, "after-snapshot", afterSnapshot, etagLog)
	h.requireCommittedIntents("in-snapshot", "after-snapshot")

	h.reapOnce(2)
	require.Empty(t, h.intents(), "the pass must drop the committed intents")
	requireReadsBack(t, c, "in-snapshot", inSnapshot, etagSnap)
	requireReadsBack(t, c, "after-snapshot", afterSnapshot, etagLog)
}
