package clusterrun

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/mulgadc/predastore/internal/gateway"
	"github.com/mulgadc/predastore/internal/gateway/auth"
	"github.com/mulgadc/predastore/internal/gateway/handlers"
	"github.com/mulgadc/predastore/internal/topology"
	"github.com/mulgadc/predastore/pkg/masterkey"
)

const (
	testRegion    = "ap-southeast-2"
	testAccessKey = "AKIAIOSFODNN7EXAMPLE"
	testSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	testAccountID = "123456789012"
)

// signedRequest builds an S3 request signed the way a client would sign it, so
// it passes the gateway's own auth middleware rather than bypassing it.
func signedRequest(t *testing.T, method, target string, body []byte) *http.Request {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rdr)
	signInPlace(t, req, body)
	return req
}

// signInPlace signs an already-built request, for the cases that need a header
// set before signing.
func signInPlace(t *testing.T, req *http.Request, body []byte) {
	t.Helper()
	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])
	// The server recovers the signed payload hash from this header; the SDK doesn't set it.
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	signer := v4.NewSigner(func(so *v4.SignerOptions) { so.DisableURIPathEscaping = true })
	if err := signer.SignHTTP(context.Background(),
		aws.Credentials{AccessKeyID: testAccessKey, SecretAccessKey: testSecretKey},
		req, payloadHash, "s3", testRegion, time.Now().UTC()); err != nil {
		t.Fatalf("sign request: %v", err)
	}
}

// serve runs one request through the gateway and returns the recorder.
func serve(t *testing.T, h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

// testMasterKey writes a fresh 32-byte key to disk and loads it the way the
// entrypoint does.
func testMasterKey(t *testing.T) *masterkey.Key {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("rand: %v", err)
	}
	path := filepath.Join(t.TempDir(), "master.key")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	key, err := masterkey.Load(path)
	if err != nil {
		t.Fatalf("masterkey.Load: %v", err)
	}
	return key
}

// TestClusterRuntimeObjectRoundTrip runs a whole cluster — three storage
// nodes and one state replica — in one process over pipe streams, with no
// network sockets and no certs, and drives S3 operations through the gateway's
// own handlers. Each node has its own service and rpc server, so this also
// covers several servers coexisting on the shared pipe registry.
func TestClusterRuntimeObjectRoundTrip(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &gateway.Config{
		Region: testRegion,
		RS:     gateway.RS{Data: 2, Parity: 1},
		Auth: []auth.Entry{{
			AccessKeyID:     testAccessKey,
			SecretAccessKey: testSecretKey,
			AccountID:       testAccountID,
		}},
		Hosts: []topology.Host{
			{ID: 1, BindAddr: "127.0.0.1:16660", PublicAddr: "127.0.0.1:16660", DataDir: dataDir},
		},
		ClusterNodes: []topology.Node{
			{ID: 1, HostID: 1, Role: topology.RoleShardStorage},
			{ID: 2, HostID: 1, Role: topology.RoleShardStorage},
			{ID: 3, HostID: 1, Role: topology.RoleShardStorage},
			{ID: 4, HostID: 1, Role: topology.RoleStateReplica},
		},
	}

	// No certs: every node is local, so no network socket opens.
	rt, err := Build(cfg, []int{1, 2, 3, 4}, "", "", testMasterKey(t))
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	// Serve the nodes for the duration of the test, then drain them the way
	// a signal would.
	runCtx, stopRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- rt.Run(runCtx) }()
	t.Cleanup(func() {
		stopRun()
		select {
		case <-runDone:
		case <-time.After(30 * time.Second):
			t.Error("runtime did not drain")
		}
	})

	if err := rt.WaitReady(30 * time.Second); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	gw := gateway.NewHandler(cfg, rt.Clients, auth.NewConfigProvider(cfg.Auth))

	if rr := serve(t, gw, signedRequest(t, http.MethodPut, "/it-bucket", nil)); rr.Code != http.StatusOK {
		t.Fatalf("CreateBucket: status %d, body %s", rr.Code, rr.Body.String())
	}

	payload := make([]byte, 1<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if rr := serve(t, gw, signedRequest(t, http.MethodPut, "/it-bucket/dir/blob.bin", payload)); rr.Code != http.StatusOK {
		t.Fatalf("PutObject: status %d, body %s", rr.Code, rr.Body.String())
	}

	rr := serve(t, gw, signedRequest(t, http.MethodGet, "/it-bucket/dir/blob.bin", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GetObject: status %d, body %s", rr.Code, rr.Body.String())
	}
	if !bytes.Equal(rr.Body.Bytes(), payload) {
		t.Fatalf("object mismatch: got %d bytes, want %d", rr.Body.Len(), len(payload))
	}

	// Range read exercises the single-shard fast path over rpc.
	rangeReq := httptest.NewRequest(http.MethodGet, "/it-bucket/dir/blob.bin", nil)
	rangeReq.Header.Set("Range", "bytes=100-1123")
	signInPlace(t, rangeReq, nil)
	rr = serve(t, gw, rangeReq)
	if rr.Code != http.StatusPartialContent {
		t.Fatalf("GetObject range: status %d, body %s", rr.Code, rr.Body.String())
	}
	if !bytes.Equal(rr.Body.Bytes(), payload[100:1124]) {
		t.Fatalf("range mismatch: got %d bytes", rr.Body.Len())
	}

	rr = serve(t, gw, signedRequest(t, http.MethodGet, "/it-bucket", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("ListObjects: status %d, body %s", rr.Code, rr.Body.String())
	}
	var listing handlers.ListObjectsV2
	if err := xml.Unmarshal(rr.Body.Bytes(), &listing); err != nil {
		t.Fatalf("decode listing: %v", err)
	}
	if listing.Contents == nil || len(*listing.Contents) != 1 || (*listing.Contents)[0].Key != "dir/blob.bin" {
		t.Fatalf("listing did not report the object: %s", rr.Body.String())
	}

	if rr := serve(t, gw, signedRequest(t, http.MethodDelete, "/it-bucket/dir/blob.bin", nil)); rr.Code != http.StatusNoContent {
		t.Fatalf("DeleteObject: status %d, body %s", rr.Code, rr.Body.String())
	}
	if rr := serve(t, gw, signedRequest(t, http.MethodGet, "/it-bucket/dir/blob.bin", nil)); rr.Code != http.StatusNotFound {
		t.Fatalf("GetObject after delete: status %d", rr.Code)
	}
}

// TestRelativeDataDirUsesBasePath pins the launcher contract: a config with a
// relative data_dir must land under -base-path, not the working directory.
// Getting this wrong writes cluster state into the repo.
func TestRelativeDataDirUsesBasePath(t *testing.T) {
	base := t.TempDir()
	cfg := &gateway.Config{
		BasePath: base,
		RS:       gateway.RS{Data: 2, Parity: 1},
		Hosts: []topology.Host{
			{ID: 1, BindAddr: "127.0.0.1:16661", PublicAddr: "127.0.0.1:16661", DataDir: "data/host-1"},
		},
		ClusterNodes: []topology.Node{
			{ID: 1, HostID: 1, Role: topology.RoleShardStorage},
			{ID: 2, HostID: 1, Role: topology.RoleShardStorage},
			{ID: 3, HostID: 1, Role: topology.RoleStateReplica},
		},
	}

	rt, err := Build(cfg, []int{1, 2, 3}, "", "", testMasterKey(t))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer rt.Close()

	if _, err := os.Stat(filepath.Join(base, "data", "host-1", "node-1")); err != nil {
		t.Fatalf("shard store not under base path: %v", err)
	}
	if _, err := os.Stat(filepath.Join("data", "host-1")); err == nil {
		t.Fatal("relative data_dir leaked into the working directory")
	}
}
