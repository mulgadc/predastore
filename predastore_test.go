package predastore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/mulgadc/predastore/internal/gateway/handlers"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/mulgadc/predastore/internal/testport"
	"github.com/mulgadc/predastore/pkg/masterkey"
)

const (
	testRegion    = "ap-southeast-2"
	testAccessKey = "AKIAIOSFODNN7EXAMPLE"
	testSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	testAccountID = "123456789012"
)

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

// writeConfig lays down a whole-cluster config file — three storage nodes and
// one state replica on a single host — and returns its path. Going through the
// file rather than a struct literal keeps the TOML tags under test.
func writeConfig(t *testing.T, dataDir string, hostPort int) string {
	t.Helper()
	toml := fmt.Sprintf(`
version = "1.0"
region = %q

[rs]
data = 2
parity = 1

[[host]]
id = 1
bind_addr = "127.0.0.1:%d"
public_addr = "127.0.0.1:%d"
data_dir = %q

[[node]]
id = 1
host_id = 1
role = "shard-storage"

[[node]]
id = 2
host_id = 1
role = "shard-storage"

[[node]]
id = 3
host_id = 1
role = "shard-storage"

[[node]]
id = 4
host_id = 1
role = "state-replica"

[[auth]]
access_key_id = %q
secret_access_key = %q
account_id = %q
`, testRegion, hostPort, hostPort, dataDir, testAccessKey, testSecretKey, testAccountID)

	path := filepath.Join(t.TempDir(), "predastore.toml")
	if err := os.WriteFile(path, []byte(toml), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// signedRequest builds an S3 request signed the way a client would sign it, so
// it passes the gateway's own auth middleware rather than bypassing it.
func signedRequest(t *testing.T, method, url string, body []byte) *http.Request {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, url, rdr)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
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

// do sends one request and returns the status and body, closing the response.
func do(t *testing.T, c *http.Client, req *http.Request) (int, []byte) {
	t.Helper()
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, body
}

// waitForListener blocks until the gateway completes a TLS handshake. Serving
// starts only after consensus elects a leader, so the deadline covers an
// election rather than just a bind.
func waitForListener(t *testing.T, addr string, pool *x509.CertPool) {
	t.Helper()
	dialer := &net.Dialer{Timeout: time.Second}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := tls.DialWithDialer(dialer, "tcp", addr,
			&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13})
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("gateway never listened on %s", addr)
}

// TestNodeObjectRoundTrip boots a whole cluster — three storage nodes and one
// state replica — in one process through the public facade, and drives real
// signed S3 traffic over its HTTPS listener. Nodes talk over pipe streams, so
// no inter-node socket opens; each has its own service and rpc server, which
// also covers several servers coexisting on the shared pipe registry.
func TestNodeObjectRoundTrip(t *testing.T) {
	base := testport.Block(t, 2)
	s3Addr := fmt.Sprintf("127.0.0.1:%d", base)
	certPath, keyPath, pool := testcerts.Generate(t)

	cfg, err := LoadConfig(writeConfig(t, t.TempDir(), base+1))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	node, err := New(Options{
		Config:    cfg,
		Host:      "127.0.0.1",
		Port:      base,
		TLSCert:   certPath,
		TLSKey:    keyPath,
		MasterKey: testMasterKey(t),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Serve for the duration of the test, then drain the way a signal would.
	runCtx, stopRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- node.Run(runCtx) }()
	t.Cleanup(func() {
		stopRun()
		select {
		case err := <-runDone:
			if err != nil {
				t.Errorf("Run returned %v, want a clean shutdown", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("node did not drain")
		}
	})

	waitForListener(t, s3Addr, pool)
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}},
		Timeout:   60 * time.Second,
	}
	url := func(path string) string { return "https://" + s3Addr + path }

	if code, body := do(t, client, signedRequest(t, http.MethodPut, url("/it-bucket"), nil)); code != http.StatusOK {
		t.Fatalf("CreateBucket: status %d, body %s", code, body)
	}

	payload := make([]byte, 1<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	if code, body := do(t, client, signedRequest(t, http.MethodPut, url("/it-bucket/dir/blob.bin"), payload)); code != http.StatusOK {
		t.Fatalf("PutObject: status %d, body %s", code, body)
	}

	code, body := do(t, client, signedRequest(t, http.MethodGet, url("/it-bucket/dir/blob.bin"), nil))
	if code != http.StatusOK {
		t.Fatalf("GetObject: status %d, body %s", code, body)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("object mismatch: got %d bytes, want %d", len(body), len(payload))
	}

	// Range read exercises the single-shard fast path over rpc.
	rangeReq := signedRequest(t, http.MethodGet, url("/it-bucket/dir/blob.bin"), nil)
	rangeReq.Header.Set("Range", "bytes=100-1123")
	signInPlace(t, rangeReq, nil)
	code, body = do(t, client, rangeReq)
	if code != http.StatusPartialContent {
		t.Fatalf("GetObject range: status %d, body %s", code, body)
	}
	if !bytes.Equal(body, payload[100:1124]) {
		t.Fatalf("range mismatch: got %d bytes", len(body))
	}

	code, body = do(t, client, signedRequest(t, http.MethodGet, url("/it-bucket"), nil))
	if code != http.StatusOK {
		t.Fatalf("ListObjects: status %d, body %s", code, body)
	}
	var listing handlers.ListObjectsV2
	if err := xml.Unmarshal(body, &listing); err != nil {
		t.Fatalf("decode listing: %v", err)
	}
	if listing.Contents == nil || len(*listing.Contents) != 1 || (*listing.Contents)[0].Key != "dir/blob.bin" {
		t.Fatalf("listing did not report the object: %s", body)
	}

	if code, body := do(t, client, signedRequest(t, http.MethodDelete, url("/it-bucket/dir/blob.bin"), nil)); code != http.StatusNoContent {
		t.Fatalf("DeleteObject: status %d, body %s", code, body)
	}
	if code, _ := do(t, client, signedRequest(t, http.MethodGet, url("/it-bucket/dir/blob.bin"), nil)); code != http.StatusNotFound {
		t.Fatalf("GetObject after delete: status %d", code)
	}
}

// TestRelativeDataDirUsesBasePath pins the launcher contract: a config with a
// relative data_dir must land under BasePath, not the working directory.
// Getting this wrong writes cluster state into the repo.
func TestRelativeDataDirUsesBasePath(t *testing.T) {
	base := t.TempDir()
	cfg := &Config{
		BasePath: base,
		Region:   testRegion,
		RS:       RS{Data: 2, Parity: 1},
		Hosts: []Host{
			{ID: 1, BindAddr: "127.0.0.1:16661", PublicAddr: "127.0.0.1:16661", DataDir: filepath.Join("data", "host-1")},
		},
		Nodes: []NodeConfig{
			{ID: 1, HostID: 1, Role: RoleShardStorage},
			{ID: 2, HostID: 1, Role: RoleShardStorage},
			{ID: 3, HostID: 1, Role: RoleStateReplica},
		},
	}

	node, err := New(Options{Config: cfg, MasterKey: testMasterKey(t)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer node.close()

	if _, err := os.Stat(filepath.Join(base, "data", "host-1", "node-1")); err != nil {
		t.Fatalf("shard store not under base path: %v", err)
	}
	if _, err := os.Stat(filepath.Join("data", "host-1")); err == nil {
		t.Fatal("relative data_dir leaked into the working directory")
	}
}
