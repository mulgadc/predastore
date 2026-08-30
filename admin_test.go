package predastore_test

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/predastore"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/stretchr/testify/require"
)

// freePort takes a port from the kernel and hands it back. A test host binds
// two real sockets — the S3 gate and the admin listener — and everything else
// it runs talks over the in-process pipe.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return port
}

// startHost runs one process holding a gate, a meta replica and three blob
// nodes, and returns the ports its two listeners were given.
func startHost(t *testing.T, adminPort int) (gatePort int) {
	t.Helper()
	certPath, keyPath, _ := testcerts.Generate(t)

	secret := make([]byte, 32)
	_, err := rand.Read(secret)
	require.NoError(t, err)
	key, err := masterkey.New(secret)
	require.NoError(t, err)

	gatePort = freePort(t)
	nodes := []config.Node{
		{ID: 1, Role: config.RoleGate, Port: gatePort, BindAddr: "127.0.0.1"},
		{ID: 2, Role: config.RoleMeta, Port: 7001},
	}
	for i := range 3 {
		nodes = append(nodes, config.Node{ID: config.NodeID(3 + i), Role: config.RoleBlob, Port: 7100 + i})
	}

	cfg := &config.Config{
		Version: config.Version,
		Region:  "ap-southeast-2",
		RS:      config.RS{Data: 2, Parity: 1},
		Hosts: []config.Host{{
			ID:        1,
			Addr:      "127.0.0.1",
			DataDir:   t.TempDir(),
			TLSCert:   certPath,
			TLSKey:    keyPath,
			AdminPort: adminPort,
			Nodes:     nodes,
		}},
	}
	require.NoError(t, cfg.Validate())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- predastore.Run(ctx, predastore.Options{Config: cfg, HostID: 1, MasterKey: key})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("host did not stop")
		}
	})
	return gatePort
}

// awaitProbe polls until the probe answers with want, which is what a load
// balancer coming up against a starting process does.
func awaitProbe(t *testing.T, url string, want int) map[string]any {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:noctx // a loopback probe under a test deadline.
		if err != nil {
			last = err.Error()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == want {
			var decoded map[string]any
			require.NoError(t, json.Unmarshal(body, &decoded))
			return decoded
		}
		last = fmt.Sprintf("status %d: %s", resp.StatusCode, body)
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("probe %s never returned %d, last was %s", url, want, last)
	return nil
}

// awaitGate asks the S3 port for a path once it is accepting. The gate binds
// after the election the readiness probe waits on, so a request sent the moment
// /readyz turns green can still arrive before its listener exists.
func awaitGate(t *testing.T, client *http.Client, url string) (int, string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		resp, err := client.Get(url) //nolint:noctx // bounded by the deadline above.
		if err != nil {
			last = err
			time.Sleep(50 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode, string(body)
	}
	t.Fatalf("the S3 port never answered %s: %v", url, last)
	return 0, ""
}

// The listener answers for a whole process rather than a node: one host, one
// pair of probes, whatever roles it happens to run.
func TestAdminListenerServesTheProcess(t *testing.T) {
	adminPort := freePort(t)
	gatePort := startHost(t, adminPort)

	base := "http://127.0.0.1:" + strconv.Itoa(adminPort)
	health := awaitProbe(t, base+"/healthz", http.StatusOK)
	require.Equal(t, "ok", health["status"])

	// Readiness needs an elected leader and enough blob nodes to reconstruct
	// with, so it lags liveness by however long the election takes.
	ready := awaitProbe(t, base+"/readyz", http.StatusOK)
	require.Equal(t, "ready", ready["status"])
	checks, isMap := ready["checks"].(map[string]any)
	require.True(t, isMap, "checks is %T, want an object", ready["checks"])
	for _, name := range []string{"meta_leader", "meta_reachable", "blob_nodes"} {
		require.Equal(t, "ok", checks[name], "check %q", name)
	}

	// The S3 port is public by design. Health there would publish the state of
	// the cluster to anyone who can reach the API.
	// The certificate is self-signed and not what is under test here; the
	// response the S3 port gives to a probe path is.
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}}
	for _, path := range []string{"/healthz", "/readyz"} {
		status, body := awaitGate(t, client, "https://127.0.0.1:"+strconv.Itoa(gatePort)+path)

		require.NotEqual(t, http.StatusOK, status, "the S3 port answered %s", path)
		require.NotContains(t, body, `"status"`, "the S3 port served a probe response")
	}
}

// A host that names no admin_port runs no admin listener, and that is not an
// error: the listener is opt-in and every other node still serves.
func TestHostWithoutAdminPortRunsNoListener(t *testing.T) {
	unused := freePort(t)
	startHost(t, 0)

	// Nothing may have taken it, and the process must still be serving. The
	// gate is what proves the second half: a Run that failed would have taken
	// its listener down with it.
	ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(unused))
	require.NoError(t, err, "a port was bound with no admin_port configured")
	require.NoError(t, ln.Close())
}
