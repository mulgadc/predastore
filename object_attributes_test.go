package predastore_test

import (
	"bytes"
	"context"
	"crypto/rand"
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
	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/predastore"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/stretchr/testify/require"
)

const (
	attrsAccessKey = "AKIAIOSFODNN7EXAMPLE"
	attrsSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	attrsRegion    = "ap-southeast-2"
	attrsBucket    = "attributes"
)

// startAuthedHost runs one process with a gate, a meta replica and three blob
// nodes, one service account and one bucket it owns, and returns the gate's
// URL and a client that trusts its certificate.
func startAuthedHost(t *testing.T) (string, *http.Client) {
	t.Helper()
	certPath, keyPath, pool := testcerts.Generate(t)

	secret := make([]byte, 32)
	_, err := rand.Read(secret)
	require.NoError(t, err)
	key, err := masterkey.New(secret)
	require.NoError(t, err)

	gatePort, adminPort := freePort(t), freePort(t)
	nodes := []config.Node{
		{ID: 1, Role: config.RoleGate, Port: gatePort, BindAddr: "127.0.0.1"},
		{ID: 2, Role: config.RoleMeta, Port: 7001},
	}
	for i := range 3 {
		nodes = append(nodes, config.Node{ID: config.NodeID(3 + i), Role: config.RoleBlob, Port: 7100 + i})
	}
	const account = "123456789012"
	cfg := &config.Config{
		Version: config.Version,
		Region:  attrsRegion,
		RS:      config.RS{Data: 2, Parity: 1},
		Hosts: []config.Host{{
			ID: 1, Addr: "127.0.0.1", DataDir: t.TempDir(),
			TLSCert: certPath, TLSKey: keyPath, AdminPort: adminPort, Nodes: nodes,
		}},
		Buckets: []config.Bucket{{Name: attrsBucket, Region: attrsRegion, AccountID: account}},
		Auth: []config.AuthEntry{{
			AccessKeyID: attrsAccessKey, SecretAccessKey: attrsSecretKey, AccountID: account,
			Policy: []config.PolicyRule{{Bucket: "*", Actions: []string{"s3:*"}}},
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

	awaitProbe(t, "http://127.0.0.1:"+strconv.Itoa(adminPort)+"/readyz", http.StatusOK)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}}
	return "https://127.0.0.1:" + strconv.Itoa(gatePort), client
}

// signedRequest builds a SigV4-signed request, which is what every S3 client
// sends and what the gate verifies before any handler runs.
func signedRequest(t *testing.T, method, url string, body []byte, headers map[string]string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body)) //nolint:noctx // bounded by the test.
	require.NoError(t, err)
	req.ContentLength = int64(len(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	require.NoError(t, signer.SignHTTP(context.Background(),
		aws.Credentials{AccessKeyID: attrsAccessKey, SecretAccessKey: attrsSecretKey},
		req, payloadHash, "s3", attrsRegion, time.Now().UTC()))
	return req
}

// A signed PUT's Content-Type and x-amz-meta-* headers come back on HEAD and
// GET through the whole stack: listener, SigV4, meta replication and shards.
func TestObjectAttributesSurviveTheWholeStack(t *testing.T) {
	base, client := startAuthedHost(t)
	url := base + "/" + attrsBucket + "/doc.txt"

	// The gate binds after the election /readyz waits on, so the first request
	// can still arrive before its listener exists.
	var put *http.Response
	deadline := time.Now().Add(20 * time.Second)
	for {
		var err error
		put, err = client.Do(signedRequest(t, http.MethodPut, url, []byte("bar"), map[string]string{
			"Content-Type": "audio/ogg", "X-Amz-Meta-Key1": "value1", "X-Amz-Meta-Empty": "",
		}))
		if err == nil || time.Now().After(deadline) {
			require.NoError(t, err)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	body, _ := io.ReadAll(put.Body)
	_ = put.Body.Close()
	require.Equal(t, http.StatusOK, put.StatusCode, string(body))

	for _, method := range []string{http.MethodHead, http.MethodGet} {
		resp, err := client.Do(signedRequest(t, method, url, nil, nil))
		require.NoError(t, err)
		got, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", method, got)

		require.Equal(t, "audio/ogg", resp.Header.Get("Content-Type"), method)
		require.Equal(t, "value1", resp.Header.Get("X-Amz-Meta-Key1"), method)
		require.Equal(t, []string{""}, resp.Header.Values("X-Amz-Meta-Empty"), method)
		if method == http.MethodGet {
			require.Equal(t, "bar", string(got))
		}
	}
}
