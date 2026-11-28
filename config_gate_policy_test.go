package predastore_test

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The policy rules ride through the real loader and the real gate wiring: the
// TOML is loaded from disk, the host runs through predastore.Run, and every
// request below is signed against the live TLS listener. A wiring regression
// between Load and the gate cannot pass this.
//
// The gate needs a meta leader for some routes, but auth runs before any of
// them, so a denial is observable without cluster state behind the gate.

// signedSend signs and issues one request at the live gate.
func signedSend(t *testing.T, client *http.Client, base, method, path,
	accessKey, secret, region string, body []byte) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, base+path, bytes.NewReader(body))
	require.NoError(t, err)

	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", hash)

	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	require.NoError(t, signer.SignHTTP(t.Context(),
		aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secret},
		req, hash, "s3", region, time.Now().UTC()))

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, got
}

// TestLoadedConfigPolicyReachesTheGate drives a signed regression from a TOML
// file on disk through the normal gate setup: Load, Validate, Run, and the
// live listener. It covers all three policy forms and isolates action
// enforcement by denying PUT on the same bucket the scoped key can read.
func TestLoadedConfigPolicyReachesTheGate(t *testing.T) {
	certPath, keyPath, _ := testcerts.Generate(t)
	adminPort, gatePort := freePort(t), freePort(t)

	const (
		scoped   = "AKIASCOPED0000000001"
		scopedSk = "scoped-secret-key-000000000000000000000000001"
		root     = "AKIAROOTKEY000000001"
		rootSk   = "root-secret-key-0000000000000000000000000001"
		denied   = "AKIADENIED000000001"
		deniedSk = "denied-secret-key-00000000000000000000000001"
	)

	tomlText := `version = 1
region = "ap-southeast-2"

[rs]
data = 1
parity = 0

[[host]]
id = 1
addr = "127.0.0.1"
data_dir = "` + filepath.ToSlash(t.TempDir()) + `"
tls_cert = "` + filepath.ToSlash(certPath) + `"
tls_key = "` + filepath.ToSlash(keyPath) + `"
admin_port = ` + strconv.Itoa(adminPort) + `

[[host.node]]
id = 1
role = "gate"
port = ` + strconv.Itoa(gatePort) + `
bind_addr = "127.0.0.1"

[[host.node]]
id = 2
role = "meta"
port = 7201

[[host.node]]
id = 3
role = "blob"
port = 7301

[[bucket]]
name = "docs"
region = "ap-southeast-2"
account_id = "123456789012"

# A scoped service key: read-only on one bucket.
[[auth]]
access_key_id = "` + scoped + `"
secret_access_key = "` + scopedSk + `"
account_id = "123456789012"
policy = [
  { bucket = "docs", actions = ["s3:GetObject"] },
]

# The unrestricted form: no policy list at all.
[[auth]]
access_key_id = "` + root + `"
secret_access_key = "` + rootSk + `"
account_id = "123456789012"

# The deny-all form: an explicitly empty policy list.
[[auth]]
access_key_id = "` + denied + `"
secret_access_key = "` + deniedSk + `"
account_id = "123456789012"
policy = []
`
	path := filepath.Join(t.TempDir(), "predastore.toml")
	require.NoError(t, os.WriteFile(path, []byte(tomlText), 0o600))

	cfg, err := config.Load(path)
	require.NoError(t, err)
	runHost(t, cfg, newMasterKey(t))

	awaitProbe(t, "http://127.0.0.1:"+strconv.Itoa(adminPort)+"/readyz", http.StatusOK)
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}}
	base := "https://127.0.0.1:" + strconv.Itoa(gatePort)
	awaitGate(t, client, base+"/")

	// The scoped key reads its bucket: auth passes, the object is simply not
	// there. Anything but a denial means the policy admitted the action.
	status, _ := signedSend(t, client, base, http.MethodGet, "/docs/missing",
		scoped, scopedSk, cfg.Region, nil)
	assert.NotEqual(t, http.StatusForbidden, status,
		"the scoped key must be admitted for the action its policy grants")

	// The same key, the same bucket, an action the policy does not grant: the
	// denial isolates action enforcement from bucket scoping.
	status, body := signedSend(t, client, base, http.MethodPut, "/docs/new-object",
		scoped, scopedSk, cfg.Region, []byte("data"))
	assert.Equal(t, http.StatusForbidden, status, "PUT must be denied for a read-only policy")
	assert.Contains(t, string(body), "AccessDenied")

	// The policy-less key is the unrestricted form: its PUT goes through auth.
	status, _ = signedSend(t, client, base, http.MethodPut, "/docs/root-object",
		root, rootSk, cfg.Region, []byte("data"))
	assert.NotEqual(t, http.StatusForbidden, status,
		"a key without a policy must not be confined by the scoped key's rules")

	// The explicitly empty policy denies everything, on the bucket it could
	// otherwise reach.
	status, body = signedSend(t, client, base, http.MethodGet, "/docs/missing",
		denied, deniedSk, cfg.Region, nil)
	assert.Equal(t, http.StatusForbidden, status, "an explicit empty policy denies everything")
	assert.Contains(t, string(body), "AccessDenied")
}
