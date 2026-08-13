package predastore_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/predastore"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/mulgadc/predastore/internal/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startReplicaOverQUIC runs one meta replica reachable over quic on
// loopback, presenting certPath/keyPath, and blocks until it has
// bootstrapped and elected itself leader. It returns the one-node cluster
// config the replica answers as.
func startReplicaOverQUIC(t *testing.T, certPath, keyPath string) *predastore.Config {
	t.Helper()

	quic, err := transport.NewQUICTransport("127.0.0.1", 0, certPath, keyPath)
	require.NoError(t, err)
	t.Cleanup(func() { quic.Close() })
	ln, err := quic.Listen()
	require.NoError(t, err)

	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)

	cfg := &config.Config{
		Hosts: []config.Host{{
			ID:      1,
			Addr:    "127.0.0.1",
			TLSCert: certPath,
			TLSKey:  keyPath,
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleMeta, Port: port},
			},
		}},
	}

	res, err := rpc.NewResolver(cfg, 1, quic)
	require.NoError(t, err)

	leader := make(chan struct{})
	svc, err := meta.New(meta.Config{
		NodeID:    1,
		DataDir:   t.TempDir(),
		Peers:     []config.NodeID{1},
		Bootstrap: true,
		Listeners: []transport.Listener{ln},
		Resolver:  res,
		OnLeader:  sync.OnceFunc(func() { close(leader) }),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	select {
	case <-leader:
	case <-time.After(5 * time.Second):
		t.Fatal("replica never elected a leader")
	}

	return cfg
}

// startStatusCluster is startReplicaOverQUIC with a self-signed certificate,
// returning the pool that trusts exactly it alongside the cluster config.
func startStatusCluster(t *testing.T) (*predastore.Config, *x509.CertPool) {
	t.Helper()
	certPath, keyPath, pool := testcerts.Generate(t)
	return startReplicaOverQUIC(t, certPath, keyPath), pool
}

// TestNodeStatus_EndToEnd exercises the whole re-exported surface a caller
// outside this module has: build a Config, hand it, a node id and a trusted
// CA pool to NodeStatus, get back the raft status that Config's own node
// answers with — with no other predastore import required.
func TestNodeStatus_EndToEnd(t *testing.T) {
	cfg, pool := startStatusCluster(t)

	require.Equal(t, []predastore.NodeID{1}, predastore.MetaNodesOnHost(cfg, 1))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	st, err := predastore.NodeStatus(ctx, cfg, 1, pool)
	require.NoError(t, err)
	assert.Equal(t, "1", st.NodeID)
	assert.Equal(t, "Leader", st.State)
	assert.True(t, st.IsLeader)
	assert.NotEmpty(t, st.Leader)
	assert.NotEmpty(t, st.Term)
}

// TestNodeStatus_UnknownNode confirms a node id absent from the
// configuration is a plain error rather than a nil-pointer surprise: this is
// the mistake a caller passing the wrong id makes, not the cluster's, so it
// should read as one.
func TestNodeStatus_UnknownNode(t *testing.T) {
	cfg, pool := startStatusCluster(t)

	_, err := predastore.NodeStatus(context.Background(), cfg, 99, pool)
	require.Error(t, err)
}

// TestMetaNodesOnHost_NoHost confirms an id naming no host is nil rather
// than an error: a host running no meta node of its own is a normal
// topology, not a mistake.
func TestMetaNodesOnHost_NoHost(t *testing.T) {
	cfg, _ := startStatusCluster(t)
	assert.Nil(t, predastore.MetaNodesOnHost(cfg, 99))
}

// TestNodeStatus_CrossHostCertificate is the shape a real deployment is:
// every [[host]] names the same tls_cert/tls_key path, but each host's file
// there holds a different certificate. The caller here holds neither the
// target's certificate nor its key — only the CA that issued it — and the
// target's config entry points at paths that do not exist on this machine
// at all, so the test fails outright if NodeStatus ever tries to read them.
func TestNodeStatus_CrossHostCertificate(t *testing.T) {
	ca, caKey := genTestCA(t)
	caPool := x509.NewCertPool()
	caPool.AddCert(ca)

	// A leaf distinct from anything the caller holds, signed by the shared
	// CA rather than self-signed.
	remoteCert, remoteKey := genTestLeaf(t, ca, caKey, "remote-meta-node")
	cfg := startReplicaOverQUIC(t, remoteCert, remoteKey)

	// Point the host's config entry at paths this process cannot read, to
	// prove NodeStatus never touches them for a node it did not start.
	cfg.Hosts[0].TLSCert = "/nonexistent/remote-host/server.pem"
	cfg.Hosts[0].TLSKey = "/nonexistent/remote-host/server.key"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	st, err := predastore.NodeStatus(ctx, cfg, 1, caPool)
	require.NoError(t, err)
	assert.True(t, st.IsLeader)
}

// genTestCA creates a self-signed CA cert/key pair for issuing leaf
// certificates from, distinct from testcerts.Generate's single self-signed
// leaf: a status client must verify a node's cert through a CA it did not
// itself present.
func genTestCA(t *testing.T) (*x509.Certificate, *rsa.PrivateKey) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "predastore-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert, key
}

// genTestLeaf issues a certificate for name signed by ca/caKey, writes the
// cert and key as PEM files under t.TempDir(), and returns their paths.
func genTestLeaf(t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey, name string) (certPath, keyPath string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	require.NoError(t, err)

	dir := t.TempDir()
	certPath = filepath.Join(dir, "leaf.pem")
	keyPath = filepath.Join(dir, "leaf.key")
	require.NoError(t, os.WriteFile(certPath,
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath,
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600))
	return certPath, keyPath
}
