package gate

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/iampolicy"
	"github.com/mulgadc/predastore/endpointproxy"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// endpointPKI is a formation CA with the two kinds of leaf it signs: node
// certificates carry clientAuth, guest service certificates do not.
type endpointPKI struct {
	caPath string
	pool   *x509.CertPool
	node   tls.Certificate
	guest  tls.Certificate
}

func newEndpointPKI(t *testing.T) endpointPKI {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "formation-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	leaf := func(serial int64, eku ...x509.ExtKeyUsage) tls.Certificate {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: fmt.Sprintf("leaf-%d", serial)},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: eku,
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		require.NoError(t, err)
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}

	caPath := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600))
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return endpointPKI{
		caPath: caPath, pool: pool,
		node:  leaf(2, x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth),
		guest: leaf(3, x509.ExtKeyUsageServerAuth),
	}
}

func testHeader() endpointproxy.Header {
	return endpointproxy.Header{
		Version:     endpointproxy.ContractVersion1,
		Source:      netip.MustParseAddrPort("10.99.0.5:41288"),
		Destination: netip.MustParseAddrPort("52.95.128.10:443"),
		VpceID:      "vpce-0a1b2c3d4e5f60718",
		VpcID:       "vpc-0123456789abcdef0",
		AccountID:   "000000000001",
	}
}

// endpointServer serves, behind the endpoint listener, a handler that reports
// what each request learned about where it came from.
func endpointServer(t *testing.T, pki endpointPKI) (addr string, inner *tls.Config, l *endpointListener) {
	t.Helper()
	outer, err := endpointTLSConfig(pki.node, pki.caPath)
	require.NoError(t, err)
	inner = &tls.Config{Certificates: []tls.Certificate{pki.node}, MinVersion: tls.VersionTLS13}

	raw, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	l = newEndpointListener(raw, outer)
	srv := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		ConnContext:       endpointConnContext,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h, ok := endpointFrom(r.Context())
			_, _ = fmt.Fprintf(w, "%s %t %s", r.RemoteAddr, ok, h.VpceID)
		}),
	}
	go func() { _ = srv.Serve(tls.NewListener(l, inner)) }()
	t.Cleanup(func() { _ = srv.Close() })
	return raw.Addr().String(), inner, l
}

// dialEndpoint opens the node-to-node session as a proxy would: outer TLS with
// clientCert, then header, and returns the stream the guest's TLS rides on.
func dialEndpoint(t *testing.T, addr string, pki endpointPKI, clientCert *tls.Certificate, header []byte) (*tls.Conn, error) {
	t.Helper()
	cfg := &tls.Config{RootCAs: pki.pool, MinVersion: tls.VersionTLS13, ServerName: "127.0.0.1"}
	if clientCert != nil {
		cfg.Certificates = []tls.Certificate{*clientCert}
	}
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, cfg)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(header); err != nil {
		return nil, err
	}
	return conn, nil
}

// guestGet sends one HTTPS request over the session, as the guest's own TLS.
func guestGet(stream net.Conn, pki endpointPKI) (string, error) {
	guest := tls.Client(stream, &tls.Config{RootCAs: pki.pool, MinVersion: tls.VersionTLS13, ServerName: "127.0.0.1"})
	req, _ := http.NewRequest(http.MethodGet, "https://127.0.0.1/bucket/key", nil)
	if err := req.Write(guest); err != nil {
		return "", err
	}
	resp, err := http.ReadResponse(bufio.NewReader(guest), req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var body [256]byte
	n, _ := resp.Body.Read(body[:])
	return string(body[:n]), nil
}

// A request through the endpoint is addressed as the guest the header names,
// and carries the endpoint's identity to the authorizer.
func TestEndpointListener_RequestCarriesTheGuestAndEndpoint(t *testing.T) {
	pki := newEndpointPKI(t)
	addr, _, _ := endpointServer(t, pki)
	header, err := testHeader().Marshal()
	require.NoError(t, err)

	stream, err := dialEndpoint(t, addr, pki, &pki.node, header)
	require.NoError(t, err)
	got, err := guestGet(stream, pki)
	require.NoError(t, err)
	assert.Equal(t, "10.99.0.5:41288 true vpce-0a1b2c3d4e5f60718", got)
}

// Only a node certificate opens a session: none at all, and a guest service
// certificate signed by the same CA, are both refused before any header.
func TestEndpointListener_RefusesPeersWithoutANodeCertificate(t *testing.T) {
	pki := newEndpointPKI(t)
	addr, _, _ := endpointServer(t, pki)
	header, err := testHeader().Marshal()
	require.NoError(t, err)

	for name, cert := range map[string]*tls.Certificate{"no certificate": nil, "guest certificate": &pki.guest} {
		t.Run(name, func(t *testing.T) {
			stream, err := dialEndpoint(t, addr, pki, cert, header)
			if err == nil {
				_, err = guestGet(stream, pki)
			}
			require.Error(t, err, "a session without a node certificate must not reach the routes")
		})
	}
}

// A header of a contract version this gate does not speak closes the session
// rather than serving the guest under a misread identity.
func TestEndpointListener_RefusesAnUnknownContractVersion(t *testing.T) {
	pki := newEndpointPKI(t)
	addr, _, _ := endpointServer(t, pki)
	header, err := testHeader().Marshal()
	require.NoError(t, err)
	// The version TLV is the first after the 28-byte IPv4 PROXY v2 prefix.
	require.Equal(t, byte(endpointproxy.TypeVersion), header[28])
	header[31] = 2

	stream, err := dialEndpoint(t, addr, pki, &pki.node, header)
	require.NoError(t, err)
	_, err = guestGet(stream, pki)
	require.Error(t, err)
}

// Close abandons a peer stalled in its handshake instead of waiting out the
// handshake timeout, so the gate shuts down on time.
func TestEndpointListener_CloseDoesNotWaitForAStalledPeer(t *testing.T) {
	pki := newEndpointPKI(t)
	outer, err := endpointTLSConfig(pki.node, pki.caPath)
	require.NoError(t, err)
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	l := newEndpointListener(raw, outer)

	stalled, err := net.Dial("tcp", raw.Addr().String())
	require.NoError(t, err)
	defer stalled.Close()
	require.Eventually(t, func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return len(l.pending) == 1
	}, 5*time.Second, 10*time.Millisecond)

	closed := make(chan error, 1)
	go func() { closed <- l.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(endpointHandshakeTimeout / 2):
		t.Fatal("Close waited on a peer stalled in its handshake")
	}
	_, err = l.Accept()
	require.ErrorIs(t, err, net.ErrClosed)
}

func TestEndpointTLSConfig_RefusesAnUnusableCA(t *testing.T) {
	pki := newEndpointPKI(t)
	_, err := endpointTLSConfig(pki.node, filepath.Join(t.TempDir(), "absent.pem"))
	require.Error(t, err)

	empty := filepath.Join(t.TempDir(), "empty.pem")
	require.NoError(t, os.WriteFile(empty, []byte("not a certificate"), 0o600))
	_, err = endpointTLSConfig(pki.node, empty)
	require.ErrorContains(t, err, "holds no certificate")
}

// endpointRequest is a request as the endpoint listener hands it to the routes.
func endpointRequest() *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/reports/q3.csv", nil)
	h := testHeader()
	r.RemoteAddr = h.Source.String()
	return r.WithContext(context.WithValue(r.Context(), endpointCtxKey{}, h))
}

// Through an endpoint the guest's private address is aws:VpcSourceIp, and
// aws:SourceIp is absent: neither the guest's nor the node's address fills it.
func TestConditionKeys_ThroughAnEndpoint(t *testing.T) {
	keys := conditionKeys(endpointRequest(), "s3:GetObject", &auth.CredentialResult{})

	assert.Equal(t, "vpce-0a1b2c3d4e5f60718", keys[iampolicy.KeySourceVpce])
	assert.Equal(t, "vpc-0123456789abcdef0", keys[iampolicy.KeySourceVpc])
	assert.Equal(t, "10.99.0.5", keys[iampolicy.KeyVpcSourceIP])
	_, present := keys[iampolicy.KeySourceIP]
	assert.False(t, present, "aws:SourceIp must be absent through an endpoint")
}

func policy(t *testing.T, js string) []iampolicy.PolicyDocument {
	t.Helper()
	var d iampolicy.PolicyDocument
	require.NoError(t, json.Unmarshal([]byte(js), &d))
	return []iampolicy.PolicyDocument{d}
}

// The policies customers write about endpoints decide as on AWS: an allow on a
// public address range does not admit endpoint traffic, a NotIpAddress deny on
// aws:SourceIp catches it, and aws:SourceVpce scopes it.
func TestConditionKeys_EndpointPoliciesDecideAsOnAWS(t *testing.T) {
	decide := func(js string) iampolicy.Decision {
		r := endpointRequest()
		return iampolicy.EvaluateWithKeys("s3:GetObject", "arn:aws:s3:::reports/q3.csv",
			policy(t, js), conditionKeys(r, "s3:GetObject", &auth.CredentialResult{}))
	}

	assert.NotEqual(t, iampolicy.Allow, decide(`{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":"s3:GetObject","Resource":"*",
		 "Condition":{"IpAddress":{"aws:SourceIp":["0.0.0.0/0"]}}}]}`),
		"an allow on aws:SourceIp must not match a request that carries none")

	assert.Equal(t, iampolicy.Deny, decide(`{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":"s3:*","Resource":"*"},
		{"Effect":"Deny","Action":"s3:*","Resource":"*",
		 "Condition":{"NotIpAddress":{"aws:SourceIp":["203.0.113.0/24"]}}}]}`),
		"a NotIpAddress deny on aws:SourceIp holds when the key is absent")

	assert.Equal(t, iampolicy.Allow, decide(`{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":"s3:GetObject","Resource":"*",
		 "Condition":{"StringEquals":{"aws:SourceVpce":"vpce-0a1b2c3d4e5f60718"},
		              "IpAddress":{"aws:VpcSourceIp":"10.99.0.0/16"}}}]}`))

	assert.NotEqual(t, iampolicy.Allow, decide(`{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":"s3:GetObject","Resource":"*",
		 "Condition":{"StringEquals":{"aws:SourceVpce":"vpce-00000000000000000"}}}]}`))
}
