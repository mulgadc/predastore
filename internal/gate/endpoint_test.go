package gate

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"encoding/xml"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/iampolicy"
	"github.com/mulgadc/predastore/endpointproxy"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/model"
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

// withEndpoint is ctx as the endpoint listener hands it to the routes, with the
// endpoint's policy already parsed; a nil policy is a version 1 connection.
func withEndpoint(ctx context.Context, h endpointproxy.Header, policy *iampolicy.PolicyDocument) context.Context {
	return context.WithValue(ctx, endpointCtxKey{}, endpointIdentity{Header: h, policy: policy})
}

// endpointServer serves, behind the endpoint listener, a handler that reports
// what each request learned about where it came from.
func endpointServer(t *testing.T, pki endpointPKI) (addr string, inner *tls.Config, l *endpointListener) {
	t.Helper()
	return endpointServerFor(t, pki, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, ok := endpointFrom(r.Context())
		_, _ = fmt.Fprintf(w, "%s %t %s", r.RemoteAddr, ok, h.VpceID)
	}))
}

// endpointServerFor serves handler behind the endpoint listener.
func endpointServerFor(t *testing.T, pki endpointPKI, handler http.Handler) (addr string, inner *tls.Config, l *endpointListener) {
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
		Handler:           handler,
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
// guestTLS is the guest's own TLS session, riding the endpoint stream.
func guestTLS(stream net.Conn, pki endpointPKI) *tls.Conn {
	return tls.Client(stream, &tls.Config{RootCAs: pki.pool, MinVersion: tls.VersionTLS13, ServerName: "127.0.0.1"})
}

func guestGet(stream net.Conn, pki endpointPKI) (string, error) {
	guest := guestTLS(stream, pki)
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
	header[31] = 3

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
	return r.WithContext(withEndpoint(r.Context(), h, nil))
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

// fakeInner is a listener whose connections the test hands over one at a time,
// so the order of Accept and Close is the test's to choose.
type fakeInner struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func (f *fakeInner) Accept() (net.Conn, error) {
	c, ok := <-f.conns
	if !ok {
		return nil, net.ErrClosed
	}
	return c, nil
}

// Close leaves Accept blocked, as a connection already in the kernel's queue
// is still returned after the listening socket starts closing.
func (f *fakeInner) Close() error   { f.once.Do(func() { close(f.closed) }); return nil }
func (f *fakeInner) Addr() net.Addr { return &net.TCPAddr{} }

// A connection accepted after Close has swept the pending set is closed at once,
// not left holding a handshake until its deadline.
func TestEndpointListener_ConnectionAcceptedDuringCloseIsNotAdmitted(t *testing.T) {
	pki := newEndpointPKI(t)
	outer, err := endpointTLSConfig(pki.node, pki.caPath)
	require.NoError(t, err)
	inner := &fakeInner{conns: make(chan net.Conn), closed: make(chan struct{})}
	l := newEndpointListener(inner, outer)

	closed := make(chan error, 1)
	go func() { closed <- l.Close() }()
	require.Eventually(t, func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.closed
	}, 5*time.Second, time.Millisecond)

	server, peer := net.Pipe()
	defer peer.Close()
	inner.conns <- server
	close(inner.conns)

	_ = peer.SetReadDeadline(time.Now().Add(endpointHandshakeTimeout / 2))
	_, err = peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "the late connection must be closed, not left to its handshake deadline")
	require.NoError(t, <-closed)
}

// A connection already handed to the HTTP server belongs to its drain: closing
// the listener must not cut it.
func TestEndpointListener_CloseLeavesAnAdmittedConnectionOpen(t *testing.T) {
	pki := newEndpointPKI(t)
	outer, err := endpointTLSConfig(pki.node, pki.caPath)
	require.NoError(t, err)
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	l := newEndpointListener(raw, outer)

	header, err := testHeader().Marshal()
	require.NoError(t, err)
	client, err := dialEndpoint(t, raw.Addr().String(), pki, &pki.node, header)
	require.NoError(t, err)
	admitted, err := l.Accept()
	require.NoError(t, err)
	defer admitted.Close()

	require.NoError(t, l.Close())
	_, err = admitted.Write([]byte("still open"))
	require.NoError(t, err)
	got := make([]byte, len("still open"))
	_, err = io.ReadFull(client, got)
	require.NoError(t, err)
	assert.Equal(t, "still open", string(got))
}

// endpointDecision sends a signed ListBucket through the whole server, through
// an endpoint or not, as a principal holding policy, and returns the status.
func endpointDecision(t *testing.T, viaEndpoint bool, statements ...iampolicy.Statement) int {
	t.Helper()
	server := newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta: newFakeMeta(t,
			model.BucketMetadata{Name: "owner-bucket", Region: "ap-southeast-2", AccountID: acctOwner, OwnerID: keyCreator}),
		CredProv: &stubCredProvider{creds: map[string]*auth.CredentialResult{
			keyOwner: {SecretAccessKey: secret, AccountID: acctOwner, PolicyDocuments: []iampolicy.PolicyDocument{{
				Version: "2012-10-17", Statement: statements,
			}}},
		}},
	})
	req := httptest.NewRequest(http.MethodGet, "/owner-bucket", nil)
	if viaEndpoint {
		h := testHeader()
		req.RemoteAddr = h.Source.String()
		req = req.WithContext(withEndpoint(req.Context(), h, nil))
	} else {
		req.RemoteAddr = "192.0.2.10:41288"
	}
	signTestReq(t, req, nil, keyOwner, secret, "ap-southeast-2", "s3")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	require.Contains(t, []int{http.StatusOK, http.StatusForbidden}, rr.Code, rr.Body.String())
	return rr.Code
}

func onCondition(effect, op, key string, values ...string) iampolicy.Statement {
	cv := iampolicy.ConditionValue(values)
	return iampolicy.Statement{
		Effect: effect, Action: iampolicy.StringOrArr{"s3:*"}, Resource: iampolicy.StringOrArr{"*"},
		Condition: map[string]map[string]iampolicy.ConditionValue{op: {key: cv}},
	}
}

// The policies customers write about endpoints decide as on AWS through the real
// authorizer, and the same policy on the ordinary path decides as before.
// The ordinary request comes from 192.0.2.10; the guest is 10.99.0.5.
func TestEndpointPolicies_DecideAsOnAWS(t *testing.T) {
	allowAll := allowAllPolicy.Statement[0]
	for name, tc := range map[string]struct {
		statements         []iampolicy.Statement
		ordinary, endpoint int
	}{
		"allow on aws:SourceIp does not match endpoint traffic": {
			[]iampolicy.Statement{onCondition("Allow", "IpAddress", "aws:SourceIp", "192.0.2.0/24")},
			http.StatusOK, http.StatusForbidden,
		},
		"NotIpAddress deny on aws:SourceIp holds with the key absent": {
			[]iampolicy.Statement{allowAll, onCondition("Deny", "NotIpAddress", "aws:SourceIp", "192.0.2.0/24")},
			http.StatusOK, http.StatusForbidden,
		},
		"IpAddress deny on the guest CIDR in aws:SourceIp does not match": {
			[]iampolicy.Statement{allowAll, onCondition("Deny", "IpAddress", "aws:SourceIp", "10.99.0.0/16")},
			http.StatusOK, http.StatusOK,
		},
		"aws:VpcSourceIp deny on the guest CIDR matches only endpoint traffic": {
			[]iampolicy.Statement{allowAll, onCondition("Deny", "IpAddress", "aws:VpcSourceIp", "10.99.0.0/16")},
			http.StatusOK, http.StatusForbidden,
		},
		"allow scoped to this endpoint": {
			[]iampolicy.Statement{onCondition("Allow", "StringEquals", "aws:SourceVpce", "vpce-0a1b2c3d4e5f60718")},
			http.StatusForbidden, http.StatusOK,
		},
		"allow scoped to another endpoint": {
			[]iampolicy.Statement{onCondition("Allow", "StringEquals", "aws:SourceVpce", "vpce-00000000000000000")},
			http.StatusForbidden, http.StatusForbidden,
		},
		"allow scoped to this VPC": {
			[]iampolicy.Statement{onCondition("Allow", "StringEquals", "aws:SourceVpc", "vpc-0123456789abcdef0")},
			http.StatusForbidden, http.StatusOK,
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.ordinary, endpointDecision(t, false, tc.statements...), "ordinary path")
			assert.Equal(t, tc.endpoint, endpointDecision(t, true, tc.statements...), "through the endpoint")
		})
	}
}

// The per-object authorizer of a batch delete sees the endpoint's keys too, so
// an endpoint-scoped deny fences keys inside a batch as it does a single call.
func TestEndpointPolicies_ReachThePerObjectAuthorizer(t *testing.T) {
	server := newTestGate(t, Config{
		Region: "ap-southeast-2",
		Meta: newFakeMeta(t,
			model.BucketMetadata{Name: "owner-bucket", Region: "ap-southeast-2", AccountID: acctOwner, OwnerID: keyCreator}),
		CredProv: &stubCredProvider{creds: map[string]*auth.CredentialResult{
			keyOwner: {SecretAccessKey: secret, AccountID: acctOwner, PolicyDocuments: []iampolicy.PolicyDocument{{
				Version: "2012-10-17",
				Statement: []iampolicy.Statement{allowAllPolicy.Statement[0], {
					Effect: "Deny", Action: iampolicy.StringOrArr{"s3:DeleteObject"},
					Resource: iampolicy.StringOrArr{"arn:aws:s3:::owner-bucket/secret/*"},
					Condition: map[string]map[string]iampolicy.ConditionValue{
						"StringEquals": {"aws:SourceVpce": {"vpce-0a1b2c3d4e5f60718"}},
					},
				}},
			}}},
		}},
	})
	body := []byte("<Delete><Object><Key>public/a</Key></Object><Object><Key>secret/b</Key></Object></Delete>")
	req := httptest.NewRequest(http.MethodPost, "/owner-bucket?delete=", bytes.NewReader(body))
	h := testHeader()
	req.RemoteAddr = h.Source.String()
	req = req.WithContext(withEndpoint(req.Context(), h, nil))
	signTestReq(t, req, body, keyOwner, secret, "ap-southeast-2", "s3")
	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var result handlers.DeleteResult
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &result))
	require.Len(t, result.Errors, 1)
	assert.Equal(t, "secret/b", result.Errors[0].Key)
	assert.Equal(t, string(model.ErrAccessDenied), result.Errors[0].Code)
	require.Len(t, result.Deleted, 1)
	assert.Equal(t, "public/a", result.Deleted[0].Key)
}
