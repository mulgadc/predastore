package s3db

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/internal/tlsconfig"
)

// streamLayerRootCAs is the package-level root CA pool consulted by the Raft
// dialer. Nil ⇒ OS trust store, which is the production path (cluster CA
// installed via update-ca-certificates / update-ca-trust). Tests inject an
// ephemeral CA via setStreamLayerRootCAs before any dial happens. Mirrors
// quicclient.SetDefaultRootCAs.
var streamLayerRootCAs atomic.Pointer[x509.CertPool]

// setStreamLayerRootCAs replaces the root CA pool consulted by the Raft TLS
// dialer. Pass nil to restore the OS trust store. Production code never calls
// this — it exists so tests that exercise the strict-verify code path can
// trust an ephemeral CA.
func setStreamLayerRootCAs(pool *x509.CertPool) {
	streamLayerRootCAs.Store(pool)
}

// tlsStreamLayer implements raft.StreamLayer over a TLS-wrapped TCP listener.
// Server side: tls.NewListener wraps the bind listener with the cluster cert.
// Client side: tls.DialWithDialer with strict-verify against the OS trust
// store (or the package-level test pool).
type tlsStreamLayer struct {
	advertise net.Addr
	listener  net.Listener
}

var _ raft.StreamLayer = (*tlsStreamLayer)(nil)

// newTLSStreamLayer wraps the given bind listener with a TLS server config
// loaded from certPath/keyPath. The advertise address is what other peers are
// told to dial. The caller is responsible for closing the returned layer
// (which closes the wrapped listener).
func newTLSStreamLayer(listener net.Listener, advertise net.Addr, certPath, keyPath string) (*tlsStreamLayer, error) {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load raft tls cert/key (%s, %s): %w", certPath, keyPath, err)
	}
	serverCfg := &tls.Config{
		Certificates:     []tls.Certificate{cert},
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: tlsconfig.Curves,
	}
	return &tlsStreamLayer{
		advertise: advertise,
		listener:  tls.NewListener(listener, serverCfg),
	}, nil
}

// Dial implements raft.StreamLayer. tls.DialWithDialer derives ServerName
// from the host portion of address when the config leaves it empty; for IP
// hosts (the common case for cluster.toml advertise addresses) verification
// falls back to IP SAN matching.
func (s *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	clientCfg := &tls.Config{
		RootCAs:          streamLayerRootCAs.Load(),
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: tlsconfig.Curves,
	}
	return tls.DialWithDialer(dialer, "tcp", string(address), clientCfg)
}

func (s *tlsStreamLayer) Accept() (net.Conn, error) {
	return s.listener.Accept()
}

func (s *tlsStreamLayer) Close() error {
	return s.listener.Close()
}

// Addr returns the advertise address so raft hands peers the externally
// reachable endpoint rather than the bind address (which may be 0.0.0.0).
func (s *tlsStreamLayer) Addr() net.Addr {
	if s.advertise != nil {
		return s.advertise
	}
	return s.listener.Addr()
}
