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

// streamLayerRootCAs is the root CA pool consulted by the Raft dialer.
// Nil ⇒ OS trust store (production path). Tests inject an ephemeral CA via
// setStreamLayerRootCAs.
var streamLayerRootCAs atomic.Pointer[x509.CertPool]

// setStreamLayerRootCAs is a test hook. Production code never calls this.
func setStreamLayerRootCAs(pool *x509.CertPool) {
	streamLayerRootCAs.Store(pool)
}

type tlsStreamLayer struct {
	advertise net.Addr
	listener  net.Listener
}

var _ raft.StreamLayer = (*tlsStreamLayer)(nil)

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

// Dial leaves ServerName empty so tls.DialWithDialer derives it from the
// address host — IP SAN matching for IP-literal advertise addresses.
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

// Addr returns the advertise address — bind may be 0.0.0.0, peers need a
// reachable endpoint.
func (s *tlsStreamLayer) Addr() net.Addr {
	if s.advertise != nil {
		return s.advertise
	}
	return s.listener.Addr()
}
