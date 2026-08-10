package gate

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/mulgadc/predastore/internal/gate/placement"
	"github.com/mulgadc/predastore/internal/meta"
	"github.com/mulgadc/predastore/internal/tlsconfig"
	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/predastore/pkg/ratelimit"
)

const (
	defaultHost = "0.0.0.0"
	defaultPort = 8443

	// defaultPprofOutput is where a CPU profile lands when none is configured.
	defaultPprofOutput = "/tmp/predastore-cpu.prof"

	// shutdownGrace bounds how long Run waits for in-flight requests to finish
	// after the context is cancelled.
	shutdownGrace = 10 * time.Second
)

// Clients are the cluster connections the gate does its work through. It
// owns neither transport: the process that runs the nodes builds both and
// hands them over.
type Clients struct {
	// Meta reaches the replicas holding bucket, object and upload metadata.
	Meta *meta.Client
	// Blob reaches the nodes holding shards.
	Blob *blob.Client
}

// store returns the metadata store behind the state client. Resolved through
// the nil check so a typed-nil client cannot masquerade as a live one behind
// the interface.
func (c Clients) store() handlers.Meta {
	if c.Meta == nil {
		return nil
	}
	return c.Meta
}

// ServerConfig is the process-supplied half of the gate's configuration:
// everything that arrives by wiring rather than from the configuration file.
// The caller builds it and hands it to NewServer.
type ServerConfig struct {
	// Config is the gate's slice of the product configuration, already
	// parsed and resolved by the caller. Required.
	Config *Config

	// Host and Port are the S3 listen address. Zero values default to
	// 0.0.0.0:8443.
	Host string
	Port int

	// TLSCert and TLSKey are required: the gate only serves HTTPS.
	TLSCert string
	TLSKey  string

	// MasterKey is the AES-256 key protecting data at rest. Required, and
	// never sourced from the configuration file, so no plaintext secret path
	// lives on disk beside the config.
	MasterKey *masterkey.Key

	// Clients are the cluster connections the gate works through. The
	// server only runs the S3 HTTPS frontend: no state or blob nodes are
	// launched, and the caller owns their supporting runtime.
	Clients Clients

	// PprofEnabled writes a CPU profile for the lifetime of Run, saved to
	// PprofOutputPath (default /tmp/predastore-cpu.prof) as Run returns.
	PprofEnabled    bool
	PprofOutputPath string
}

// Server is predastore's S3 gate: the HTTPS listener, the SigV4 + IAM
// middleware chain and the route table. It is the S3 implementation, not a
// front end onto one — the handlers it routes to erasure code, place and
// record their own operations.
type Server struct {
	cfg       ServerConfig
	config    *Config
	masterKey *masterkey.Key // AEAD + fingerprint, no raw bytes.

	router    chi.Router
	credProv  auth.CredentialProvider
	throttler *ratelimit.Throttler

	// Handler dependencies, shared by the route table and the auth middleware.
	handlerCfg handlers.Config
	state      handlers.Meta         // bucket, object and upload metadata
	buckets    *handlers.BucketCache // config-defined buckets, plus those created since startup

	// pprofFile is the temp file the CPU profile streams into while Run is
	// serving; it is copied to the output path on the way out.
	pprofFile *os.File
}

// NewServer builds the S3 gate: it resolves the credential chain and
// assembles the route table over the supplied configuration and clients.
// Nothing listens until Run is called.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Host == "" {
		cfg.Host = defaultHost
	}
	if cfg.Port == 0 {
		cfg.Port = defaultPort
	}
	if cfg.PprofEnabled && cfg.PprofOutputPath == "" {
		cfg.PprofOutputPath = defaultPprofOutput
	}

	if cfg.Config == nil {
		return nil, fmt.Errorf("no configuration provided: set ServerConfig.Config")
	}
	config := cfg.Config

	// The master key is mandatory, and checked before anything expensive so a
	// misconfigured launch fails immediately.
	if cfg.MasterKey == nil {
		return nil, fmt.Errorf("master key is required: set ServerConfig.MasterKey")
	}
	slog.Info("master key loaded", "fingerprint", cfg.MasterKey.Fingerprint)

	// The clients arrive fully wired from the process that owns the cluster
	// nodes; the gate never launches storage or state itself.
	if cfg.Clients.Meta == nil || cfg.Clients.Blob == nil {
		return nil, fmt.Errorf("no cluster clients provided: build the cluster runtime and set ServerConfig.Clients")
	}

	credProv, err := newCredentialProvider(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize credential provider: %w", err)
	}

	s := newGate(config, cfg.Clients.store(), cfg.Clients.Blob, credProv)
	s.cfg = cfg
	s.masterKey = cfg.MasterKey
	slog.Info("HTTP/2 server initialized - using net/http for connection multiplexing")

	return s, nil
}

// NewHandler builds the S3 request handler over the given cluster clients,
// without a listener or a master key. Production goes through NewServer; this
// is the seam tests drive with httptest.
func NewHandler(config *Config, clients Clients, credProv auth.CredentialProvider) http.Handler {
	return newGate(config, clients.store(), clients.Blob, credProv).router
}

// newGate builds the routing half of the server over an arbitrary state
// store. It is the seam tests use to stand a map in for a raft cluster;
// production always arrives here through NewServer with a *meta.Client.
func newGate(config *Config, store handlers.Meta, shards *blob.Client, credProv auth.CredentialProvider) *Server {
	cfg := config.handlerConfig()

	s := &Server{
		config:     config,
		router:     chi.NewRouter(),
		credProv:   credProv,
		handlerCfg: cfg,
		state:      store,
		buckets:    handlers.NewBucketCache(cfg.Buckets),
	}

	if config.RateLimit.Enabled {
		s.throttler = ratelimit.New(config.RateLimit)
	}

	s.setupMiddleware()
	s.setupRoutes(shards, placement.NewRing(config.BlobNodeIDs))
	return s
}

// newCredentialProvider resolves the auth chain: NATS-backed IAM when it is
// configured, with the config-defined accounts always as the fallback.
func newCredentialProvider(config *Config) (auth.CredentialProvider, error) {
	configProv := auth.NewConfigProvider(config.Auth)

	if config.IAM == nil {
		slog.Info("IAM not configured, using config-only auth")
		return configProv, nil
	}

	natsProv, err := auth.NewNATSIAMProvider(config.IAM)
	if err != nil {
		return nil, fmt.Errorf("IAM configured but NATS provider failed to initialize: %w", err)
	}

	slog.Info("Using NATS IAM + config chain auth")
	return auth.NewChainProvider(natsProv, configProv), nil
}

// Run serves S3 over TLS until ctx is cancelled or the listener fails, then
// drains in-flight requests within the grace period and releases everything it
// started. It is the server's whole lifecycle: there is nothing to stop it
// with but the context.
func (s *Server) Run(ctx context.Context) error {
	if s.cfg.TLSCert == "" || s.cfg.TLSKey == "" {
		return errors.New("TLS is required - set ServerConfig.TLSCert and ServerConfig.TLSKey")
	}

	// Teardown runs LIFO, so registration order here is the reverse of the
	// order things are released in: the listener drains first, then the
	// credential provider and the throttler that fronted it, and profiling
	// last so it covers the whole shutdown.
	if s.cfg.PprofEnabled {
		if err := s.startProfiling(); err != nil {
			// A missing profile is not worth refusing to serve over.
			slog.Error("Failed to start CPU profiling", "error", err)
		}
		defer func() {
			if err := s.stopProfiling(); err != nil {
				slog.Error("Error stopping CPU profile", "error", err)
			}
		}()
	}
	if s.throttler != nil {
		defer s.throttler.Stop()
	}
	if s.credProv != nil {
		defer s.credProv.Close()
	}

	cert, err := tls.LoadX509KeyPair(s.cfg.TLSCert, s.cfg.TLSKey)
	if err != nil {
		return fmt.Errorf("failed to load TLS certificate: %w", err)
	}

	// NextProtos advertises HTTP/2 over ALPN, with HTTP/1.1 as the fallback.
	tlsCfg := &tls.Config{
		Certificates:     []tls.Certificate{cert},
		NextProtos:       []string{"h2", "http/1.1"},
		MinVersion:       tls.VersionTLS13,
		CurvePreferences: tlsconfig.Curves,
	}

	addr := net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.cfg.Port))
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           s.router,
		TLSConfig:         tlsCfg,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}

	// Buffered so the send never blocks, and closed after it so the drain below
	// can wait for Serve to return whether or not the select already took the
	// value. No goroutine outlives Run.
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpSrv.Serve(tls.NewListener(ln, tlsCfg))
		close(serveErr)
	}()

	defer func() {
		drainCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		if err := httpSrv.Shutdown(drainCtx); err != nil {
			slog.Error("S3 gate did not drain within grace period", "error", err)
		}
		<-serveErr
	}()

	slog.Info("Starting S3 gate", "addr", addr, "http2", true)

	select {
	case <-ctx.Done():
		slog.Info("Shutting down S3 gate...")
		return nil
	case err := <-serveErr:
		// A gate that cannot bind must take the process down rather than
		// leave the cluster running headless.
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("s3 gate: %w", err)
	}
}

// startProfiling starts CPU profiling into a temp file.
func (s *Server) startProfiling() error {
	tmpFile, err := os.CreateTemp("", "predastore-cpu-*.prof.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temp profile file: %w", err)
	}
	s.pprofFile = tmpFile

	if err := pprof.StartCPUProfile(tmpFile); err != nil {
		if closeErr := tmpFile.Close(); closeErr != nil {
			slog.Debug("Failed to close temp profile file", "error", closeErr)
		}
		if removeErr := os.Remove(tmpFile.Name()); removeErr != nil {
			slog.Debug("Failed to remove temp profile file", "error", removeErr)
		}
		s.pprofFile = nil
		return fmt.Errorf("failed to start CPU profile: %w", err)
	}

	slog.Info("CPU profiling started", "tempFile", tmpFile.Name(), "outputPath", s.cfg.PprofOutputPath)
	return nil
}

// stopProfiling stops CPU profiling and saves the profile to the output path.
func (s *Server) stopProfiling() error {
	if s.pprofFile == nil {
		return nil
	}

	pprof.StopCPUProfile()
	tempPath := s.pprofFile.Name()
	if err := s.pprofFile.Close(); err != nil {
		slog.Warn("Failed to close pprof file", "error", err)
	}
	s.pprofFile = nil

	if err := copyFile(tempPath, s.cfg.PprofOutputPath); err != nil {
		slog.Error("Failed to save CPU profile", "error", err, "tempPath", tempPath)
		return err
	}

	if err := os.Remove(tempPath); err != nil {
		slog.Debug("Failed to remove temp profile file", "path", tempPath, "error", err)
	}

	slog.Info("CPU profile saved", "path", s.cfg.PprofOutputPath)
	return nil
}

// copyFile copies a file from src to dst.
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(dst), 0750); err != nil {
		return err
	}

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := dstFile.ReadFrom(srcFile); err != nil {
		return err
	}
	return dstFile.Sync()
}
