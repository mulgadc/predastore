package s3

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sync"

	"github.com/mulgadc/predastore/otelsetup"
	"github.com/mulgadc/predastore/pkg/masterkey"
)

// Server encapsulates the S3-compatible server with all its components.
type Server struct {
	// Configuration
	configPath        string
	host              string
	port              int
	tlsCert           string
	tlsKey            string
	basePath          string
	debug             bool
	encryptionKeyPath string         // Path to the 32-byte AES-256 master key file.
	masterKey         *masterkey.Key // Loaded master key handle (AEAD + fingerprint, no raw bytes).

	// Runtime state
	config   *Config
	server   *HTTP2Server
	clients  Clients // cluster clients, wired by the process that runs the nodes
	credProv CredentialProvider

	// Profiling
	pprofEnabled    bool
	pprofFile       *os.File
	pprofOutputPath string

	// Lifecycle
	mu      sync.Mutex
	running bool
	// serveErr carries a fatal error from the async listener so the caller
	// can shut down instead of running on with a dead gateway.
	serveErr chan error
}

// Option configures a Server.
type Option func(*Server) error

// NewServer creates a new S3 server with the given options.
func NewServer(opts ...Option) (*Server, error) {
	s := &Server{
		host:     "0.0.0.0",
		port:     8443,
		serveErr: make(chan error, 1),
	}

	// Apply options
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, fmt.Errorf("failed to apply option: %w", err)
		}
	}

	// Initialize the server
	if err := s.init(); err != nil {
		return nil, fmt.Errorf("failed to initialize server: %w", err)
	}

	return s, nil
}

// WithConfigPath sets the path to the TOML configuration file.
func WithConfigPath(path string) Option {
	return func(s *Server) error {
		s.configPath = path
		return nil
	}
}

// WithAddress sets the server host and port.
func WithAddress(host string, port int) Option {
	return func(s *Server) error {
		s.host = host
		s.port = port
		return nil
	}
}

// WithTLS sets the TLS certificate and key paths.
func WithTLS(certPath, keyPath string) Option {
	return func(s *Server) error {
		s.tlsCert = certPath
		s.tlsKey = keyPath
		return nil
	}
}

// WithBasePath sets the base directory for data storage.
func WithBasePath(path string) Option {
	return func(s *Server) error {
		s.basePath = path
		return nil
	}
}

// WithDebug enables debug logging.
func WithDebug(enabled bool) Option {
	return func(s *Server) error {
		s.debug = enabled
		return nil
	}
}

// WithClients supplies the cluster clients the gateway works through. The
// server then only runs the S3 HTTPS frontend: no state or storage nodes are
// launched, and the caller owns their supporting runtime (rpc server, raft
// nodes, shard stores). Used by cluster mode, where cmd/s3d assembles the
// topology.
func WithClients(clients Clients) Option {
	return func(s *Server) error {
		s.clients = clients
		return nil
	}
}

// WithEncryptionKeyFile sets the path to the 32-byte AES-256 master key file
// used for encryption at rest. The path is supplied via CLI/env only, never
// TOML, so no plaintext secret path lives in the config file.
func WithEncryptionKeyFile(path string) Option {
	return func(s *Server) error {
		s.encryptionKeyPath = path
		return nil
	}
}

// WithPprof enables CPU profiling.
// The profile is written to a temp file during operation and saved to outputPath on shutdown.
// If outputPath is empty, it defaults to /tmp/predastore-cpu.prof.
func WithPprof(enabled bool, outputPath string) Option {
	return func(s *Server) error {
		s.pprofEnabled = enabled
		if outputPath == "" {
			outputPath = "/tmp/predastore-cpu.prof"
		}
		s.pprofOutputPath = outputPath
		return nil
	}
}

// init initializes the server components.
func (s *Server) init() error {
	// Check environment variable for pprof if not already enabled
	if !s.pprofEnabled && os.Getenv("PPROF_ENABLED") == "1" {
		s.pprofEnabled = true
		if s.pprofOutputPath == "" {
			s.pprofOutputPath = os.Getenv("PPROF_OUTPUT")
			if s.pprofOutputPath == "" {
				s.pprofOutputPath = "/tmp/predastore-cpu.prof"
			}
		}
	}

	// Start CPU profiling if enabled
	if s.pprofEnabled {
		if err := s.startProfiling(); err != nil {
			slog.Error("Failed to start CPU profiling", "error", err)
			// Don't fail server start, just log the error
		}
	}

	// Create and load configuration
	s.config = &Config{
		ConfigPath: s.configPath,
		Debug:      s.debug,
		BasePath:   s.basePath,
	}

	// Read configuration file if provided
	if s.configPath != "" {
		if err := s.config.ReadConfig(); err != nil {
			return fmt.Errorf("failed to read config: %w", err)
		}
	}

	// CLI/env flags override config file settings
	// This ensures HIVE_PREDASTORE_DEBUG=true works even if config file has debug=false
	if s.debug {
		s.config.Debug = true
	}

	// Master key is mandatory. The key path itself is delivered via CLI/env
	// only (not TOML) to avoid plaintext-secret-in-config and to keep s3d's
	// config surface decoupled from quicd's. See encryption-at-rest plan.
	if s.encryptionKeyPath == "" {
		return fmt.Errorf("encryption key file is required (use -encryption-key-file or ENCRYPTION_KEY_FILE)")
	}
	key, err := masterkey.Load(s.encryptionKeyPath)
	if err != nil {
		return fmt.Errorf("load master key: %w", err)
	}
	s.masterKey = key
	slog.Info("master key loaded", "fingerprint", key.Fingerprint)

	// Set log level early so debug logs during initialization are visible
	var logLevel slog.Level
	if s.config.Debug {
		logLevel = slog.LevelDebug
	} else if s.config.DisableLogging {
		logLevel = slog.LevelError
	} else {
		logLevel = slog.LevelInfo
	}
	otelsetup.SetDefaultJSONLogger(logLevel)

	if s.config.Debug {
		slog.Info("Debug logging enabled")
	}

	// The clients arrive fully wired from the process that owns the cluster
	// nodes; the gateway never launches storage or state itself.
	if s.clients.State == nil || s.clients.Storage == nil {
		return fmt.Errorf("no cluster clients provided: build the cluster runtime and pass WithClients")
	}

	// Initialize credential provider
	credProv, err := s.initCredentialProvider()
	if err != nil {
		return fmt.Errorf("failed to initialize credential provider: %w", err)
	}
	s.credProv = credProv

	// Setup HTTP routes using the HTTP/2 server
	slog.Info("Server init")
	s.server = NewHTTP2Server(s.config, s.clients, s.credProv)
	slog.Info("HTTP/2 server initialized - using net/http for connection multiplexing")

	return nil
}

// initCredentialProvider resolves the auth chain: NATS-backed IAM when it is
// configured, with the config-defined accounts always as the fallback.
func (s *Server) initCredentialProvider() (CredentialProvider, error) {
	configProv := NewConfigProvider(s.config.Auth)

	if s.config.IAM == nil {
		slog.Info("IAM not configured, using config-only auth")
		return configProv, nil
	}

	natsProv, err := NewNATSIAMProvider(s.config.IAM)
	if err != nil {
		return nil, fmt.Errorf("IAM configured but NATS provider failed to initialize: %w", err)
	}

	slog.Info("Using NATS IAM + config chain auth")
	return NewChainProvider(natsProv, configProv), nil
}

// ListenAndServe starts the server and blocks until shutdown.
func (s *Server) ListenAndServe() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}
	s.running = true
	s.mu.Unlock()

	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	slog.Info("Starting S3 server", "host", s.host, "port", s.port)

	if s.tlsCert == "" || s.tlsKey == "" {
		return fmt.Errorf("TLS is required - set tlsCert and tlsKey")
	}

	slog.Info(">>> USING HTTP/2 SERVER (net/http + chi) <<<", "addr", addr)
	return s.server.ListenAndServe(addr, s.tlsCert, s.tlsKey)
}

// ListenAndServeAsync starts the server in a goroutine.
func (s *Server) ListenAndServeAsync() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}
	s.running = true
	s.mu.Unlock()

	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	slog.Info("Starting S3 server (async)", "host", s.host, "port", s.port)

	if s.tlsCert == "" || s.tlsKey == "" {
		return fmt.Errorf("TLS is required - set tlsCert and tlsKey")
	}

	go func() {
		slog.Info(">>> USING HTTP/2 SERVER (net/http + chi) <<<", "addr", addr)
		err := s.server.ListenAndServe(addr, s.tlsCert, s.tlsKey)
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return
		}
		slog.Error("Server error", "error", err)
		select {
		case s.serveErr <- err:
		default:
		}
	}()

	return nil
}

// startProfiling starts CPU profiling to a temp file.
func (s *Server) startProfiling() error {
	// Create temp file for profiling
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
		return fmt.Errorf("failed to start CPU profile: %w", err)
	}

	slog.Info("CPU profiling started", "tempFile", tmpFile.Name(), "outputPath", s.pprofOutputPath)
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

	// Copy temp file to output path
	if err := copyFile(tempPath, s.pprofOutputPath); err != nil {
		slog.Error("Failed to save CPU profile", "error", err, "tempPath", tempPath)
		return err
	}

	// Remove temp file
	if err := os.Remove(tempPath); err != nil {
		slog.Debug("Failed to remove temp profile file", "path", tempPath, "error", err)
	}

	slog.Info("CPU profile saved", "path", s.pprofOutputPath)
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

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	slog.Info("Shutting down S3 server...")

	// Shutdown HTTP server first (stop accepting new requests)
	slog.Info("Shutting down HTTP server...")
	if s.server != nil {
		if err := s.server.Shutdown(ctx); err != nil {
			slog.Error("Error shutting down HTTP server", "error", err)
		}
	}

	// Close credential provider (NATS connections, watchers)
	if s.credProv != nil {
		s.credProv.Close()
	}

	// Stop profiling and save profile
	if s.pprofEnabled {
		if err := s.stopProfiling(); err != nil {
			slog.Error("Error stopping CPU profile", "error", err)
		}
	}

	s.running = false

	slog.Info("S3 server shutdown complete")
	return nil
}

// WaitForShutdownSignal blocks until SIGINT/SIGTERM is received or the
// embedded database crashes. Returns an error if shutdown was triggered by a
// DB failure, nil for normal signal-based shutdown.
// ServeError reports a fatal listener failure. A gateway that cannot bind
// must take the process down rather than leave the cluster running headless.
func (s *Server) ServeError() <-chan error { return s.serveErr }
