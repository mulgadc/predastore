package s3

import (
	"context"
	"fmt"
	"log"
	"log/slog"

	"github.com/gofiber/fiber/v2"
	"github.com/mulgadc/predastore/backend"
	"go.uber.org/automaxprocs/maxprocs"
)

// Server encapsulates the S3-compatible server with all its dependencies
type Server struct {
	config  *Config
	backend backend.Backend
	app     *fiber.App

	// TLS configuration
	tlsCert string
	tlsKey  string

	// Server state
	started bool
}

// ServerOption is a functional option for configuring the Server
type ServerOption func(*Server) error

// NewServer creates a new S3 server with the given options.
// This is the recommended way to create and start a predastore server.
//
// Example usage:
//
//	server, err := s3.NewServer(
//	    s3.WithConfigPath("config/server.toml"),
//	    s3.WithAddress("0.0.0.0", 8443),
//	    s3.WithTLS("config/server.pem", "config/server.key"),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer server.Shutdown(context.Background())
//	log.Fatal(server.ListenAndServe())
func NewServer(opts ...ServerOption) (*Server, error) {
	// Create server with defaults
	s := &Server{
		config: &Config{
			ConfigPath:  "config/server.toml",
			Port:        443,
			Host:        "0.0.0.0",
			BackendType: "filesystem",
		},
		tlsCert: "config/server.pem",
		tlsKey:  "config/server.key",
	}

	// Apply options
	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, fmt.Errorf("failed to apply server option: %w", err)
		}
	}

	// Adjust MAXPROCS for cgroups
	undo, err := maxprocs.Set(maxprocs.Logger(log.Printf))
	if err != nil {
		slog.Warn("Failed to set GOMAXPROCS", "error", err)
	} else {
		defer undo()
	}

	// Read and validate configuration
	if err := s.config.ReadConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	// Initialize backend
	if err := s.initBackend(); err != nil {
		return nil, fmt.Errorf("failed to initialize backend: %w", err)
	}

	// Setup routes
	s.app = s.config.SetupRoutesWithBackend(s.backend)

	return s, nil
}

// initBackend initializes the storage backend based on configuration
func (s *Server) initBackend() error {
	backendType := s.config.BackendType
	if backendType == "" {
		backendType = "filesystem"
	}

	var err error
	s.backend, err = backend.New(backendType, s.config)
	if err != nil {
		return fmt.Errorf("failed to create %s backend: %w", backendType, err)
	}

	return nil
}

// ListenAndServe starts the server and blocks until it's stopped.
// Use Shutdown() to stop the server gracefully.
func (s *Server) ListenAndServe() error {
	s.started = true
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	slog.Info("Starting S3 server", "address", addr)
	return s.app.ListenTLS(addr, s.tlsCert, s.tlsKey)
}

// ListenAndServeAsync starts the server in a goroutine.
// Returns immediately after starting. Use Shutdown() to stop.
func (s *Server) ListenAndServeAsync() error {
	s.started = true
	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	slog.Info("Starting S3 server (async)", "address", addr)

	go func() {
		if err := s.app.ListenTLS(addr, s.tlsCert, s.tlsKey); err != nil {
			slog.Error("Server error", "error", err)
		}
	}()

	return nil
}

// Shutdown gracefully shuts down the server with the given context.
func (s *Server) Shutdown(ctx context.Context) error {
	if !s.started {
		return nil
	}
	slog.Info("Shutting down S3 server")
	return s.app.ShutdownWithContext(ctx)
}

// App returns the underlying Fiber app for advanced usage.
func (s *Server) App() *fiber.App {
	return s.app
}

// Config returns the server configuration.
func (s *Server) Config() *Config {
	return s.config
}

// --- Functional Options ---

// WithConfigPath sets the path to the configuration file.
func WithConfigPath(path string) ServerOption {
	return func(s *Server) error {
		s.config.ConfigPath = path
		return nil
	}
}

// WithAddress sets the host and port for the server.
func WithAddress(host string, port int) ServerOption {
	return func(s *Server) error {
		s.config.Host = host
		s.config.Port = port
		return nil
	}
}

// WithTLS sets the TLS certificate and key paths.
func WithTLS(certPath, keyPath string) ServerOption {
	return func(s *Server) error {
		s.tlsCert = certPath
		s.tlsKey = keyPath
		return nil
	}
}

// WithBasePath sets the base path for storage.
func WithBasePath(path string) ServerOption {
	return func(s *Server) error {
		s.config.BasePath = path
		return nil
	}
}

// WithDebug enables debug logging.
func WithDebug(debug bool) ServerOption {
	return func(s *Server) error {
		s.config.Debug = debug
		return nil
	}
}

// WithDisableLogging disables request logging.
func WithDisableLogging(disable bool) ServerOption {
	return func(s *Server) error {
		s.config.DisableLogging = disable
		return nil
	}
}

// WithBackendType sets the storage backend type ("filesystem" or "distributed").
func WithBackendType(backendType string) ServerOption {
	return func(s *Server) error {
		s.config.BackendType = backendType
		return nil
	}
}

// WithConfig directly sets the configuration.
func WithConfig(config *Config) ServerOption {
	return func(s *Server) error {
		// Copy config values
		s.config = config
		return nil
	}
}
