package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/mulgadc/predastore/s3"
)

func main() {
	config := flag.String("config", "config/server.toml", "S3 server configuration file")
	tlsKey := flag.String("tls-key", "config/server.key", "Path to TLS key")
	tlsCert := flag.String("tls-cert", "config/server.pem", "Path to TLS cert")
	basePath := flag.String("base-path", "", "Base path for the S3 directory when undefined in the config file")
	debug := flag.Bool("debug", false, "Enable verbose debug logs")
	port := flag.Int("port", 443, "Server port")
	host := flag.String("host", "0.0.0.0", "Server host")
	backendType := flag.String("backend", "filesystem", "Storage backend type (filesystem, distributed)")
	nodeID := flag.Int("node", -1, "Node ID to run (distributed mode only, -1 = dev mode runs all nodes)")

	flag.Parse()

	// Environment variables override CLI options
	if os.Getenv("CONFIG") != "" {
		*config = os.Getenv("CONFIG")
	}
	if os.Getenv("TLS_KEY") != "" {
		*tlsKey = os.Getenv("TLS_KEY")
	}
	if os.Getenv("TLS_CERT") != "" {
		*tlsCert = os.Getenv("TLS_CERT")
	}
	if os.Getenv("PORT") != "" {
		*port, _ = strconv.Atoi(os.Getenv("PORT"))
	}
	if os.Getenv("BACKEND") != "" {
		*backendType = os.Getenv("BACKEND")
	}
	if os.Getenv("NODE") != "" {
		*nodeID, _ = strconv.Atoi(os.Getenv("NODE"))
	}

	// Determine backend type
	backend := s3.BackendFilesystem
	if *backendType == "distributed" {
		backend = s3.BackendDistributed
	}

	// Create the S3 server with all options
	server, err := s3.NewServer(
		s3.WithConfigPath(*config),
		s3.WithAddress(*host, *port),
		s3.WithTLS(*tlsCert, *tlsKey),
		s3.WithBasePath(*basePath),
		s3.WithDebug(*debug),
		s3.WithBackend(backend),
		s3.WithNodeID(*nodeID),
	)
	if err != nil {
		slog.Error("Failed to create server", "error", err)
		os.Exit(1)
	}

	// Start server asynchronously
	if err := server.ListenAndServeAsync(); err != nil {
		slog.Error("Failed to start server", "error", err)
		os.Exit(1)
	}

	// Wait for shutdown signal
	server.WaitForShutdownSignal()

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		slog.Error("Error during shutdown", "error", err)
		os.Exit(1)
	}
}
