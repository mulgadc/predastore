package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/mulgadc/predastore/s3"

	_ "github.com/mulgadc/predastore/internal/fipsboot"
)

func main() {
	config := flag.String("config", "", "S3 server configuration file (required)")
	tlsKey := flag.String("tls-key", "certs/server.key", "Path to TLS key")
	tlsCert := flag.String("tls-cert", "certs/server.pem", "Path to TLS cert")
	basePath := flag.String("base-path", "", "Base path for the S3 directory when undefined in the config file")
	debug := flag.Bool("debug", false, "Enable verbose debug logs")
	port := flag.Int("port", 443, "Server port")
	host := flag.String("host", "0.0.0.0", "Server host")
	nodeID := flag.Int("node", -1, "Node ID to run (-1 = dev mode runs all nodes)")
	encryptionKeyFile := flag.String("encryption-key-file", "", "Path to 32-byte AES-256 master key for encryption at rest (required)")

	flag.Parse()

	// Environment variable override for config
	if os.Getenv("CONFIG") != "" {
		*config = os.Getenv("CONFIG")
	}
	if *config == "" {
		slog.Error("Missing required flag: -config")
		flag.Usage()
		os.Exit(1)
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
	if os.Getenv("NODE") != "" {
		*nodeID, _ = strconv.Atoi(os.Getenv("NODE"))
	}
	if os.Getenv("ENCRYPTION_KEY_FILE") != "" {
		*encryptionKeyFile = os.Getenv("ENCRYPTION_KEY_FILE")
	}
	if *encryptionKeyFile == "" {
		slog.Error("Missing required flag: -encryption-key-file (or ENCRYPTION_KEY_FILE)")
		flag.Usage()
		os.Exit(1)
	}

	// Create the S3 server with all options
	server, err := s3.NewServer(
		s3.WithConfigPath(*config),
		s3.WithAddress(*host, *port),
		s3.WithTLS(*tlsCert, *tlsKey),
		s3.WithBasePath(*basePath),
		s3.WithDebug(*debug),
		s3.WithNodeID(*nodeID),
		s3.WithEncryptionKeyFile(*encryptionKeyFile),
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

	// Wait for shutdown signal or DB failure
	waitErr := server.WaitForShutdownSignal()

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		slog.Error("Error during shutdown", "error", err)
		os.Exit(1)
	}

	if waitErr != nil {
		slog.Error("Exiting due to database failure", "error", waitErr)
		os.Exit(1)
	}
}
