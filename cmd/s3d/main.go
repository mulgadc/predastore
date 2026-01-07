package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strconv"

	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/backend/filesystem"
	"github.com/mulgadc/predastore/quic/quicserver"
	"github.com/mulgadc/predastore/s3"
	"go.uber.org/automaxprocs/maxprocs"
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

	// Initialize S3 configuration
	s3Config := s3.New(&s3.Config{
		ConfigPath: *config,
		Port:       *port,
		Host:       *host,
		Debug:      *debug,
		BasePath:   *basePath,
	})

	// Adjust MAXPROCS for cgroups environments
	undo, err := maxprocs.Set(maxprocs.Logger(log.Printf))
	if err != nil {
		log.Printf("Failed to set GOMAXPROCS: %v", err)
	} else {
		defer undo()
	}

	// Read configuration file
	if err := s3Config.ReadConfig(); err != nil {
		slog.Error("Error reading config file", "error", err)
		os.Exit(1)
	}

	// Create the storage backend
	var be backend.Backend

	switch *backendType {
	case "filesystem":
		// Convert s3.Config buckets to filesystem.BucketConfig
		buckets := make([]filesystem.BucketConfig, len(s3Config.Buckets))
		for i, b := range s3Config.Buckets {
			buckets[i] = filesystem.BucketConfig{
				Name:     b.Name,
				Pathname: b.Pathname,
				Region:   b.Region,
				Type:     b.Type,
				Public:   b.Public,
			}
		}

		fsConfig := &filesystem.Config{
			Buckets:   buckets,
			OwnerID:   "predastore",
			OwnerName: "predastore",
		}

		be, err = filesystem.New(fsConfig)
		if err != nil {
			slog.Error("Failed to initialize filesystem backend", "error", err)
			os.Exit(1)
		}
		slog.Info("Initialized filesystem backend", "buckets", len(buckets))

	case "distributed":
		slog.Error("Distributed backend not yet implemented")
		os.Exit(1)

	default:
		slog.Error("Unknown backend type", "type", *backendType)
		os.Exit(1)
	}

	defer be.Close()

	// Setup HTTP routes with the backend
	app := s3Config.SetupRoutesWithBackend(be)

	// Launch QUIC servers for distributed nodes
	for k, v := range s3Config.Nodes {
		quicAddr := fmt.Sprintf("%s:%d", v.Host, v.Port)
		slog.Info("Launching QUIC server", "node", k, "path", v.Path, "addr", quicAddr)
		go quicserver.New(v.Path, quicAddr)
	}

	// Start the S3 server
	slog.Info("Starting S3 server", "host", *host, "port", *port, "backend", *backendType)
	log.Fatal(app.ListenTLS(fmt.Sprintf("%s:%d", *host, *port), *tlsCert, *tlsKey))
}
