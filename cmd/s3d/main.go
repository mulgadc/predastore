package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mulgadc/predastore/internal/clusterrun"
	"github.com/mulgadc/predastore/otelsetup"
	"github.com/mulgadc/predastore/pkg/masterkey"
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
	nodeID := flag.Int("node", -1, "Node ID to run (-1 = dev mode runs all nodes; legacy [[db]]/[[nodes]] configs)")
	nodes := flag.String("nodes", "", "Comma-separated node IDs to run in this process (cluster topology configs; empty = all nodes)")
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
	if os.Getenv("NODES") != "" {
		*nodes = os.Getenv("NODES")
	}
	if os.Getenv("ENCRYPTION_KEY_FILE") != "" {
		*encryptionKeyFile = os.Getenv("ENCRYPTION_KEY_FILE")
	}
	if *encryptionKeyFile == "" {
		slog.Error("Missing required flag: -encryption-key-file (or ENCRYPTION_KEY_FILE)")
		flag.Usage()
		os.Exit(1)
	}

	// Telemetry is best-effort: a failed init never blocks the S3 server.
	otelShutdown, err := otelsetup.Init(context.Background(), "predastore")
	if err != nil {
		slog.Warn("Telemetry init failed, continuing without export", "error", err)
	} else {
		defer func() {
			flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := otelShutdown(flushCtx); err != nil {
				slog.Warn("Telemetry shutdown", "error", err)
			}
		}()
	}

	// A [[host]]/[[node]] topology selects cluster mode: the process runs
	// the selected nodes over the rpc transports and hands the S3 server a
	// prepared backend. Legacy [[db]]/[[nodes]] configs keep the historical
	// launch path.
	clusterCfg := &s3.Config{ConfigPath: *config, BasePath: *basePath}
	if err := clusterCfg.ReadConfig(); err != nil {
		slog.Error("Failed to read config", "error", err)
		os.Exit(1)
	}

	options := []s3.Option{
		s3.WithConfigPath(*config),
		s3.WithAddress(*host, *port),
		s3.WithTLS(*tlsCert, *tlsKey),
		s3.WithBasePath(*basePath),
		s3.WithDebug(*debug),
		s3.WithEncryptionKeyFile(*encryptionKeyFile),
	}

	var cleanup func()
	if len(clusterCfg.Hosts) > 0 {
		localIDs, err := parseNodeIDs(*nodes, clusterCfg)
		if err != nil {
			slog.Error("Invalid -nodes selection", "error", err)
			os.Exit(1)
		}
		key, err := masterkey.Load(*encryptionKeyFile)
		if err != nil {
			slog.Error("Failed to load master key", "error", err)
			os.Exit(1)
		}
		rt, err := clusterrun.Build(clusterCfg, localIDs, *tlsCert, *tlsKey, key)
		if err != nil {
			slog.Error("Failed to build cluster runtime", "error", err)
			os.Exit(1)
		}
		cleanup = rt.Close
		options = append(options, s3.WithPreparedBackend(rt.Backend))
	} else {
		if *nodes != "" {
			slog.Error("-nodes requires a [[host]]/[[node]] cluster topology in the config")
			os.Exit(1)
		}
		options = append(options, s3.WithNodeID(*nodeID))
	}

	// Create the S3 server with all options
	server, err := s3.NewServer(options...)
	if err != nil {
		slog.Error("Failed to create server", "error", err)
		if cleanup != nil {
			cleanup()
		}
		os.Exit(1)
	}

	// Start server asynchronously
	if err := server.ListenAndServeAsync(); err != nil {
		slog.Error("Failed to start server", "error", err)
		if cleanup != nil {
			cleanup()
		}
		os.Exit(1)
	}

	// Wait for shutdown signal or DB failure
	waitErr := server.WaitForShutdownSignal()

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	shutdownErr := server.Shutdown(ctx)
	if cleanup != nil {
		cleanup()
	}
	if shutdownErr != nil {
		slog.Error("Error during shutdown", "error", shutdownErr)
		os.Exit(1)
	}

	if waitErr != nil {
		slog.Error("Exiting due to database failure", "error", waitErr)
		os.Exit(1)
	}
}

// parseNodeIDs resolves the -nodes selection; empty selects every node in
// the topology, running the whole cluster in one process.
func parseNodeIDs(selection string, cfg *s3.Config) ([]int, error) {
	if selection == "" {
		ids := make([]int, len(cfg.ClusterNodes))
		for i, n := range cfg.ClusterNodes {
			ids[i] = n.ID
		}
		return ids, nil
	}
	parts := strings.Split(selection, ",")
	ids := make([]int, 0, len(parts))
	for _, p := range parts {
		id, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return nil, fmt.Errorf("bad node id %q: %w", p, err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}
