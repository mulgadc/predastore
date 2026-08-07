package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mulgadc/predastore/clusterrun"
	"github.com/mulgadc/predastore/internal/gateway"
	"github.com/mulgadc/predastore/otelsetup"
	"github.com/mulgadc/predastore/pkg/masterkey"
	"golang.org/x/sync/errgroup"

	_ "github.com/mulgadc/predastore/internal/fipsboot"
)

func main() {
	if err := run(); err != nil {
		slog.Error("s3d exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	config := flag.String("config", "", "S3 server configuration file (required)")
	tlsKey := flag.String("tls-key", "certs/server.key", "Path to TLS key")
	tlsCert := flag.String("tls-cert", "certs/server.pem", "Path to TLS cert")
	basePath := flag.String("base-path", "", "Base path for the S3 directory when undefined in the config file")
	debug := flag.Bool("debug", false, "Enable verbose debug logs")
	port := flag.Int("port", 443, "S3 gateway port")
	host := flag.String("host", "0.0.0.0", "S3 gateway host")
	nodes := flag.String("nodes", "", "Comma-separated node IDs to run in this process (empty = every node in the topology)")
	encryptionKeyFile := flag.String("encryption-key-file", "", "Path to 32-byte AES-256 master key for encryption at rest (required)")

	flag.Parse()
	applyEnvOverrides(config, tlsKey, tlsCert, port, nodes, encryptionKeyFile)

	if *config == "" {
		flag.Usage()
		return errors.New("missing required flag: -config")
	}
	if *encryptionKeyFile == "" {
		flag.Usage()
		return errors.New("missing required flag: -encryption-key-file (or ENCRYPTION_KEY_FILE)")
	}

	// One context for the whole process: ctrl-c cancels the rpc servers, the
	// node services and the S3 gateway together.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Telemetry is best-effort: a failed init never blocks the S3 server.
	otelShutdown, err := otelsetup.Init(ctx, "predastore")
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

	cfg := &gateway.Config{ConfigPath: *config, BasePath: *basePath}
	if err := cfg.ReadConfig(); err != nil {
		return fmt.Errorf("read config: %w", err)
	}

	localIDs, err := parseNodeIDs(*nodes, cfg)
	if err != nil {
		return fmt.Errorf("invalid -nodes selection: %w", err)
	}
	key, err := masterkey.Load(*encryptionKeyFile)
	if err != nil {
		return fmt.Errorf("load master key: %w", err)
	}

	rt, err := clusterrun.Build(cfg, localIDs, *tlsCert, *tlsKey, key)
	if err != nil {
		return fmt.Errorf("build cluster runtime: %w", err)
	}

	server, err := gateway.NewServer(
		gateway.WithConfigPath(*config),
		gateway.WithAddress(*host, *port),
		gateway.WithTLS(*tlsCert, *tlsKey),
		gateway.WithBasePath(*basePath),
		gateway.WithDebug(*debug),
		gateway.WithEncryptionKeyFile(*encryptionKeyFile),
		gateway.WithClients(rt.Clients),
	)
	if err != nil {
		rt.Close()
		return fmt.Errorf("create server: %w", err)
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return rt.Run(gctx) })
	g.Go(func() error {
		// Serving before consensus settles would fail writes that would have
		// succeeded a moment later; a timeout degrades rather than aborts,
		// since the state client retries.
		if err := rt.WaitReady(30 * time.Second); err != nil {
			slog.Warn("No leader elected within timeout, serving anyway", "error", err)
		}
		return serveS3(gctx, server)
	})
	return g.Wait()
}

// serveS3 runs the gateway until the context is cancelled, then shuts it down
// within a bounded grace period.
func serveS3(ctx context.Context, server *gateway.Server) error {
	if err := server.ListenAndServeAsync(); err != nil {
		return fmt.Errorf("start s3 gateway: %w", err)
	}
	select {
	case <-ctx.Done():
	case err := <-server.ServeError():
		return fmt.Errorf("s3 gateway: %w", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shut down s3 gateway: %w", err)
	}
	return nil
}

// applyEnvOverrides lets the launcher configure s3d without rewriting flags.
func applyEnvOverrides(config, tlsKey, tlsCert *string, port *int, nodes, encryptionKeyFile *string) {
	if v := os.Getenv("CONFIG"); v != "" {
		*config = v
	}
	if v := os.Getenv("TLS_KEY"); v != "" {
		*tlsKey = v
	}
	if v := os.Getenv("TLS_CERT"); v != "" {
		*tlsCert = v
	}
	if v := os.Getenv("PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			*port = p
		}
	}
	if v := os.Getenv("NODES"); v != "" {
		*nodes = v
	}
	if v := os.Getenv("ENCRYPTION_KEY_FILE"); v != "" {
		*encryptionKeyFile = v
	}
}

// parseNodeIDs resolves the -nodes selection; empty selects every node in the
// topology, running the whole cluster in one process.
func parseNodeIDs(selection string, cfg *gateway.Config) ([]int, error) {
	if selection == "" {
		return clusterrun.AllNodeIDs(cfg), nil
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
