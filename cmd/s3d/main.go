// Command s3d runs one host of a predastore cluster: the nodes the config
// pins to the host named by -host, and the S3 gate in front of them. It is
// a thin entrypoint — flags, telemetry and a signal context, then predastore.Run.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mulgadc/predastore"
	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/mulgadc/predastore/pkg/otelsetup"

	_ "github.com/mulgadc/predastore/internal/fipsboot"
)

func main() {
	if err := run(); err != nil {
		slog.Error("s3d exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "S3 server configuration file (required)")
	host := flag.Int("host", 0, "ID of the [[host]] this process runs (required)")
	encryptionKeyFile := flag.String("encryption-key-file", "", "Path to 32-byte AES-256 master key for encryption at rest (required)")
	pprofEnabled := flag.Bool("pprof", false, "Write a CPU profile for the lifetime of the process")
	pprofOutput := flag.String("pprof-output", "", "Where the CPU profile is saved")
	logLevel := slog.LevelInfo
	flag.TextVar(&logLevel, "log-level", logLevel, "Minimum log level (debug|info|warn|error)")

	flag.Parse()

	if *configPath == "" {
		flag.Usage()
		return errors.New("missing required flag: -config")
	}
	if *host <= 0 {
		flag.Usage()
		return errors.New("missing required flag: -host")
	}
	if *encryptionKeyFile == "" {
		flag.Usage()
		return errors.New("missing required flag: -encryption-key-file")
	}

	// One context for the whole process: ctrl-c cancels the rpc servers, the
	// node services and the S3 gate together.
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

	// After Init, so the default logger also fans out to the OTLP bridge.
	otelsetup.SetDefaultJSONLogger(logLevel)

	cfg, err := predastore.LoadConfig(*configPath)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}

	key, err := masterkey.Load(*encryptionKeyFile)
	if err != nil {
		return fmt.Errorf("load master key: %w", err)
	}

	return predastore.Run(ctx, predastore.Options{
		Config:    cfg,
		HostID:    predastore.HostID(*host),
		MasterKey: key,
		Pprof:     *pprofEnabled,
		PprofPath: *pprofOutput,
	})
}
