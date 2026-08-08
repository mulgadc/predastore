// Command s3d runs one host of a predastore cluster: the nodes the config
// pins to the host named by -host, and the S3 gateway in front of them. It is
// a thin entrypoint — flags, environment, telemetry and a signal context, then
// predastore.Run.
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
	basePath := flag.String("base-path", "", "Base path for the S3 directory when undefined in the config file")
	debug := flag.Bool("debug", false, "Enable verbose debug logs")
	host := flag.Int("host", 0, "ID of the [[host]] this process runs (required)")
	encryptionKeyFile := flag.String("encryption-key-file", "", "Path to 32-byte AES-256 master key for encryption at rest (required)")

	flag.Parse()
	applyEnvOverrides(configPath, host, encryptionKeyFile)

	if *configPath == "" {
		flag.Usage()
		return errors.New("missing required flag: -config")
	}
	if *host <= 0 {
		flag.Usage()
		return errors.New("missing required flag: -host (or HOST)")
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

	cfg, err := predastore.LoadConfig(*configPath)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	// The flag is the fallback the config file overrides, not the other way
	// round, so a config that pins its own base path stays portable.
	if cfg.BasePath == "" {
		cfg.BasePath = *basePath
	}

	key, err := masterkey.Load(*encryptionKeyFile)
	if err != nil {
		return fmt.Errorf("load master key: %w", err)
	}

	return predastore.Run(ctx, predastore.Options{
		Config:    cfg,
		HostID:    predastore.HostID(*host),
		MasterKey: key,
		Debug:     *debug,
	})
}

// applyEnvOverrides lets the launcher configure s3d without rewriting flags.
func applyEnvOverrides(config *string, host *int, encryptionKeyFile *string) {
	if v := os.Getenv("CONFIG"); v != "" {
		*config = v
	}
	if v := os.Getenv("HOST"); v != "" {
		if id, err := strconv.Atoi(v); err == nil {
			*host = id
		}
	}
	if v := os.Getenv("ENCRYPTION_KEY_FILE"); v != "" {
		*encryptionKeyFile = v
	}
}
