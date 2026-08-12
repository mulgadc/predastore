// Command s3d runs one host of a predastore cluster: the nodes the config
// pins to the host named by -host, and the S3 gate in front of them. It is
// a thin entrypoint — flags, telemetry and a signal context, then predastore.Run.
package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mulgadc/bluebottle/pkg/masterkey"
	"github.com/mulgadc/bluebottle/pkg/otelsetup"
	"github.com/mulgadc/predastore"

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
	hostID := flag.Int("host", 0, "ID of the [[host]] this process runs (required)")
	bindAddr := flag.String("bind-addr", "", "Listen address for this host's cluster traffic, without a port (overrides bind_addr)")
	gateBindAddr := flag.String("gate-bind-addr", "", "Listen address for the S3 API, without a port (overrides the gate's bind_addr)")
	dataDir := flag.String("data-dir", "", "On-disk root for this host's nodes (overrides data_dir)")
	encryptionKey := flag.String("encryption-key", "", "Path to the 32-byte AES-256 key protecting data at rest (overrides encryption_key)")
	tlsCert := flag.String("tls-cert", "", "Path to this host's TLS certificate (overrides tls_cert)")
	tlsKey := flag.String("tls-key", "", "Path to this host's TLS key (overrides tls_key)")
	logLevel := slog.LevelInfo
	flag.TextVar(&logLevel, "log-level", logLevel, "Minimum log level (debug|info|warn|error)")

	flag.Parse()

	if *configPath == "" {
		flag.Usage()
		return errors.New("missing required flag: -config")
	}
	if *hostID <= 0 {
		flag.Usage()
		return errors.New("missing required flag: -host")
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

	// The host-local fields have two sources, so they are settled here, into the
	// entry the rest of the tree reads: flag, then config, then default.
	host := hostEntry(cfg, predastore.HostID(*hostID))
	if host == nil {
		return fmt.Errorf("host %d is not in %s", *hostID, *configPath)
	}
	host.BindAddr = cmp.Or(*bindAddr, host.BindAddr, host.Addr)
	if *gateBindAddr != "" {
		gate := gateEntry(host)
		if gate == nil {
			return fmt.Errorf("-gate-bind-addr given but host %d runs no gate", *hostID)
		}
		gate.BindAddr = *gateBindAddr
	}
	host.DataDir = cmp.Or(*dataDir, host.DataDir)
	host.EncryptionKey = cmp.Or(*encryptionKey, host.EncryptionKey)
	host.TLSCert = cmp.Or(*tlsCert, host.TLSCert)
	host.TLSKey = cmp.Or(*tlsKey, host.TLSKey)
	if err := validateHost(host); err != nil {
		return err
	}
	// The file's own checks run again over the merged tree: -data-dir can
	// supply a root the file never had, and a node's derived directory only
	// collides with an explicit one once that root is known.
	if err := cfg.Validate(); err != nil {
		return err
	}

	key, err := masterkey.Load(host.EncryptionKey)
	if err != nil {
		return fmt.Errorf("load master key: %w", err)
	}

	return predastore.Run(ctx, predastore.Options{
		Config:    cfg,
		HostID:    host.ID,
		MasterKey: key,
	})
}

// hostEntry is the [[host]] this process runs, addressable so the flags that
// override it can be written back into the config everything downstream reads.
func hostEntry(cfg *predastore.Config, id predastore.HostID) *predastore.HostConfig {
	for i := range cfg.Hosts {
		if cfg.Hosts[i].ID == id {
			return &cfg.Hosts[i]
		}
	}
	return nil
}

// gateEntry is the gate of this host, addressable for the same reason: the S3
// listen address is a flag as well as a config field. A host declares at most
// one, so the first is the only one.
func gateEntry(h *predastore.HostConfig) *predastore.NodeConfig {
	for i := range h.Nodes {
		if h.Nodes[i].Role == predastore.RoleGate {
			return &h.Nodes[i]
		}
	}
	return nil
}

// validateHost checks the resolved fields this machine needs. It is local by
// design: a path missing from another host's entry is that host's problem and
// must not stop this one from starting.
func validateHost(h *predastore.HostConfig) error {
	if h.EncryptionKey == "" {
		return errors.New("no encryption key: set --encryption-key or encryption_key")
	}
	if h.TLSCert == "" || h.TLSKey == "" {
		return errors.New("no TLS identity: set --tls-cert and --tls-key, or tls_cert and tls_key")
	}
	return nil
}
