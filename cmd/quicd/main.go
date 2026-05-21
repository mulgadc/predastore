package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/mulgadc/predastore/internal/masterkey"
	"github.com/mulgadc/predastore/quic/quicserver"
)

func main() {
	storageRoot := flag.String("storage-root", "./data", "Storage root directory")
	addr := flag.String("addr", "0.0.0.0:7443", "Address to listen on")
	encryptionKeyFile := flag.String("encryption-key-file", "", "Path to 32-byte AES-256 master key for encryption at rest (required)")
	tlsCert := flag.String("tls-cert", "", "Path to PEM-encoded TLS server certificate (required)")
	tlsKey := flag.String("tls-key", "", "Path to PEM-encoded TLS server key (required)")
	flag.Parse()

	if env := os.Getenv("ENCRYPTION_KEY_FILE"); env != "" {
		*encryptionKeyFile = env
	}
	if env := os.Getenv("TLS_CERT"); env != "" {
		*tlsCert = env
	}
	if env := os.Getenv("TLS_KEY"); env != "" {
		*tlsKey = env
	}
	if *encryptionKeyFile == "" {
		slog.Error("Missing required flag: -encryption-key-file (or ENCRYPTION_KEY_FILE)")
		flag.Usage()
		os.Exit(1)
	}
	if *tlsCert == "" || *tlsKey == "" {
		slog.Error("Missing required flags: -tls-cert and -tls-key (or TLS_CERT/TLS_KEY)")
		flag.Usage()
		os.Exit(1)
	}

	key, err := masterkey.Load(*encryptionKeyFile)
	if err != nil {
		slog.Error("Failed to load master key", "path", *encryptionKeyFile, "error", err)
		os.Exit(1)
	}

	quicserver.New(*storageRoot, *addr,
		quicserver.WithMasterKey(key),
		quicserver.WithTLSCertFiles(*tlsCert, *tlsKey),
	)
}
