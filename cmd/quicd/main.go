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
	flag.Parse()

	if env := os.Getenv("ENCRYPTION_KEY_FILE"); env != "" {
		*encryptionKeyFile = env
	}
	if *encryptionKeyFile == "" {
		slog.Error("Missing required flag: -encryption-key-file (or ENCRYPTION_KEY_FILE)")
		flag.Usage()
		os.Exit(1)
	}

	key, err := masterkey.Load(*encryptionKeyFile)
	if err != nil {
		slog.Error("Failed to load master key", "path", *encryptionKeyFile, "error", err)
		os.Exit(1)
	}

	quicserver.New(*storageRoot, *addr, quicserver.WithMasterKey(key))
}
