package main

import (
	"flag"
	"log"
	"os"
	"strconv"

	"github.com/mulgadc/predastore/s3"

	// Import backends to trigger their init() registration
	_ "github.com/mulgadc/predastore/backend/filesystem"
)

func main() {
	config := flag.String("config", "config/server.toml", "S3 server configuration file")
	tlsKey := flag.String("tls-key", "config/server.key", "Path to TLS key")
	tlsCert := flag.String("tls-cert", "config/server.pem", "Path to TLS cert")
	basePath := flag.String("base-path", "", "Base path for the S3 directory when undefined in the config file")
	debug := flag.Bool("debug", false, "Enable verbose debug logs")
	port := flag.Int("port", 443, "Server port")
	host := flag.String("host", "0.0.0.0", "Server host")
	flag.Parse()

	// Env vars overwrite CLI options
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

	server, err := s3.NewServer(
		s3.WithConfigPath(*config),
		s3.WithAddress(*host, *port),
		s3.WithTLS(*tlsCert, *tlsKey),
		s3.WithBasePath(*basePath),
		s3.WithDebug(*debug),
	)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	log.Fatal(server.ListenAndServe())
}
