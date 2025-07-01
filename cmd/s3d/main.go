package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strconv"

	"github.com/mulgadc/predastore/s3"
	"go.uber.org/automaxprocs/maxprocs"
)

func main() {

	config := flag.String("config", "config/server.toml", "S3 server configuration file")
	tls_key := flag.String("tls-key", "config/server.key", "Path to TLS key")
	tls_cert := flag.String("tls-cert", "config/server.pem", "Path to TLS cert")
	base_path := flag.String("base-path", "", "Base path for the S3 directory when undefined in the config file")
	debug := flag.Bool("debug", false, "Enable verbose debug logs")
	port := flag.Int("port", 443, "Server port")
	host := flag.String("host", "0.0.0.0", "Server host")
	flag.Parse()

	// Env vars overwrite CLI options
	if os.Getenv("CONFIG") != "" {
		*config = os.Getenv("CONFIG")
	}

	if os.Getenv("TLS_KEY") != "" {
		*tls_key = os.Getenv("TLS_KEY")
	}

	if os.Getenv("TLS_CERT") != "" {
		*tls_cert = os.Getenv("TLS_CERT")
	}

	if os.Getenv("PORT") != "" {
		*port, _ = strconv.Atoi(os.Getenv("PORT"))
	}

	s3 := s3.New(&s3.Config{
		ConfigPath: *config,
		Port:       *port,
		Host:       *host,
		Debug:      *debug,
		BasePath:   *base_path,
	})

	// Adjust MAXPROCS if running under linux/cgroups quotas.
	undo, err := maxprocs.Set(maxprocs.Logger(log.Printf))
	if err != nil {
		log.Printf("Failed to set GOMAXPROCS: %v", err)
	} else {
		defer undo()
	}

	err = s3.ReadConfig()

	if err != nil {
		slog.Warn("Error reading config file", "error", err)
		os.Exit(-1)
	}

	app := s3.SetupRoutes()

	log.Fatal(app.ListenTLS(fmt.Sprintf("%s:%d", *host, *port), *tls_cert, *tls_key))

}
