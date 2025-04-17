package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strconv"

	"github.com/mulgadc/predastore/s3"
)

func main() {

	s3 := s3.New()

	config := flag.String("config", "config/server.toml", "S3 server configuration file")
	tls_key := flag.String("tls-key", "config/server.key", "Path to TLS key")
	tls_cert := flag.String("tls-cert", "config/server.pem", "Path to TLS cert")
	port := flag.Int("port", 443, "Server port")

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

	err := s3.ReadConfig(*config)

	if err != nil {
		slog.Warn("Error reading config file", "error", err)
		os.Exit(-1)
	}

	app := s3.SetupRoutes()
	//log.Fatal(app.Listen(":3000"))

	log.Fatal(app.ListenTLS(fmt.Sprintf(":%d", *port), *tls_cert, *tls_key))

}
