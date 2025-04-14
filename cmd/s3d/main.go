package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
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

	fmt.Println("READING config file", *config)

	err := s3.ReadConfig(*config)

	if err != nil {
		slog.Warn("Error reading config file", "error", err)
		os.Exit(-1)
	}

	app := fiber.New(fiber.Config{
		// Override default error handler
		ErrorHandler: func(ctx *fiber.Ctx, err error) error {

			//spew.Dump(ctx)
			//fmt.Println("--")
			//fmt.Println(ctx.Request())

			// Status code defaults to 500
			code := fiber.StatusInternalServerError

			// Retrieve the custom status code if it's a *fiber.Error
			var e *fiber.Error
			if errors.As(err, &e) {
				code = e.Code
			}

			// Send custom error page
			err = ctx.Status(code).SendFile(fmt.Sprintf("./%d.html", code))
			if err != nil {
				// In case the SendFile fails
				return ctx.Status(fiber.StatusInternalServerError).SendString("Internal Server Error")
			}

			// Return from handler
			return nil
		}},
	)

	// Or extend your config for customization
	// Logging remote IP and Port
	app.Use(logger.New(logger.Config{
		Format: "[${ip}]:${port} ${status} - ${method} ${path}\n",
	}))

	//fmt.Println(s3)

	// List buckets
	app.Get("/", func(c *fiber.Ctx) error {
		return s3.ListBuckets(c)

		//return c.SendString(fmt.Sprintf("S3 server version (%s) for %s", s3.Version, c.Hostname()))
	})

	// ListObjectsV2
	app.Get("/:bucket<alpha>", func(c *fiber.Ctx) error {
		//spew.Dump(c.Request)
		//spew.Dump(c.Request().Header)

		bucket := c.Params("bucket")
		fmt.Println(c.Params("bucket"))
		return s3.ListObjectsV2Handler(bucket, c)
	})

	// GetObject (HEAD)
	app.Head("/:bucket<alpha>/*", func(c *fiber.Ctx) error {
		//spew.Dump(c.Request)
		//spew.Dump(c.Request().Header)

		bucket := c.Params("bucket")
		fmt.Println(c.Params("bucket"))

		file := c.Params("*")

		return s3.GetObjectHead(bucket, file, c)
	})

	// GetObject (GET, BODY)
	app.Get("/:bucket<alpha>/*", func(c *fiber.Ctx) error {
		//spew.Dump(c.Request)
		//spew.Dump(c.Request().Header)

		bucket := c.Params("bucket")
		fmt.Println(c.Params("bucket"))

		file := c.Params("*")

		return s3.GetObject(bucket, file, c)
	})

	// GetObject (GET, BODY)
	app.Put("/:bucket<alpha>/*", func(c *fiber.Ctx) error {

		//spew.Dump(c.Request)
		//spew.Dump(c.Request().Header)

		bucket := c.Params("bucket")
		fmt.Println(c.Params("bucket"))

		file := c.Params("*")
		fmt.Println(file)

		return s3.PutObject(bucket, file, c)
		//		s3.GetObject(bucket, file, c)
	})

	//log.Fatal(app.Listen(":3000"))

	log.Fatal(app.ListenTLS(fmt.Sprintf(":%d", *port), *tls_cert, *tls_key))

}
