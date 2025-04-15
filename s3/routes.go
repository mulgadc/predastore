package s3

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

func (s3 *Config) SetupRoutes() *fiber.App {

	app := fiber.New(fiber.Config{
		// Override default error handler
		ErrorHandler: func(ctx *fiber.Ctx, err error) error {

			//spew.Dump(ctx)
			//fmt.Println("--")
			//fmt.Println(ctx.Request())

			// Status code defaults to 500
			httpCode := fiber.StatusInternalServerError
			var s3error S3Error
			var e *fiber.Error

			// Check for specific error types
			switch {
			case strings.Contains(err.Error(), "NoSuchBucket"):
				// File or bucket not found
				httpCode = fiber.StatusNotFound
				s3error.Code = err.Error()
				s3error.Message = "The specified bucket does not exist"

			case strings.Contains(err.Error(), "AccessDenied"):
				// Permission error
				httpCode = fiber.StatusForbidden
				s3error.Code = err.Error()
				s3error.Message = "Access Denied"

			case strings.Contains(err.Error(), "NoSuchObject"):
				// Permission error
				httpCode = fiber.StatusNotFound
				s3error.Code = err.Error()
				s3error.Message = "Key does not exist"

			case errors.As(err, &e):
				httpCode = e.Code
				s3error.Message = e.Message
			}

			return ctx.Status(httpCode).XML(s3error)

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

	return app
}
