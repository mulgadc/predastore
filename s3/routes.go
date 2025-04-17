package s3

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

func (s3 *Config) SetupRoutes() *fiber.App {

	// Configure slog for logging
	slog.New(slog.NewTextHandler(os.Stdout, nil))

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
			case strings.Contains(err.Error(), "NoSuchBucket") || strings.Contains(err.Error(), "Bucket not found"):
				// File or bucket not found
				httpCode = fiber.StatusNotFound
				s3error.Code = "NoSuchBucket"
				s3error.Message = "The specified bucket does not exist"

			case strings.Contains(err.Error(), "AccessDenied") || strings.Contains(err.Error(), "Not enough permissions"):
				// Permission error
				httpCode = fiber.StatusForbidden
				s3error.Code = "AccessDenied"
				s3error.Message = "Access Denied"

			case strings.Contains(err.Error(), "NoSuchObject") || strings.Contains(err.Error(), "not found") ||
				errors.Is(err, os.ErrNotExist):
				// File not found
				httpCode = fiber.StatusNotFound
				s3error.Code = "NoSuchKey"
				s3error.Message = "The specified key does not exist"

			case strings.Contains(err.Error(), "Invalid signature") || strings.Contains(err.Error(), "Invalid access key"):
				// Authentication error
				httpCode = fiber.StatusForbidden
				s3error.Code = "SignatureDoesNotMatch"
				s3error.Message = "The request signature does not match"

			case strings.Contains(err.Error(), "Missing Authorization header"):
				// Missing auth header
				httpCode = fiber.StatusForbidden
				s3error.Code = "AccessDenied"
				s3error.Message = "Access Denied"

			case errors.As(err, &e):
				httpCode = e.Code
				s3error.Message = e.Message
				s3error.Code = "InternalError"
			default:
				s3error.Code = "InternalError"
				s3error.Message = err.Error()
			}

			// Add request ID and host ID
			s3error.RequestId = ctx.GetRespHeader("x-amz-request-id", "00000000-0000-0000-0000-000000000000")
			s3error.HostId = ctx.Hostname()

			// Set standard S3 error response headers
			ctx.Set("Content-Type", "application/xml")

			return ctx.Status(httpCode).XML(s3error)
		}},
	)

	// Add logging middleware
	app.Use(logger.New(logger.Config{
		Format: "[${ip}]:${port} ${status} - ${method} ${path}\n",
	}))

	// Check if authentication is enabled in config
	// (if there are auth entries in the config)
	needsAuth := len(s3.Auth) > 0

	// Map of endpoints that should have auth enabled
	authEndpoints := map[string]bool{
		// General bucket operations
		"/":  true, // List buckets always requires auth
		"/*": true, // Any direct root operation requires auth

		// Protected endpoints for buckets
		"/:bucket":   !s3.AllowAnonymousListing,
		"/:bucket/*": !s3.AllowAnonymousAccess,
	}

	// List buckets
	app.Get("/", func(c *fiber.Ctx) error {
		// Apply auth middleware if needed
		if needsAuth && authEndpoints["/"] {
			if err := s3.sigV4AuthMiddleware(c); err != nil {
				slog.Error("Authentication error", "error", err)
				return err
			}
		}
		return s3.ListBuckets(c)
	})

	// ListObjectsV2
	app.Get("/:bucket<alpha>", func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")

		// Check if bucket exists before checking auth
		bucketConfig, err := s3.BucketConfig(bucket)
		if err != nil {
			return fmt.Errorf("NoSuchBucket: %w", err)
		}

		// Apply auth middleware for non-public buckets
		if needsAuth && !bucketConfig.Public && authEndpoints["/:bucket"] {
			if err := s3.sigV4AuthMiddleware(c); err != nil {
				return err
			}
		}

		return s3.ListObjectsV2Handler(bucket, c)
	})

	// GetObject (HEAD)
	app.Head("/:bucket<alpha>/*", func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")
		file := c.Params("*")

		// Check if bucket exists before checking auth
		bucketConfig, err := s3.BucketConfig(bucket)
		if err != nil {
			return fmt.Errorf("NoSuchBucket: %w", err)
		}

		// Apply auth middleware for non-public buckets
		if needsAuth && !bucketConfig.Public && authEndpoints["/:bucket/*"] {
			if err := s3.sigV4AuthMiddleware(c); err != nil {
				return err
			}
		}

		return s3.GetObjectHead(bucket, file, c)
	})

	// GetObject (GET, BODY)
	app.Get("/:bucket<alpha>/*", func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")
		file := c.Params("*")

		// Check if bucket exists before checking auth
		bucketConfig, err := s3.BucketConfig(bucket)
		if err != nil {
			return fmt.Errorf("NoSuchBucket: %w", err)
		}

		// Apply auth middleware for non-public buckets
		if needsAuth && !bucketConfig.Public && authEndpoints["/:bucket/*"] {
			if err := s3.sigV4AuthMiddleware(c); err != nil {
				return err
			}
		}

		return s3.GetObject(bucket, file, c)
	})

	// PutObject (PUT)
	app.Put("/:bucket<alpha>/*", func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")
		file := c.Params("*")

		// Check if bucket exists before checking auth
		_, err := s3.BucketConfig(bucket)
		if err != nil {
			return fmt.Errorf("NoSuchBucket: %w", err)
		}

		// Always require auth for PUT operations
		if needsAuth {
			if err := s3.sigV4AuthMiddleware(c); err != nil {
				slog.Error("Authentication error", "error", err)
				return err
			}
		}

		return s3.PutObject(bucket, file, c)
	})

	return app
}
