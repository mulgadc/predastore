package s3

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/google/uuid"
	"github.com/mulgadc/predastore/backend"
)

// SetupRoutesWithBackend sets up the Fiber app with a provided backend
func (s3 *Config) SetupRoutesWithBackend(be backend.Backend) *fiber.App {

	var logLevel slog.Level

	if s3.Debug {
		logLevel = slog.LevelDebug
	} else if s3.DisableLogging {
		logLevel = slog.LevelError
	} else {
		logLevel = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	})

	// Create a new logger with the custom handler
	slogger := slog.New(handler)

	// Set it as the default logger
	slog.SetDefault(slogger)

	// Configure slog for logging
	slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Allow to overwrite stream request body via env var (e.g benchmarking script)
	streamRequestBodyEnv := os.Getenv("StreamRequestBody")

	streamRequestBody := true

	if streamRequestBodyEnv == "false" {
		streamRequestBody = false
	}

	app := fiber.New(fiber.Config{

		// Disable the startup banner
		DisableStartupMessage: s3.DisableLogging,

		// Set the body limit for S3 specs to 5GiB
		BodyLimit: 5 * 1024 * 1024 * 1024,

		// Use streaming for more efficiency
		StreamRequestBody: streamRequestBody,

		// Override default error handler
		ErrorHandler: func(ctx *fiber.Ctx, err error) error {
			return s3.ErrorHandler(ctx, err)
		}},
	)

	if !s3.DisableLogging {
		app.Use(logger.New())
	}

	/*
		app.Use(logger.New(logger.Config{
			Format: "[${ip}]:${port} ${status} - ${method} ${path}\n",
		}))
	*/

	// Add authentication middleware for all requests
	app.Use(s3.SigV4AuthMiddleware)

	// List buckets
	app.Get("/", func(c *fiber.Ctx) error {

		return be.ListBuckets(c)
	})

	// ListObjectsV2
	app.Get(`/:bucket<regex([a-z0-9-.]+)>`, func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")

		return be.ListObjectsV2Handler(bucket, c)
	})

	// GetObject (HEAD)
	app.Head(`/:bucket<regex([a-z0-9-.]+)>/*`, func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")
		file := c.Params("*")

		return be.GetObjectHead(bucket, file, c)
	})

	// GetObject (GET, BODY)
	app.Get(`/:bucket<regex([a-z0-9-.]+)>/*`, func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")
		file := c.Params("*")

		return be.GetObject(bucket, file, c)
	})

	// PutObject (PUT)
	app.Put(`/:bucket<regex([a-z0-9-.]+)>/*`, func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")
		file := c.Params("*")

		return be.PutObject(bucket, file, c)
	})

	app.Post(`/:bucket<regex([a-z0-9-.]+)>/*`, func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")
		file := c.Params("*")

		// Confirm if posting a multipart upload, or complete a multipart upload
		if c.Query("uploadId") == "" {
			return be.CreateMultipartUpload(bucket, file, c)
		} else {
			return be.CompleteMultipartUpload(bucket, file, c.Query("uploadId"), c)
		}
	})

	// DeleteObject
	app.Delete(`/:bucket<regex([a-z0-9-.]+)>/*`, func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")
		file := c.Params("*")

		fmt.Println("Deleting object", bucket, file)

		return be.DeleteObject(bucket, file, c)

	})

	return app
}

// SetupRoutes is a convenience wrapper for tests
// It uses the old implementation methods wrapped in an adapter
func (s3 *Config) SetupRoutes() *fiber.App {
	be := &configBackendAdapter{config: s3}
	return s3.SetupRoutesWithBackend(be)
}

// configBackendAdapter wraps old Config methods to implement Backend interface
type configBackendAdapter struct {
	config *Config
}

func (a *configBackendAdapter) DeleteObject(bucket string, file string, c *fiber.Ctx) error {
	return a.config.DeleteObject(bucket, file, c)
}

func (a *configBackendAdapter) GetObject(bucket string, file string, c *fiber.Ctx) error {
	return a.config.GetObject(bucket, file, c)
}

func (a *configBackendAdapter) GetObjectHead(bucket string, file string, c *fiber.Ctx) error {
	return a.config.GetObjectHead(bucket, file, c)
}

func (a *configBackendAdapter) PutObject(bucket string, file string, c *fiber.Ctx) error {
	return a.config.PutObject(bucket, file, c)
}

func (a *configBackendAdapter) ListBuckets(c *fiber.Ctx) error {
	return a.config.ListBuckets(c)
}

func (a *configBackendAdapter) ListObjectsV2Handler(bucket string, c *fiber.Ctx) error {
	return a.config.ListObjectsV2Handler(bucket, c)
}

func (a *configBackendAdapter) CreateMultipartUpload(bucket string, file string, c *fiber.Ctx) error {
	return a.config.CreateMultipartUpload(bucket, file, c)
}

func (a *configBackendAdapter) CompleteMultipartUpload(bucket string, file string, uploadId string, c *fiber.Ctx) error {
	return a.config.CompleteMultipartUpload(bucket, file, uploadId, c)
}

func (a *configBackendAdapter) PutObjectPart(bucket string, file string, partNumber int, uploadId string, c *fiber.Ctx) error {
	return a.config.PutObjectPart(bucket, file, partNumber, uploadId, c)
}

func (s3 *Config) ErrorHandler(ctx *fiber.Ctx, err error) error {
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

	s3error.RequestId = ctx.GetRespHeader("x-amz-request-id", uuid.NewString())
	s3error.HostId = ctx.Hostname()

	// Set standard S3 error response headers
	ctx.Set("Content-Type", "application/xml")

	return ctx.Status(httpCode).XML(s3error)
}
