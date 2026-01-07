package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/google/uuid"
	"github.com/mulgadc/predastore/backend"
	"github.com/mulgadc/predastore/s3/chunked"
)

func (s3 *Config) SetupRoutes() *fiber.App {

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

		return s3.ListBuckets(c)
	})

	// ListObjectsV2
	app.Get(`/:bucket<regex([a-z0-9-.]+)>`, func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")

		return s3.ListObjectsV2Handler(bucket, c)
	})

	// GetObject (HEAD)
	app.Head(`/:bucket<regex([a-z0-9-.]+)>/*`, func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")
		file := c.Params("*")

		return s3.GetObjectHead(bucket, file, c)
	})

	// GetObject (GET, BODY)
	app.Get(`/:bucket<regex([a-z0-9-.]+)>/*`, func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")
		file := c.Params("*")

		return s3.GetObject(bucket, file, c)
	})

	// PutObject (PUT)
	app.Put(`/:bucket<regex([a-z0-9-.]+)>/*`, func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")
		file := c.Params("*")

		return s3.PutObject(bucket, file, c)
	})

	app.Post(`/:bucket<regex([a-z0-9-.]+)>/*`, func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")
		file := c.Params("*")

		// Confirm if posting a multipart upload, or complete a multipart upload
		if c.Query("uploadId") == "" {
			return s3.CreateMultipartUpload(bucket, file, c)
		} else {
			return s3.CompleteMultipartUpload(bucket, file, c.Query("uploadId"), c)
		}
	})

	// DeleteObject
	app.Delete(`/:bucket<regex([a-z0-9-.]+)>/*`, func(c *fiber.Ctx) error {
		bucket := c.Params("bucket")
		file := c.Params("*")

		fmt.Println("Deleting object", bucket, file)

		return s3.DeleteObject(bucket, file, c)

	})

	return app
}

func (s3 *Config) ErrorHandler(ctx *fiber.Ctx, err error) error {
	// Status code defaults to 500
	httpCode := fiber.StatusInternalServerError
	var s3error S3Error
	var e *fiber.Error

	// Check for typed backend errors first
	if backendErr, ok := backend.IsS3Error(err); ok {
		httpCode = backendErr.StatusCode
		s3error.Code = string(backendErr.Code)
		s3error.Message = backendErr.Message
	} else {
		// Fallback to string matching for legacy code
		switch {
		case strings.Contains(err.Error(), "NoSuchBucket") || strings.Contains(err.Error(), "Bucket not found"):
			httpCode = fiber.StatusNotFound
			s3error.Code = "NoSuchBucket"
			s3error.Message = "The specified bucket does not exist"

		case strings.Contains(err.Error(), "AccessDenied") || strings.Contains(err.Error(), "Not enough permissions"):
			httpCode = fiber.StatusForbidden
			s3error.Code = "AccessDenied"
			s3error.Message = "Access Denied"

		case strings.Contains(err.Error(), "NoSuchObject") || strings.Contains(err.Error(), "NoSuchKey") ||
			strings.Contains(err.Error(), "not found") || errors.Is(err, os.ErrNotExist):
			httpCode = fiber.StatusNotFound
			s3error.Code = "NoSuchKey"
			s3error.Message = "The specified key does not exist"

		case strings.Contains(err.Error(), "Invalid signature") || strings.Contains(err.Error(), "Invalid access key"):
			httpCode = fiber.StatusForbidden
			s3error.Code = "SignatureDoesNotMatch"
			s3error.Message = "The request signature does not match"

		case strings.Contains(err.Error(), "Missing Authorization header"):
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
	}

	// Add request ID and host ID
	s3error.RequestId = ctx.GetRespHeader("x-amz-request-id", uuid.NewString())
	s3error.HostId = ctx.Hostname()

	// Set standard S3 error response headers
	ctx.Set("Content-Type", "application/xml")

	return ctx.Status(httpCode).XML(s3error)
}

// SetupRoutesWithBackend creates a Fiber app with the provided backend
// This is the recommended way to setup routes with the new backend architecture
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
	slog.SetDefault(slog.New(handler))

	streamRequestBodyEnv := os.Getenv("StreamRequestBody")
	streamRequestBody := streamRequestBodyEnv != "false"

	app := fiber.New(fiber.Config{
		DisableStartupMessage: s3.DisableLogging,
		BodyLimit:             5 * 1024 * 1024 * 1024, // 5GiB
		StreamRequestBody:     streamRequestBody,
		ErrorHandler:          s3.ErrorHandler,
	})

	if !s3.DisableLogging {
		app.Use(logger.New())
	}

	app.Use(s3.SigV4AuthMiddleware)

	// List buckets
	app.Get("/", func(c *fiber.Ctx) error {
		ctx := context.Background()
		resp, err := be.ListBuckets(ctx)
		if err != nil {
			return err
		}

		result := ListBuckets{
			Owner: BucketOwner{
				ID:          resp.Owner.ID,
				DisplayName: resp.Owner.DisplayName,
			},
		}
		for _, b := range resp.Buckets {
			result.Buckets = append(result.Buckets, ListBucket{
				Name:         b.Name,
				CreationDate: b.CreationDate,
			})
		}
		return c.XML(result)
	})

	// ListObjectsV2
	app.Get(`/:bucket<regex([a-z0-9-.]+)>`, func(c *fiber.Ctx) error {
		ctx := context.Background()
		bucket := c.Params("bucket")
		queries := c.Queries()

		resp, err := be.ListObjects(ctx, &backend.ListObjectsRequest{
			Bucket:    bucket,
			Prefix:    queries["prefix"],
			Delimiter: queries["delimiter"],
		})
		if err != nil {
			return err
		}

		contents := make([]ListObjectsV2_Contents, 0, len(resp.Contents))
		for _, obj := range resp.Contents {
			contents = append(contents, ListObjectsV2_Contents{
				Key:          obj.Key,
				LastModified: obj.LastModified,
				ETag:         obj.ETag,
				Size:         obj.Size,
				StorageClass: obj.StorageClass,
			})
		}

		prefixes := make([]ListObjectsV2_Dir, 0, len(resp.CommonPrefixes))
		for _, p := range resp.CommonPrefixes {
			prefixes = append(prefixes, ListObjectsV2_Dir{Prefix: p})
		}

		result := ListObjectsV2{
			Name:           resp.Name,
			Prefix:         resp.Prefix,
			KeyCount:       resp.KeyCount,
			MaxKeys:        resp.MaxKeys,
			IsTruncated:    resp.IsTruncated,
			Contents:       &contents,
			CommonPrefixes: &prefixes,
		}
		return c.XML(result)
	})

	// HeadObject
	app.Head(`/:bucket<regex([a-z0-9-.]+)>/*`, func(c *fiber.Ctx) error {
		ctx := context.Background()
		bucket := c.Params("bucket")
		key := c.Params("*")

		resp, err := be.HeadObject(ctx, bucket, key)
		if err != nil {
			return err
		}

		c.Set("Content-Type", resp.ContentType)
		c.Set("Content-Length", fmt.Sprintf("%d", resp.ContentLength))
		c.Set("ETag", resp.ETag)
		c.Set("Last-Modified", resp.LastModified.Format("Mon, 02 Jan 2006 15:04:05 GMT"))
		return c.SendString("")
	})

	// GetObject
	app.Get(`/:bucket<regex([a-z0-9-.]+)>/*`, func(c *fiber.Ctx) error {
		ctx := context.Background()
		bucket := c.Params("bucket")
		key := c.Params("*")

		req := &backend.GetObjectRequest{
			Bucket:     bucket,
			Key:        key,
			RangeStart: -1,
			RangeEnd:   -1,
		}

		// Parse Range header
		if rangeHeader := c.Get("Range"); rangeHeader != "" {
			if strings.HasPrefix(rangeHeader, "bytes=") {
				rangeSpec := rangeHeader[6:]
				if idx := strings.Index(rangeSpec, "-"); idx >= 0 {
					if idx > 0 {
						start, _ := strconv.ParseInt(rangeSpec[:idx], 10, 64)
						req.RangeStart = start
					}
					if idx < len(rangeSpec)-1 {
						end, _ := strconv.ParseInt(rangeSpec[idx+1:], 10, 64)
						req.RangeEnd = end
					}
				}
			}
		}

		resp, err := be.GetObject(ctx, req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		c.Set("Content-Type", resp.ContentType)
		c.Set("Content-Length", fmt.Sprintf("%d", resp.Size))
		c.Set("ETag", resp.ETag)

		if resp.StatusCode == 206 {
			c.Set("Content-Range", resp.ContentRange)
			c.Status(206)
		}

		// Stream the response
		_, err = io.Copy(c, resp.Body)
		return err
	})

	// PutObject
	app.Put(`/:bucket<regex([a-z0-9-.]+)>/*`, func(c *fiber.Ctx) error {
		ctx := context.Background()
		bucket := c.Params("bucket")
		key := c.Params("*")

		// Check for multipart part upload
		if partNum := c.Query("partNumber"); partNum != "" {
			uploadID := c.Query("uploadId")
			partNumber, _ := strconv.Atoi(partNum)

			decodedLen, _ := strconv.ParseInt(c.Get("x-amz-decoded-content-length"), 10, 64)

			resp, err := be.UploadPart(ctx, &backend.UploadPartRequest{
				Bucket:          bucket,
				Key:             key,
				UploadID:        uploadID,
				PartNumber:      partNumber,
				Body:            chunked.RequestBodyReader(c),
				ContentEncoding: c.Get("content-encoding"),
				IsChunked:       c.Get("content-encoding") == "aws-chunked",
				DecodedLength:   decodedLen,
			})
			if err != nil {
				return err
			}

			c.Set("ETag", resp.ETag)
			c.Set("x-amz-server-side-encryption", "AES256")
			return nil
		}

		// Regular put object
		decodedLen, _ := strconv.ParseInt(c.Get("x-amz-decoded-content-length"), 10, 64)

		resp, err := be.PutObject(ctx, &backend.PutObjectRequest{
			Bucket:          bucket,
			Key:             key,
			Body:            chunked.RequestBodyReader(c),
			ContentEncoding: c.Get("content-encoding"),
			IsChunked:       c.Get("content-encoding") == "aws-chunked",
			DecodedLength:   decodedLen,
		})
		if err != nil {
			return err
		}

		c.Set("ETag", resp.ETag)
		return nil
	})

	// POST - CreateMultipartUpload or CompleteMultipartUpload
	app.Post(`/:bucket<regex([a-z0-9-.]+)>/*`, func(c *fiber.Ctx) error {
		ctx := context.Background()
		bucket := c.Params("bucket")
		key := c.Params("*")

		uploadID := c.Query("uploadId")
		if uploadID == "" {
			// Create multipart upload
			resp, err := be.CreateMultipartUpload(ctx, &backend.CreateMultipartUploadRequest{
				Bucket: bucket,
				Key:    key,
			})
			if err != nil {
				return err
			}

			c.Set("x-amz-server-side-encryption", "AES256")
			return c.XML(InitiateMultipartUploadResult{
				Bucket:   resp.Bucket,
				Key:      resp.Key,
				UploadId: resp.UploadID,
			})
		}

		// Complete multipart upload
		var completeReq CompleteMultipartUpload
		if err := c.BodyParser(&completeReq); err != nil {
			return err
		}

		parts := make([]backend.CompletedPart, len(completeReq.Parts))
		for i, p := range completeReq.Parts {
			parts[i] = backend.CompletedPart{
				PartNumber: p.PartNumber,
				ETag:       p.ETag,
			}
		}

		resp, err := be.CompleteMultipartUpload(ctx, &backend.CompleteMultipartUploadRequest{
			Bucket:   bucket,
			Key:      key,
			UploadID: uploadID,
			Parts:    parts,
		})
		if err != nil {
			return err
		}

		return c.XML(CompleteMultipartUploadResult{
			Location: fmt.Sprintf("https://%s%s", c.Hostname(), resp.Location),
			Bucket:   resp.Bucket,
			Key:      resp.Key,
			ETag:     resp.ETag,
		})
	})

	// DeleteObject
	app.Delete(`/:bucket<regex([a-z0-9-.]+)>/*`, func(c *fiber.Ctx) error {
		ctx := context.Background()
		bucket := c.Params("bucket")
		key := c.Params("*")

		err := be.DeleteObject(ctx, &backend.DeleteObjectRequest{
			Bucket: bucket,
			Key:    key,
		})
		if err != nil {
			return err
		}

		c.Status(204)
		return nil
	})

	return app
}
