package backend

import (
	"github.com/gofiber/fiber/v2"
)

// Backend interface defines all storage backend operations
type Backend interface {
	// Object operations
	DeleteObject(bucket string, file string, c *fiber.Ctx) error
	GetObject(bucket string, file string, c *fiber.Ctx) error
	GetObjectHead(bucket string, file string, c *fiber.Ctx) error
	PutObject(bucket string, file string, c *fiber.Ctx) error

	// Bucket operations
	ListBuckets(c *fiber.Ctx) error
	ListObjectsV2Handler(bucket string, c *fiber.Ctx) error

	// Multipart upload operations
	CreateMultipartUpload(bucket string, file string, c *fiber.Ctx) error
	CompleteMultipartUpload(bucket string, file string, uploadId string, c *fiber.Ctx) error
	PutObjectPart(bucket string, file string, partNumber int, uploadId string, c *fiber.Ctx) error
}
