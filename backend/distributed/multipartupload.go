package distributed

import (
	"github.com/gofiber/fiber/v2"
)

func (backend Backend) CreateMultipartUpload(bucket string, object string, c *fiber.Ctx) error {
	// TODO: Implement multipart upload for distributed backend
	return nil
}

func (backend Backend) CompleteMultipartUpload(bucket string, object string, uploadId string, c *fiber.Ctx) error {
	// TODO: Implement multipart upload completion for distributed backend
	return nil
}
