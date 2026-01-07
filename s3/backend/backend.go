package backend

import (
	"fmt"
	"io"

	"github.com/gofiber/fiber/v2"
	"github.com/mulgadc/predastore/s3/distributed"
	"github.com/mulgadc/predastore/s3/filesystem"
)

type Backend interface {
	Delete(bucket string, file string, c *fiber.Ctx) error

	GetObjectHead(bucket string, file string, c *fiber.Ctx) error
	Get(bucket string, file string, out io.Writer, c *fiber.Ctx) error

	ListBuckets(c *fiber.Ctx) error
	ListObjectsV2Handler(bucket string, c *fiber.Ctx) error

	CompleteMultipartUpload(bucket string, file string, uploadId string, c *fiber.Ctx) error
	PutObjectPart(bucket string, file string, partNumber int, uploadId string, c *fiber.Ctx)

	PutObject(bucket string, file string, c *fiber.Ctx) error
}

func New(btype string, config interface{}) (Backend, error) {

	switch btype {

	case "filesystem":
		return filesystem.New(config)

	case "distributed":
		return distributed.New(config)

	}

	return nil, fmt.Errorf("unknown service type: %s", btype)
}
