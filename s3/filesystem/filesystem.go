package filesystem

import (
	"io"

	"github.com/gofiber/fiber/v2"
	"github.com/mulgadc/predastore/s3"
)

type Backend struct {
	Config s3.Config
}

func New(config interface{}) (svc *Backend, err error) {
	svc = &Backend{
		Config: s3.Config{},
	}

	return svc, nil
}

func (backend Backend) Delete(bucket string, file string, c *fiber.Ctx) (err error) {

	return

}

func (backend Backend) GetObjectHead(bucket string, file string, c *fiber.Ctx) (err error) {

	return

}

func (backend Backend) Get(bucket string, file string, out io.Writer, c *fiber.Ctx) (err error) {

	return
}

func (backend Backend) ListBuckets(c *fiber.Ctx) (err error) {

	return

}

func (backend Backend) ListObjectsV2Handler(bucket string, c *fiber.Ctx) (err error) {

	return

}

func (backend Backend) CompleteMultipartUpload(bucket string, file string, uploadId string, c *fiber.Ctx) (err error) {

	return

}

func (backend Backend) PutObjectPart(bucket string, file string, partNumber int, uploadId string, c *fiber.Ctx) {

}

func (backend Backend) PutObject(bucket string, file string, c *fiber.Ctx) (err error) {

	return

}
