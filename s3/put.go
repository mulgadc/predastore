package s3

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/gofiber/fiber/v2"
)

func (s3 *Config) PutObject(bucket string, file string, c *fiber.Ctx) error {

	bucket_config, err := s3.BucketConfig(bucket)

	if err != nil {
		return errors.New("Could not find bucket")
	}

	// Confirm directory exists
	_, err = os.Stat(bucket_config.Pathname)

	if err != nil {
		slog.Warn("Error reading config file", "path", bucket_config.Pathname, "error", err)
		return err
	}

	pathname := fmt.Sprintf("%s/%s", bucket_config.Pathname, file)

	// Open the file
	fileio, err := os.Create(pathname)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}

	defer fileio.Close()

	// Write the file
	_, err = fileio.Write(c.Body())

	if err != nil {
		slog.Warn("Error reading config file", "error", err)
		return err
	}

	return nil
}
