package s3

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/gofiber/fiber/v2"
)

func (s3 *Config) PutObject(bucket string, file string, c *fiber.Ctx) error {

	bucket_config, err := s3.BucketConfig(bucket)

	if err != nil {
		return errors.New("NoSuchBucket")
	}

	// Validate the key name
	err = isValidKeyName(file)
	if err != nil {
		return errors.New("InvalidKey")
	}

	// Confirm directory exists
	_, err = os.Stat(bucket_config.Pathname)

	if err != nil {
		slog.Warn("Error reading config file", "path", bucket_config.Pathname, "error", err)
		return err
	}

	// Check if this is a multi-part upload, the params are a GET

	slog.Debug("PutObject", "partNumber", c.Query("partNumber"), "uploadId", c.Query("uploadId"))

	if c.Query("partNumber") != "" && c.Query("uploadId") != "" {

		// TODO Confirm catching part number and uploadId errors
		partNumber, err := strconv.Atoi(c.Query("partNumber"))
		if err != nil {
			return errors.New("InvalidPart")
		}

		return s3.PutObjectPart(bucket, file, partNumber, c.Query("uploadId"), c)
	}

	pathname := fmt.Sprintf("%s/%s", bucket_config.Pathname, file)

	// Get the base name of the file
	baseName := filepath.Base(pathname)

	slog.Info("Base name", "baseName", baseName)
	slog.Info("Pathname", "pathname", filepath.Dir(pathname))

	// Create the directories if they don't exist
	err = os.MkdirAll(filepath.Dir(pathname), 0755)
	if err != nil {
		slog.Error("Error creating directories", "error", err)
		return err
	}

	// Open the file
	fileio, err := os.Create(pathname)
	if err != nil {
		slog.Error("Error opening file", "error", err)
		return err
	}

	defer fileio.Close()

	// Write the file
	_, err = fileio.Write(c.Body())

	if err != nil {
		slog.Warn("Error writing file", "error", err)
		return err
	}

	return nil
}
