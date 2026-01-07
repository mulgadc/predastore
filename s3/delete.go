package s3

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/gofiber/fiber/v2"
)

func (s3 *Config) DeleteObject(bucket string, file string, c *fiber.Ctx) error {

	slog.Info("Deleting object", "bucket", bucket, "file", file)
	bucket_config, err := s3.BucketConfig(bucket)

	if err != nil {
		return errors.New("NoSuchBucket")
	}

	// Validate the key name
	err = IsValidKeyName(file)
	if err != nil {
		return errors.New("InvalidKey")
	}

	pathname := fmt.Sprintf("%s/%s", bucket_config.Pathname, file)

	_, err = os.Stat(pathname)

	if err != nil {
		return errors.New("NoSuchObject")
	}

	err = os.Remove(pathname)

	if err != nil {
		slog.Error("Error deleting object", "error", err)
		return errors.New("InternalError")
	}

	// Delete the directory and parent directories if they are empty
	// Traverse up the directory tree and remove empty directories until the bucket directory is reached
	err = deleteEmptyParentDirs(pathname, bucket_config.Pathname)

	if err != nil {
		slog.Error("Error deleting empty parent directories", "error", err)
		return errors.New("InternalError")
	}

	c.Status(204)

	return nil

}

func deleteEmptyParentDirs(path, stopAt string) error {
	// Clean paths to ensure consistent format
	path = filepath.Clean(path)
	stopAt = filepath.Clean(stopAt)

	// Get directory of the file
	dir := filepath.Dir(path)

	// While we haven't reached the stop directory
	for dir != stopAt && strings.HasPrefix(dir, stopAt) {
		slog.Info("Checking purge directory", "dir", dir)
		// Read directory contents
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}

		// If directory is not empty, stop
		if len(entries) > 0 {
			slog.Info("Directory is not empty, stopping purge", "dir", dir)
			break
		}

		// Remove empty directory
		if err := os.Remove(dir); err != nil {
			return err
		}

		// Move up to parent
		dir = filepath.Dir(dir)
		slog.Info("Moved up to parent directory", "dir", dir)
	}

	return nil
}
