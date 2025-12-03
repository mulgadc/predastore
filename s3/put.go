package s3

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"

	"github.com/gofiber/fiber/v2"
	"github.com/mulgadc/predastore/s3/chunked"
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

	// Determine chunked, checksums and method to store
	/*
		content-encoding:aws-chunked
		content-type:text/markdown
		host:127.0.0.1:8443
		transfer-encoding:chunked
		x-amz-content-sha256:STREAMING-UNSIGNED-PAYLOAD-TRAILER
		x-amz-date:20251128T115922Z
		x-amz-decoded-content-length:26927
		x-amz-sdk-checksum-algorithm:CRC64NVME
		x-amz-trailer:x-amz-checksum-crc64nvme
	*/

	slog.Info("Content-encoding", "content-encoding", c.Get("content-encoding"))

	// Setup a reader (using RequestBodySteam, fallback to Body otherwise)
	reader := chunked.RequestBodyReader(c)

	if c.Get("content-encoding") == "aws-chunked" {

		slog.Debug("Detected chunked upload with encoding", "checksum", c.Get("x-amz-sdk-checksum-algorithm"))

		decodedLenStr := c.Get("x-amz-decoded-content-length")
		var decodedLen int64 = 0
		if decodedLenStr != "" {
			decodedLen, err = strconv.ParseInt(decodedLenStr, 10, 64)
			if err != nil {
				slog.Warn("Error parsing x-amz-decoded-content-length", "error", err)
				return err
			}
		}

		slog.Debug("Using chunked decoder", "decodedLen", decodedLen)

		chunkedDecoder := chunked.NewDecoder(reader, decodedLen)

		// Write the file
		// Stream decoded payload to disk
		if _, err := io.Copy(fileio, chunkedDecoder); err != nil {
			slog.Warn("Error writing chunked file", "error", err)
			return err
		}

		// Validate checksum
		// TODO

	} else {

		// Write the file, use io.Copy for improved performance vs c.Body()
		if _, err := io.Copy(fileio, reader); err != nil {
			slog.Warn("Error writing chunked file", "error", err)
			return err
		}

	}

	return nil
}
