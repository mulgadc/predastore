package s3

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

func (s3 *Config) CompleteMultipartUpload(bucket string, file string, uploadId string, c *fiber.Ctx) error {

	// TODO: Move validation to common function
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

	// Create the temporary directory for the uploadId
	// Create temporary directory to store parts
	tempDir := os.TempDir()

	// Create a unique directory for the uploadId if it does not exist
	uploadDir := fmt.Sprintf("%s/%s", tempDir, uploadId)

	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		err = os.MkdirAll(uploadDir, 0755)
		if err != nil {
			slog.Warn("Upload directory does not exist", "error", err)
			return errors.New("NoSuchUploadId")
		}
	}

	// Parse the body response
	completeMultipartUpload := CompleteMultipartUpload{}

	//err = c.BodyParser(&completeMultipartUpload)
	err = xml.Unmarshal(c.Body(), &completeMultipartUpload)

	slog.Debug("CompleteMultipartUpload", "completeMultipartUpload", completeMultipartUpload)

	if err != nil {
		slog.Warn("Error parsing body", "error", err)
		return err
	}

	// Confirm each ETAG matches the part number
	for _, part := range completeMultipartUpload.Parts {
		if part.ETag == "" {
			slog.Warn("ETag is empty", "part", part)
			return errors.New("InvalidPart")
		}

		// Read the part file on disk
		partFile := fmt.Sprintf("%s/%s.%d", uploadDir, file, part.PartNumber)
		partStat, err := os.Stat(partFile)

		if os.IsNotExist(err) {
			slog.Warn("Part file does not exist", "part", part)
			return errors.New("InvalidPart")
		}

		// Confirm the filePart >= 5MB, however the last part can be less than 5MB

		if part.PartNumber != len(completeMultipartUpload.Parts) && partStat.Size() < 5*1024*1024 {
			slog.Warn("Part file is less than 5MB", "part", part)
			return errors.New("InvalidPart")
		}

		// Calculate ETAG (MD5) of the part file
		partIo, err := os.Open(partFile)
		if err != nil {
			slog.Warn("Error opening part file", "part", part)
			return errors.New("InvalidPart")
		}

		defer partIo.Close()

		md5Writer := md5.New()

		// Effectively read the file MD5 use io.Reader
		buffer := make([]byte, 32*1024)
		for {
			n, err := partIo.Read(buffer)
			if n > 0 {
				md5Writer.Write(buffer[:n])
			}

			if err == io.EOF {
				break
			}
			if err != nil {
				slog.Warn("Error calculating ETAG", "part", part)
				return errors.New("InvalidPart")

			}
		}

		etag := hex.EncodeToString(md5Writer.Sum(nil))

		if etag != part.ETag {
			slog.Warn("ETag does not match", "part", part)
			//return errors.New("InvalidPart")
		}

	}

	// Create the final file, concatenate all the parts
	pathname := fmt.Sprintf("%s/%s", bucket_config.Pathname, file)

	// Open the file
	fileio, err := os.Create(pathname)
	if err != nil {
		slog.Warn("Error opening file:", "error", err)
		return errors.New("FileOpenError")
	}

	defer fileio.Close()

	// Read each part file and write to the final file
	for _, part := range completeMultipartUpload.Parts {

		partFile := fmt.Sprintf("%s/%s.%d", uploadDir, file, part.PartNumber)
		partIo, err := os.Open(partFile)
		if err != nil {
			slog.Warn("Error opening part file", "part", part)
			return errors.New("InvalidPart")
		}

		// Use a read buffer for efficiency
		buffer := make([]byte, 32*1024)
		for {
			n, err := partIo.Read(buffer)
			if n > 0 {
				fileio.Write(buffer[:n])
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				slog.Warn("Error writing part file", "part", part)
				return errors.New("InvalidPart")
			}

		}

		err = os.Remove(partFile)
		if err != nil {
			slog.Warn("Error cleaning up part file", "part", part)
			return errors.New("InvalidPart")
		}

	}

	// Send the success response
	completeMultipartUploadResult := CompleteMultipartUploadResult{}

	completeMultipartUploadResult.Location = fmt.Sprintf("https://%s/%s/%s", c.Hostname(), bucket, file)
	completeMultipartUploadResult.Bucket = bucket
	completeMultipartUploadResult.Key = file

	// Loop through the parts and calculate the ETag
	partETags := []string{}
	for _, part := range completeMultipartUpload.Parts {
		partETags = append(partETags, part.ETag)
	}

	etag := calculateMultipartETag(partETags, len(completeMultipartUpload.Parts))

	slog.Info("Final ETag", "etag", etag)
	completeMultipartUploadResult.ETag = etag

	return c.XML(completeMultipartUploadResult)

}

func calculateMultipartETag(partETags []string, numParts int) string {
	// Concatenate all part MD5s
	concat := make([]byte, 0, len(partETags)*16) // Each MD5 is 16 bytes

	for _, etag := range partETags {
		// Remove quotes and -1 suffix from part ETags
		cleanETag := strings.Trim(etag, "\"")
		cleanETag = strings.Split(cleanETag, "-")[0]

		// Convert hex to bytes
		md5Bytes, _ := hex.DecodeString(cleanETag)
		concat = append(concat, md5Bytes...)
	}

	// Calculate final MD5
	finalMD5 := md5.Sum(concat)

	// Return in format "md5-numparts"
	return fmt.Sprintf("\"%x-%d\"", finalMD5, numParts)
}

func (s3 *Config) CreateMultipartUpload(bucket string, file string, c *fiber.Ctx) error {

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

	// Create the temporary directory for the uploadId
	// Create temporary directory to store parts
	tempDir := os.TempDir()

	uploadId := uuid.New().String()

	// Create a unique directory for the uploadId if it does not exist
	uploadDir := fmt.Sprintf("%s/%s", tempDir, uploadId)

	slog.Debug("Creating upload directory", "uploadDir", uploadDir)

	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {
		err = os.MkdirAll(uploadDir, 0755)
		if err != nil {
			slog.Warn("Error creating upload directory", "error", err)
			return err
		}
	}

	Resp_InitiateMultipartUpload := InitiateMultipartUploadResult{}

	// TODO: Support other encryption methods
	c.Set("x-amz-server-side-encryption", "AES256")

	Resp_InitiateMultipartUpload.Bucket = bucket
	Resp_InitiateMultipartUpload.Key = file
	Resp_InitiateMultipartUpload.UploadId = uploadId

	return c.XML(Resp_InitiateMultipartUpload)
}

func (s3 *Config) PutObjectPart(bucket string, file string, partNumber int, uploadId string, c *fiber.Ctx) error {

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

	// Create temporary directory to store parts
	tempDir := os.TempDir()

	// Create a unique directory for the uploadId if it does not exist
	uploadDir := fmt.Sprintf("%s/%s", tempDir, uploadId)
	if _, err := os.Stat(uploadDir); os.IsNotExist(err) {

		// The upload-Id does not exist, return an error
		return errors.New("NoSuchUploadId")

	}

	tempFile := fmt.Sprintf("%s/%s.%d", uploadDir, file, partNumber)

	// Open the file
	fileio, err := os.Create(tempFile)
	if err != nil {
		slog.Warn("Error opening file:", "error", err)
		return err
	}

	defer fileio.Close()

	// TODO: Confirm if this belongs in the auth middleware
	//c.Status(fiber.StatusContinue)

	// Use a io.MultiWriter to send the body and calculate the MD5 checksum
	md5Writer := md5.New()
	multiWriter := io.MultiWriter(fileio, md5Writer)

	// TODO: Implement checksum verification and multi-part syntax
	_, err = multiWriter.Write(c.Body())

	if err != nil {
		slog.Warn("Error writing file", "error", err)
		return err
	}

	// Append the MD5 checksum to the ETAG, must be quoted
	etag := fmt.Sprintf("\"%x\"", md5Writer.Sum(nil))

	c.Set("ETag", etag)

	// Base64 encode unique identifier
	id2 := base64.StdEncoding.EncodeToString([]byte(uuid.NewString()))

	// TODO: Improve, use a single function for all headers and in the correct format.
	c.Set("x-amz-id-2", id2)
	c.Set("x-amz-request-id", uuid.NewString())
	c.Set("x-amz-server-side-encryption", "AES256")

	c.Status(fiber.StatusOK)
	//return c.SendStatus(fiber.StatusOK)
	return nil

}
