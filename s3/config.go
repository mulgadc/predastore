package s3

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml/v2"
)

func (s3 *Config) ReadConfig() (err error) {

	if !filepath.IsAbs(s3.BasePath) {
		dir, err := os.Getwd()
		if err != nil {
			slog.Warn("Error getting working directory", "error", err)
			return err
		}
		s3.BasePath = filepath.Join(dir, s3.BasePath)
	}

	config, err := os.ReadFile(s3.ConfigPath)

	if err != nil {
		errorMsg := fmt.Sprintf("Error reading %s %s", s3.ConfigPath, err)
		slog.Warn("Error reading config file", "error", errorMsg)
		return errors.New(errorMsg)
	}

	err = toml.Unmarshal(config, &s3)

	// Loop through the buckets, if a directory is relative, add the base path
	for k, b := range s3.Buckets {

		// Check if the bucket name is valid
		err = isValidBucketName(b.Name)
		if err != nil {
			slog.Warn("Invalid bucket name", "bucket", b.Name, "error", err)
			return fmt.Errorf("invalid bucket name: %s", err)
		}

		// Check if the directory is relative
		if b.Type == "fs" && !filepath.IsAbs(b.Pathname) {
			s3.Buckets[k].Pathname = filepath.Join(s3.BasePath, b.Pathname)
		}

		// Check if the directory exists, otherwise create it
		if _, err := os.Stat(s3.Buckets[k].Pathname); os.IsNotExist(err) {
			os.MkdirAll(s3.Buckets[k].Pathname, 0750)
		}
	}

	if err != nil {
		errorMsg := fmt.Sprintf("Error parsing %s %s", s3.ConfigPath, err)
		slog.Warn("Error parsing config file", "error", errorMsg)
		return errors.New(errorMsg)
	}

	return nil
}

func (s3 *Config) BucketConfig(bucket string) (S3_Buckets, error) {

	for _, b := range s3.Buckets {
		if b.Name == bucket {
			return b, nil
		}
	}

	return S3_Buckets{}, errors.New("Bucket not found")
}
