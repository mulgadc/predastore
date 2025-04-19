package s3

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml/v2"
)

func (s3 *Config) ReadConfig(filename string, base_path string) (err error) {

	if !filepath.IsAbs(base_path) {
		dir, err := os.Getwd()
		if err != nil {
			slog.Warn("Error getting working directory", "error", err)
			return err
		}
		base_path = filepath.Join(dir, base_path)
	}

	config, err := os.ReadFile(filename)

	if err != nil {
		errorMsg := fmt.Sprintf("Error reading %s %s", filename, err)
		slog.Warn(errorMsg)
		return errors.New(errorMsg)
	}

	err = toml.Unmarshal(config, &s3)

	// Loop through the buckets, if a directory is relative, add the base path
	for k, b := range s3.Buckets {
		// Check if the directory is relative
		if b.Type == "fs" && !filepath.IsAbs(b.Pathname) {
			s3.Buckets[k].Pathname = filepath.Join(base_path, b.Pathname)
		}
	}

	if err != nil {
		errorMsg := fmt.Sprintf("Error parsing %s %s", filename, err)
		slog.Warn(errorMsg)
		return errors.New(errorMsg)
	}

	return nil
}

func (s3 *Config) BucketConfig(bucket string) (S3_Buckets, error) {

	//fmt.Println("Searching for bucket", bucket)
	//fmt.Println(s3)

	for _, b := range s3.Buckets {
		//fmt.Println("BUCKET: ", b)
		if b.Name == bucket {
			return b, nil
		}
	}

	return S3_Buckets{}, errors.New("Bucket not found")
}
