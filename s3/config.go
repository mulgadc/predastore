package s3

import (
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/pelletier/go-toml/v2"
)

func (s3 *Config) ReadConfig(filename string) (err error) {

	config, err := os.ReadFile(filename)

	if err != nil {
		errorMsg := fmt.Sprintf("Error reading %s %s", filename, err)
		slog.Warn(errorMsg)
		return errors.New(errorMsg)
	}

	err = toml.Unmarshal(config, &s3)

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
