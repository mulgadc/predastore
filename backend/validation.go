package backend

import (
	"errors"
	"regexp"
	"strings"
)

// IsValidBucketName validates a bucket name according to S3 naming rules
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
func IsValidBucketName(bucket string) error {
	if len(bucket) < 3 {
		return errors.New("bucket must be > 3 characters")
	}

	if len(bucket) > 63 {
		return errors.New("bucket must be < 63 characters")
	}

	// AWS bucket naming rules
	// Bucket names must begin and end with a letter or number.
	// Bucket names can consist only of lowercase letters, numbers, periods (.), and hyphens (-).
	validBucket := regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*[a-z0-9]$`)
	if !validBucket.MatchString(bucket) {
		return errors.New("bucket name must consist of lowercase letters, numbers, periods (.), and hyphens (-) and must begin and end with a letter or number")
	}

	// Bucket names must not contain two adjacent periods.
	if strings.Contains(bucket, "..") {
		return errors.New("bucket names must not contain two adjacent periods")
	}

	// Bucket names must not be formatted as an IP address (for example, 192.168.5.4).
	if regexp.MustCompile(`^\d+\.\d+\.\d+\.\d+$`).MatchString(bucket) {
		return errors.New("bucket names must not be formatted as an IP address")
	}

	// Bucket names must not start with the prefix xn--.
	if strings.HasPrefix(bucket, "xn--") {
		return errors.New("bucket names must not start with the prefix xn--")
	}

	// Bucket names must not start with the prefix sthree-.
	if strings.HasPrefix(bucket, "sthree-") {
		return errors.New("bucket names must not start with the prefix sthree-")
	}

	// Bucket names must not start with the prefix amzn-s3-demo-
	if strings.HasPrefix(bucket, "amzn-s3-demo-") {
		return errors.New("bucket names must not start with the prefix amzn-s3-demo-")
	}

	// Bucket names must not end with the suffix -s3alias.
	if strings.HasSuffix(bucket, "-s3alias") {
		return errors.New("bucket names must not end with the suffix -s3alias")
	}

	// Bucket names must not end with the suffix --ol-s3.
	if strings.HasSuffix(bucket, "--ol-s3") {
		return errors.New("bucket names must not end with the suffix --ol-s3")
	}

	// Bucket names must not end with the suffix .mrap.
	if strings.HasSuffix(bucket, ".mrap") {
		return errors.New("bucket names must not end with the suffix .mrap")
	}

	// Bucket names must not end with the suffix --x-s3.
	if strings.HasSuffix(bucket, "--x-s3") {
		return errors.New("bucket names must not end with the suffix --x-s3")
	}

	// Bucket names must not end with the suffix --table-s3.
	if strings.HasSuffix(bucket, "--table-s3") {
		return errors.New("bucket names must not end with the suffix --table-s3")
	}

	return nil
}
