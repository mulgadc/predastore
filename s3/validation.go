package s3

import (
	"errors"
	"regexp"
	"strings"
	"unicode/utf8"
)

// IsValidKeyName validates an object key name
func IsValidKeyName(key string) error {

	// Test for a valid UTF-8 character
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-guidelines
	if !utf8.ValidString(key) {
		return errors.New("key must be a valid UTF-8 string")
	}
	return nil
}

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

	// Bucket names must not end with the suffix -s3alias. This suffix is reserved for access point alias names
	if strings.HasSuffix(bucket, "-s3alias") {
		return errors.New("bucket names must not end with the suffix -s3alias")
	}

	// Bucket names must not end with the suffix --ol-s3. This suffix is reserved for Object Lambda Access Point alias names.
	if strings.HasSuffix(bucket, "--ol-s3") {
		return errors.New("bucket names must not end with the suffix --ol-s3")
	}

	// Bucket names must not end with the suffix .mrap. This suffix is reserved for Multi-Region Access Point names.
	if strings.HasSuffix(bucket, ".mrap") {
		return errors.New("bucket names must not end with the suffix .mrap")
	}

	// Bucket names must not end with the suffix --x-s3. This suffix is reserved for directory buckets
	if strings.HasSuffix(bucket, "--x-s3") {
		return errors.New("bucket names must not end with the suffix --x-s3")
	}

	// Bucket names must not end with the suffix --table-s3. This suffix is reserved for S3 Tables buckets.
	if strings.HasSuffix(bucket, "--table-s3") {
		return errors.New("bucket names must not end with the suffix --table-s3")
	}

	// Buckets used with Amazon S3 Transfer Acceleration can't have periods (.) in their names
	// Not applicable to us (yet)
	// if strings.Contains(bucket, ".") {
	// 	return errors.New("Bucket names used with Amazon S3 Transfer Acceleration can't have periods (.) in their names")
	// }

	// Bucket names must not end with the suffix --s3. This suffix is reserved for S3 on Outposts buckets.

	return nil
}
