package model

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

var (
	validBucketNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*[a-z0-9]$`)
	ipAddrPatternRe   = regexp.MustCompile(`^\d+\.\d+\.\d+\.\d+$`)
)

// Bucket is the S3 bucket a request addresses. It carries no metadata: the
// stored record is BucketMetadata, and the config-defined form lives with the
// handlers.
type Bucket struct {
	Name string
}

// Validate applies the S3 bucket naming rules. It is the only definition of a
// legal bucket name, so a name that cannot be created cannot be addressed
// either, and the config rejects its own buckets through this same method.
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
func (b Bucket) Validate() error {
	reject := func(reason string) error {
		return NewS3Error(ErrInvalidBucketName, reason, 400).WithResource(b.Name)
	}

	switch {
	case len(b.Name) < 3:
		return reject("bucket name must be at least 3 characters")
	case len(b.Name) > 63:
		return reject("bucket name must be at most 63 characters")

	// Bucket names must begin and end with a letter or number, and consist only
	// of lowercase letters, numbers, periods (.), and hyphens (-).
	case !validBucketNameRe.MatchString(b.Name):
		return reject("bucket name must consist of lowercase letters, numbers, periods (.), and hyphens (-) and must begin and end with a letter or number")
	case strings.Contains(b.Name, ".."):
		return reject("bucket names must not contain two adjacent periods")
	case ipAddrPatternRe.MatchString(b.Name):
		return reject("bucket names must not be formatted as an IP address")
	}

	// Prefixes and suffixes AWS reserves for access points, directory buckets,
	// S3 Tables and its own examples.
	for _, prefix := range []string{"xn--", "sthree-", "amzn-s3-demo-"} {
		if strings.HasPrefix(b.Name, prefix) {
			return reject("bucket names must not start with the prefix " + prefix)
		}
	}
	for _, suffix := range []string{"-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"} {
		if strings.HasSuffix(b.Name, suffix) {
			return reject("bucket names must not end with the suffix " + suffix)
		}
	}

	return nil
}

// Object is the S3 object a request addresses, as the bucket that holds it and
// the key within that bucket.
type Object struct {
	Bucket Bucket
	Key    string
}

// Validate applies the bucket rules and then the key rules. Keys that only
// normalise into their dispatched form are rejected so no two layers can
// disagree about which object was named.
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
func (o Object) Validate() error {
	if err := o.Bucket.Validate(); err != nil {
		return err
	}

	reject := func(reason string) error {
		return NewS3Error(ErrInvalidKey, reason, 400).WithResource(o.Key)
	}

	switch {
	case o.Key == "":
		return reject("object key is empty")
	case !utf8.ValidString(o.Key):
		return reject("object key must be a valid UTF-8 string")
	case strings.HasSuffix(o.Key, "/"):
		return reject("object key must not end in a slash")
	}

	for segment := range strings.SplitSeq(o.Key, "/") {
		if segment == "." || segment == ".." {
			return reject("object key must not contain a . or .. segment")
		}
	}

	return nil
}
