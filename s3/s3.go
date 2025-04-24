package s3

import (
	"encoding/xml"
	"time"
)

type ACL struct {
	AccessKeyId     string `toml:"access_key_id"`
	SecretAccessKey string `toml:"secret_access_key`
	Owner           string `toml:"owner`
	Permissions     int    `toml:"permissions`
}

type S3_Buckets struct {
	Name       string `toml:"name"`
	Region     string `toml:"region"`
	Type       string `toml:"type"`
	Pathname   string `toml:"pathname"`
	Public     bool   `toml:"public"`
	Encryption string `toml:"encryption"`
}

type Config struct {
	Version               string       `toml:"version"`
	Region                string       `toml:"region"`
	Buckets               []S3_Buckets `toml:"buckets"`
	Auth                  []AuthEntry  `toml:"auth"`
	AllowAnonymousListing bool         `toml:"allow_anonymous_listing"`
	AllowAnonymousAccess  bool         `toml:"allow_anonymous_access"`
}

// Authentication and policy
type AuthEntry struct {
	AccessKeyID     string       `toml:"access_key_id"`
	SecretAccessKey string       `toml:"secret_access_key"`
	Policy          []PolicyRule `toml:"policy"`
}

// Action						Meaning
// s3:ListBucket				List objects in a bucket
// s3:GetObject					Download (read) an object
// s3:PutObject					Upload or overwrite an object
// s3:DeleteObject				Delete an object
// s3:ListAllMyBuckets			List all buckets visible to a user
// s3:GetBucketAcl / PutAcl		(TODO) Manage access control for buckets

type PolicyRule struct {
	Bucket  string   `toml:"bucket"`  // Can be "*" or bucket name
	Actions []string `toml:"actions"` // Like "s3:GetObject", "s3:PutObject", or "s3:*"
}

// S3 ListObjects (v2)

type ListObjectsV2_Dir struct {
	Prefix string
}

type ListObjectsV2_Contents struct {
	Key          string
	LastModified time.Time
	ETag         string
	Size         int64
	StorageClass string
}

type ListObjectsV2 struct {
	XMLName        xml.Name `xml:"ListBucketResult"`
	Name           string
	Prefix         string
	KeyCount       int
	MaxKeys        int
	IsTruncated    bool
	Contents       *[]ListObjectsV2_Contents
	CommonPrefixes *[]ListObjectsV2_Dir
}

/*
<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult>
   <Buckets>
      <Bucket>
         <CreationDate>timestamp</CreationDate>
         <Name>string</Name>
      </Bucket>
   </Buckets>
   <Owner>
      <DisplayName>string</DisplayName>
      <ID>string</ID>
   </Owner>
</ListAllMyBucketsResult>
*/

type BucketOwner struct {
	ID          string
	DisplayName string
}

type ListBucket struct {
	CreationDate time.Time
	Name         string
}

type ListBuckets struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Owner   BucketOwner
	Buckets []ListBucket `xml:"Buckets>Bucket"`
}

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type CompleteMultipartUpload struct {
	XMLName xml.Name              `xml:"CompleteMultipartUpload"`
	Parts   []MultipartUploadPart `xml:"Part"`
}

type MultipartUploadPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUpload"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
	// Both of these are optional
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
	ChecksumType      string `xml:"ChecksumType,omitempty"`
}

type S3Error struct {
	XMLName    xml.Name `xml:"Error"`
	Code       string   `xml:"Code"`
	Message    string   `xml:"Message"`
	BucketName string   `xml:"BucketName"`
	RequestId  string   `xml:"RequestId"`
	HostId     string   `xml:"HostId"`
}

func New() *Config {

	return &Config{
		AllowAnonymousListing: false, // Default to not allow anonymous listing
		AllowAnonymousAccess:  false, // Default to not allow anonymous access
	}
}
