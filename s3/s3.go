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

type RS struct {
	Data   int `toml:"data"`
	Parity int `toml:"parity"`
}

type Nodes struct {
	ID     int    `toml:"id"`
	Host   string `toml:"host"`
	Port   int    `toml:"port"`
	Path   string `toml:"path"`
	DB     bool   `toml:"db"`
	DBPort int    `toml:"dbport"`
	DBPath string `toml:"dbpath"`
	Leader bool   `toml:"leader"`
	Epoch  int    `toml:"epoch"`
}

// DBNode represents a distributed database node configuration
type DBNode struct {
	ID              int    `toml:"id"`
	Host            string `toml:"host"`
	Port            int    `toml:"port"`
	RaftPort        int    `toml:"raft_port"` // Port for Raft consensus (default: Port + 1000)
	Path            string `toml:"path"`
	AccessKeyID     string `toml:"access_key_id"`
	SecretAccessKey string `toml:"secret_access_key"`
	Leader          bool   `toml:"leader"`
	Epoch           int    `toml:"epoch"`
}

type Config struct {
	ConfigPath string // Path to config file
	Version    string `toml:"version"`
	Region     string `toml:"region"`

	RS    RS      `toml:"rs"`
	Nodes []Nodes `toml:"nodes"`

	// Distributed database nodes for global state
	DB []DBNode `toml:"db"`

	Buckets []S3_Buckets `toml:"buckets"`

	// TODO: Move to IAM
	Auth                  []AuthEntry `toml:"auth"`
	AllowAnonymousListing bool        `toml:"allow_anonymous_listing"`
	AllowAnonymousAccess  bool        `toml:"allow_anonymous_access"`

	// Only needed on local (filesystem)
	Port int    `toml:"port"`
	Host string `toml:"host"`

	Debug          bool   `toml:"debug"`
	BasePath       string `toml:"base_path"`
	DisableLogging bool   `toml:"disable_logging"`

	// Distributed backend config (deprecated - use DB nodes instead)
	BadgerDir string `toml:"badger_dir"`
}

// Authentication and policy
type AuthEntry struct {
	AccessKeyID     string       `toml:"access_key_id"`
	SecretAccessKey string       `toml:"secret_access_key"`
	AccountID       string       `toml:"account_id"`
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

// CreateBucketConfiguration is the request body for CreateBucket
type CreateBucketConfiguration struct {
	XMLName            xml.Name `xml:"CreateBucketConfiguration"`
	LocationConstraint string   `xml:"LocationConstraint"`
}

// CreateBucketResult is the response for CreateBucket
type CreateBucketResult struct {
	XMLName  xml.Name `xml:"CreateBucketResult"`
	Location string   `xml:"Location"`
}

// Context key for storing authenticated user info
type contextKey string

const (
	// ContextKeyAccessKeyID is the context key for the authenticated user's access key ID
	ContextKeyAccessKeyID contextKey = "accessKeyID"
	// ContextKeyAccountID is the context key for the authenticated user's account ID
	ContextKeyAccountID contextKey = "accountID"
)

func New(ConfigSettings *Config) *Config {
	return ConfigSettings
}
