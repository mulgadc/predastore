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
	ACL        []ACL  `toml:"acl"`
	Encryption string `toml:"encryption"`
}

type Config struct {
	Version string       `toml:"version"`
	Region  string       `toml:"region"`
	Buckets []S3_Buckets `toml:"buckets"`
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

func New() *Config {
	return &Config{}
}
