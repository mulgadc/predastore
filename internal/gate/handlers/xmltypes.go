package handlers

import (
	"encoding/xml"
	"time"
)

// The XML documents the S3 REST API exchanges. They are wire types: the stored
// forms live in internal/gate/model.

// S3 ListObjects (v2)

type ListObjectsV2_Dir struct {
	Prefix string `xml:"Prefix"`
}

type ListObjectsV2_Contents struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	// ETag is empty, and omitted, for an object whose placement record carries
	// no content digest -- the same objects GetObject and HeadObject omit the
	// header for.
	ETag         string `xml:"ETag,omitempty"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

// ListObjectsV2 answers GET /{bucket}. The cursor fields are omitted when
// empty: a client that sees an empty NextContinuationToken on a truncated
// listing has no way to tell it apart from one it can follow.
type ListObjectsV2 struct {
	XMLName               xml.Name                  `xml:"ListBucketResult"`
	Name                  string                    `xml:"Name"`
	Prefix                string                    `xml:"Prefix"`
	Delimiter             string                    `xml:"Delimiter,omitempty"`
	KeyCount              int                       `xml:"KeyCount"`
	MaxKeys               int                       `xml:"MaxKeys"`
	IsTruncated           bool                      `xml:"IsTruncated"`
	ContinuationToken     string                    `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string                    `xml:"NextContinuationToken,omitempty"`
	StartAfter            string                    `xml:"StartAfter,omitempty"`
	Contents              *[]ListObjectsV2_Contents `xml:"Contents"`
	CommonPrefixes        *[]ListObjectsV2_Dir      `xml:"CommonPrefixes"`
}

// ListObjectsV1 answers GET /{bucket} when list-type is absent or not "2".
// NextMarker is carried only when the listing is truncated and a delimiter is
// in play, per the S3 spec: without a delimiter a client pages from the last
// key in Contents instead, so there is nothing to name it with.
type ListObjectsV1 struct {
	XMLName        xml.Name                  `xml:"ListBucketResult"`
	Name           string                    `xml:"Name"`
	Prefix         string                    `xml:"Prefix"`
	Marker         string                    `xml:"Marker"`
	NextMarker     string                    `xml:"NextMarker,omitempty"`
	Delimiter      string                    `xml:"Delimiter,omitempty"`
	MaxKeys        int                       `xml:"MaxKeys"`
	IsTruncated    bool                      `xml:"IsTruncated"`
	Contents       *[]ListObjectsV2_Contents `xml:"Contents"`
	CommonPrefixes *[]ListObjectsV2_Dir      `xml:"CommonPrefixes"`
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
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type ListBucket struct {
	CreationDate time.Time `xml:"CreationDate"`
	Name         string    `xml:"Name"`
}

type ListBucketsResult struct {
	XMLName xml.Name     `xml:"ListAllMyBucketsResult"`
	Owner   BucketOwner  `xml:"Owner"`
	Buckets []ListBucket `xml:"Buckets>Bucket"`
}

// MultipartUpload is one in-flight upload in a ListMultipartUploads answer.
type MultipartUpload struct {
	Key       string    `xml:"Key"`
	UploadId  string    `xml:"UploadId"`
	Initiated time.Time `xml:"Initiated"`
}

// ListMultipartUploadsResult answers GET /{bucket}?uploads. The marker fields
// are always empty and IsTruncated always false: the listing is unpaginated,
// and a client that saw a truncation flag it could not follow would silently
// stop short of the uploads it came for.
type ListMultipartUploadsResult struct {
	XMLName            xml.Name          `xml:"ListMultipartUploadsResult"`
	Bucket             string            `xml:"Bucket"`
	KeyMarker          string            `xml:"KeyMarker"`
	UploadIdMarker     string            `xml:"UploadIdMarker"`
	NextKeyMarker      string            `xml:"NextKeyMarker"`
	NextUploadIdMarker string            `xml:"NextUploadIdMarker"`
	MaxUploads         int               `xml:"MaxUploads"`
	IsTruncated        bool              `xml:"IsTruncated"`
	Uploads            []MultipartUpload `xml:"Upload"`
}

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

// ListPartsResult answers GET /{bucket}/{key}?uploadId=X. Clients call this
// before completing an upload to learn which parts the server holds, so an
// empty or missing response makes them send an empty completion.
type ListPartsResult struct {
	XMLName              xml.Name    `xml:"ListPartsResult"`
	Bucket               string      `xml:"Bucket"`
	Key                  string      `xml:"Key"`
	UploadId             string      `xml:"UploadId"`
	StorageClass         string      `xml:"StorageClass"`
	PartNumberMarker     int         `xml:"PartNumberMarker"`
	NextPartNumberMarker int         `xml:"NextPartNumberMarker"`
	MaxParts             int         `xml:"MaxParts"`
	IsTruncated          bool        `xml:"IsTruncated"`
	Parts                []ListPart  `xml:"Part"`
	Initiator            BucketOwner `xml:"Initiator"`
	Owner                BucketOwner `xml:"Owner"`
}

type ListPart struct {
	PartNumber   int       `xml:"PartNumber"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
}

type CompleteMultipartUploadRequest struct {
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

// CopyObjectResult answers PUT /{bucket}/{key} carrying x-amz-copy-source:
// the destination's own ETag and modification time, not the source's.
type CopyObjectResult struct {
	XMLName      xml.Name  `xml:"CopyObjectResult"`
	ETag         string    `xml:"ETag"`
	LastModified time.Time `xml:"LastModified"`
}

// CopyPartResult answers a part copy. The part's ETag arrives in the body
// rather than the header UploadPart sets, and clients read it from there.
type CopyPartResult struct {
	XMLName      xml.Name  `xml:"CopyPartResult"`
	ETag         string    `xml:"ETag"`
	LastModified time.Time `xml:"LastModified"`
}

// S3Error is the error document every failed request returns.
type S3Error struct {
	XMLName    xml.Name `xml:"Error"`
	Code       string   `xml:"Code"`
	Message    string   `xml:"Message"`
	BucketName string   `xml:"BucketName"`
	RequestId  string   `xml:"RequestId"`
	HostId     string   `xml:"HostId"`
}

// DeleteRequest is the body of POST /{bucket}?delete. Quiet asks for the
// deleted keys to be left out of the answer, leaving only the failures.
type DeleteRequest struct {
	XMLName xml.Name              `xml:"Delete"`
	Quiet   bool                  `xml:"Quiet"`
	Objects []DeleteRequestObject `xml:"Object"`
}

type DeleteRequestObject struct {
	Key string `xml:"Key"`
}

// DeleteResult answers POST /{bucket}?delete. A key that could not be deleted
// is reported beside the ones that were, so the outcome is per key and the
// request itself still succeeds.
type DeleteResult struct {
	XMLName xml.Name        `xml:"DeleteResult"`
	Deleted []DeletedObject `xml:"Deleted"`
	Errors  []DeleteError   `xml:"Error"`
}

type DeletedObject struct {
	Key string `xml:"Key"`
}

type DeleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// CreateBucketConfiguration is the request body for CreateBucket.
type CreateBucketConfiguration struct {
	XMLName            xml.Name `xml:"CreateBucketConfiguration"`
	LocationConstraint string   `xml:"LocationConstraint"`
}

// CreateBucketResult is the response for CreateBucket.
type CreateBucketResult struct {
	XMLName  xml.Name `xml:"CreateBucketResult"`
	Location string   `xml:"Location"`
}
