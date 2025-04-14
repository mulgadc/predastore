package main

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
func main() {

	sess, err := session.NewSession(
		aws.NewConfig().WithRegion("us-east-1").WithS3ForcePathStyle(true).WithEndpoint("https://localhost").WithHTTPClient(&http.Client{

			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},

				DisableCompression: true,
			},
		}),
	)

	// Create S3 service client
	svc := s3.New(sess)

	result, err := svc.ListBuckets(nil)
	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}

	fmt.Println("Buckets:")

	for _, b := range result.Buckets {
		fmt.Printf("* %s created on %s\n",
			aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	}

	bucket := "commoncrawl"

	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", bucket, err)
	}

	for _, item := range resp.Contents {
		fmt.Println("Name:         ", *item.Key)
		fmt.Println("Last modified:", *item.LastModified)
		fmt.Println("Size:         ", *item.Size)
		fmt.Println("Storage class:", *item.StorageClass)
		fmt.Println("")
	}

	// GET object, with io.WriterAt
	key := "CC-MAIN-20240303111005-20240303141005-00899.warc.wat.gz"

	//fooFile, err := os.OpenFile(fmt.Sprintf("/tmp/%s", key), os.O_CREATE|os.O_WRONLY, 0644)
	buf := aws.NewWriteAtBuffer([]byte{})

	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	downloader := s3manager.NewDownloader(sess)

	numBytes, err := downloader.Download(buf,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		exitErrorf("Unable to download item %q, %v", key, err)
	}

	reader := bytes.NewReader(buf.Bytes())

	// Create a gzip reader from the bytes.Reader
	gzipReader, err := gzip.NewReader(reader)
	if err != nil {
		log.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer gzipReader.Close()

	buf2 := make([]byte, 4096)
	fmt.Println(gzipReader.Read(buf2))

	fmt.Println(buf2)

	fmt.Println("Downloaded", numBytes, "bytes")

}
