package s3

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
)

func (s3 *Config) GetObjectHead(bucket string, file string, c *fiber.Ctx) error {
	bucket_config, err := s3.BucketConfig(bucket)

	if err != nil {
		return errors.New("NoSuchBucket")
	}

	pathname := fmt.Sprintf("%s/%s", bucket_config.Pathname, file)

	info, err := os.Stat(pathname)

	if err != nil {
		return errors.New("NoSuchObject")
	}

	// Open the file
	fileio, err := os.Open(pathname)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}

	defer fileio.Close()

	// Read the first 512 bytes to determine the content type
	buffer := make([]byte, 512)
	_, err = fileio.Read(buffer)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return err
	}

	// Determine the content type
	contentType := http.DetectContentType(buffer)
	fmt.Println("Content Type:", contentType)

	c.Set("Content-Type", contentType)

	c.Set("Content-Length", fmt.Sprintf("%d", info.Size()))
	c.Set("Last-Modified", info.ModTime().Format(time.RFC1123))
	c.Set("Date", time.Now().Format(time.RFC1123))

	// TODO: Improve ETAG (hash of the local file?)
	fileHash := fmt.Sprintf("%s/%s:(%s)", bucket, info.Name(), info.ModTime().String())
	md5Hash := md5.New()
	md5Hash.Write([]byte(fileHash))

	c.Set("ETag", hex.EncodeToString(md5Hash.Sum(nil)))

	return c.SendString("")

}

func (s3 *Config) GetObject(bucket string, file string, c *fiber.Ctx) error {

	bucket_config, err := s3.BucketConfig(bucket)

	if err != nil {
		return errors.New("NoSuchBucket")
	}

	pathname := fmt.Sprintf("%s/%s", bucket_config.Pathname, file)

	finfo, err := os.Stat(pathname)

	if err != nil {
		return errors.New("NoSuchObject")
	}

	//c.SendFile(pathname)

	byteRange := c.Get("Range")

	// If no byte ranges specified, send the entire file.
	if byteRange == "" {

		c.SendFile(pathname, true)

	} else {
		byteRange = byteRange[6:]

		var byteRangeStart, byteRangeEnd string
		var byteRangeStartInt, byteRangeEndInt int64

		byteRangeStart = byteRange[:strings.Index(byteRange, "-")]
		byteRangeEnd = byteRange[strings.Index(byteRange, "-")+1:]

		byteRangeStartInt, err = strconv.ParseInt(byteRangeStart, 10, 64)

		if err != nil {
			fmt.Println("Error parsing range", err)
			return err
		}

		if byteRangeEnd == "" {
			byteRangeEndInt = finfo.Size()
		} else {
			byteRangeEndInt, err = strconv.ParseInt(byteRangeEnd, 10, 64)
			byteRangeEndInt += 1

			if err != nil {
				fmt.Println("Error parsing range", err)
				return err
			}

		}

		if byteRangeEndInt > finfo.Size() {
			fmt.Println("Byte range exceeding file", byteRangeEndInt, finfo.Size())
			byteRangeEndInt = finfo.Size()
		}

		offset := byteRangeEndInt - byteRangeStartInt

		fmt.Println("Range and file check: ", byteRangeEndInt, finfo.Size(), "", byteRangeStartInt, finfo.Size())

		if byteRangeEndInt > finfo.Size()+1 || byteRangeStartInt > finfo.Size()+1 {
			c.Status(416)
			return errors.New("Range not satisfiable")
		}

		fileio, err := os.Open(pathname)

		fmt.Println("Reading from ", byteRangeStartInt)
		fmt.Println("Offset", offset)

		/*
			r, err := fileio.Seek(byteRangeStartInt, 0)

			if err != nil {
				c.Status(500)
				fmt.Println("Error seeking file:", err, r)
			}
		*/

		//c.SendStream(fileio)

		rawFile := make([]byte, offset)
		_, err = fileio.ReadAt(rawFile, byteRangeStartInt)

		if err != nil {
			c.Status(500)
			fmt.Println("Error reading file:", err)
			return err
		}

		/*
		   Accept-Ranges': 'bytes',
		   'Content-Range': 'bytes 75497472-83886079/255103209',
		   'Content-Type': 'application/octet-stream',
		   'Server': 'AmazonS3',
		   'Content-Length': '8388608'
		*/

		c.Status(200)
		c.Set("Accept-Ranges", "bytes")
		c.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", byteRangeStartInt, byteRangeEndInt, finfo.Size()))
		c.Set("Content-Type", http.DetectContentType(rawFile))
		c.Set("Content-Length", fmt.Sprintf("%d", offset+1))
		c.Set("Server", "AmazonS3")

		err = c.Send(rawFile)
		//c.SendStream(fileio, int(offset-1))

		if err != nil {
			c.Status(500)
			fmt.Println("Error reading file:", err)
		}

		fmt.Println("Reading ranges:", byteRangeStartInt, byteRangeEndInt)

	}

	return nil

}
