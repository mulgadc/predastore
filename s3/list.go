package s3

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/gofiber/fiber/v2"
)

func (s3 *Config) ListBuckets(c *fiber.Ctx) error {

	Resp_ListBuckets := ListBuckets{}

	for _, b := range s3.Buckets {

		Resp_ListBucket := ListBucket{}
		Resp_ListBucket.Name = b.Name

		dir, err := os.Stat(b.Pathname)

		if err != nil {
			slog.Warn(fmt.Sprintf("Could not determine bucket path (%s) from config: %s", dir, err))

			break
		}

		Resp_ListBucket.CreationDate = dir.ModTime()
		//time.Unix(0, dir.Sys().(*syscall.Stat_t).Ctimespec.Nsec)

		Resp_ListBuckets.Buckets = append(Resp_ListBuckets.Buckets, Resp_ListBucket)

	}

	Resp_ListBuckets.Owner.DisplayName = "calacode"
	Resp_ListBuckets.Owner.ID = "bd2a0a8327d4925861b181cd5b595ff02809e8bbd21d616f73a6b8ad95caffea"

	return c.XML(Resp_ListBuckets)

}

func (s3 *Config) ListObjectsV2Handler(bucket string, c *fiber.Ctx) error {

	req := c.Queries()
	bucket_config, err := s3.BucketConfig(bucket)

	if err != nil {
		slog.Warn(fmt.Sprintf("Could not load bucket from config: %s", err))

		return errors.New("NoSuchBucket")

	}

	Resp_ListObjectsV2 := ListObjectsV2{}
	Resp_ListObjectsV2_Contents := []ListObjectsV2_Contents{}
	Resp_ListObjectsV2_Dir := []ListObjectsV2_Dir{}

	var pathname string

	if req["prefix"] != "" {

		if strings.HasSuffix(req["prefix"], "/") {
			pathname = fmt.Sprintf("%s/%s", bucket_config.Pathname, req["prefix"])
		} else {

			pathname = fmt.Sprintf("%s/%s", bucket_config.Pathname, req["prefix"])
			dir, file := filepath.Split(pathname)
			pathname = dir
			req["prefix"] = file
		}

	} else {
		pathname = bucket_config.Pathname

	}

	files, err := os.ReadDir(pathname)

	if err != nil {
		return err
	}

	//err := filepath.WalkDir(pathname, func(path string, dir fs.DirEntry, err error) error {

	for _, file := range files {

		if file.Name() == "." || file.Name() == ".." {
			continue
		}

		if err != nil {
			fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", file.Name(), err)
			return nil
		}

		cleanPath := strings.Replace(file.Name(), bucket_config.Pathname, "", 1)

		if err != nil {
			fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", file.Name(), err)
			return err
		}

		if req["prefix"] != "" && !strings.HasSuffix(req["prefix"], "/") {

			if !strings.HasPrefix(cleanPath, req["prefix"]) {
				continue
			}

		}

		//fmt.Printf("visited file or dir: %q\n", file.Name())

		if file.IsDir() {
			dir := ListObjectsV2_Dir{}
			dir.Prefix = file.Name() + "/"
			Resp_ListObjectsV2_Dir = append(Resp_ListObjectsV2_Dir, dir)

		} else {
			resp := ListObjectsV2_Contents{}

			info, err := file.Info()

			if err != nil {
				continue
			}

			resp.Key = file.Name()

			// URL decode the key
			resp.Key, _ = url.PathUnescape(resp.Key)

			resp.LastModified = info.ModTime()
			resp.Size = info.Size()
			resp.StorageClass = "STANDARD"

			// TODO: Improve ETAG (hash of the local file?)
			fileHash := fmt.Sprintf("%s/%s:(%s)", bucket, file.Name(), resp.LastModified.String())
			md5Hash := md5.New()
			md5Hash.Write([]byte(fileHash))

			resp.ETag = hex.EncodeToString(md5Hash.Sum(nil))

			Resp_ListObjectsV2_Contents = append(Resp_ListObjectsV2_Contents, resp)
		}

	}

	Resp_ListObjectsV2.Name = bucket
	Resp_ListObjectsV2.Prefix = req["prefix"]
	Resp_ListObjectsV2.KeyCount = len(Resp_ListObjectsV2_Contents)
	Resp_ListObjectsV2.MaxKeys = 1000
	Resp_ListObjectsV2.IsTruncated = false
	Resp_ListObjectsV2.Contents = &Resp_ListObjectsV2_Contents

	Resp_ListObjectsV2.CommonPrefixes = &Resp_ListObjectsV2_Dir

	//spew.Dump(Resp_ListObjectsV2)

	return c.XML(Resp_ListObjectsV2)
}
