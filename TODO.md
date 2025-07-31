# TODO

- [x] Add support for authenticated permissions via access-key/secret, using AWS v4 AUTH
- [x] Add core authentication
  - [x] Allow public bucket support w/o authentication. Includes support for `--no-sign-request` if a profile is used
- [x] Add DELETE method
  - [x] Handle deletion of top level keys for parent hierarchy
- [x] Improve x-amz-id-2 and request-Id headers, through entire routes
- [x] Implement multi-part uploads
  - [x] Support chunked encoding for object PUT
  - [x] Support "CompleteMultipartUpload operation: Your proposed upload is smaller than the minimum allowed size" which is 5MB
- [x] Improve validation for bucket and object keys
- Add unit tests for testing put/get/list/delete without the correct permission set.
- Add support for auth header to correctly detect session > 15mins and invalidate.
- Support checksum CRC32/crc64nvme and other common checksums used for functions such as `cp` and multi-part uploads.
- Add support for S3 bucket delete
