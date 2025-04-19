# TODO

- Add support for authenticated permissions via access-key/secret, using AWS v4 AUTH
- Allow public bucket support w/o authentication
- Add unit tests for testing put/get/list/delete without the correct permission set.
- Add support for auth header to correctly detect session > 15mins and invalidate.
- Add delete method
- Support chunked encoding for object PUT
- Support checksum CRC32/crc64nvme and other common checksums used
- Support multipart uploads
