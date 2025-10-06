# S3 Client Utility

A command-line tool for interacting with S3-compatible servers with AWS Signature V4 authentication support.

## Features

- Supports GET, PUT, HEAD, and DELETE operations
- AWS Signature V4 authentication
- TLS support with option to skip certificate validation (for development)
- Customizable endpoint, region, and bucket
- Can read request body from file or command line
- Can write response to file

## Usage

```bash
# List buckets (authenticated)
./s3client -endpoint="https://localhost:443" -access-key="TESTACCESSKEY" -secret-key="TESTSECRETKEY" -method="GET"

# Get an object from a bucket (authenticated)
./s3client -endpoint="https://localhost:443" -bucket="test-bucket01" -path="test.txt" -method="GET" \
  -access-key="TESTACCESSKEY" -secret-key="TESTSECRETKEY" -output="downloaded.txt"

# Upload a file to a bucket (authenticated)
./s3client -endpoint="https://localhost:443" -bucket="test-bucket01" -path="upload.txt" -method="PUT" \
  -body-file="local_file.txt" -access-key="TESTACCESSKEY" -secret-key="TESTSECRETKEY"

# Check if an object exists (HEAD request)
./s3client -endpoint="https://localhost:443" -bucket="test-bucket01" -path="test.txt" -method="HEAD" \
  -access-key="TESTACCESSKEY" -secret-key="TESTSECRETKEY"
```

## Options

| Flag              | Description                          | Default                 |
| ----------------- | ------------------------------------ | ----------------------- |
| `-endpoint`       | S3 endpoint URL                      | `https://localhost:443` |
| `-region`         | AWS region                           | `us-east-1`             |
| `-bucket`         | S3 bucket name                       | (empty)                 |
| `-method`         | HTTP method (GET, PUT, HEAD, DELETE) | `GET`                   |
| `-path`           | Path within bucket                   | (empty)                 |
| `-body`           | Request body (for PUT)               | (empty)                 |
| `-body-file`      | File to read request body from       | (empty)                 |
| `-access-key`     | AWS access key                       | (empty)                 |
| `-secret-key`     | AWS secret key                       | (empty)                 |
| `-output`         | File to write response to            | (empty - stdout)        |
| `-skip-tls-check` | Skip TLS certificate verification    | `false`                 |

## AWS Signature V4 Authentication

This client implements the AWS Signature Version 4 signing process as specified in the [AWS documentation](https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html).

The signing process includes:

1. Creating a canonical request
2. Creating a string to sign
3. Calculating the signature
4. Adding the signature to the HTTP request

When both `-access-key` and `-secret-key` are provided, the client will automatically generate and include the necessary authorization headers.
