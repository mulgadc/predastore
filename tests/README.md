# S3 Server Tests

This directory contains tests for the S3 server implementation.

## Directory Structure

- `config/` - Contains test configuration for the S3 server
- `data/` - Contains test files that will be served by the S3 server
- `run_tests.sh` - Helper script to run all tests
- `s3_integration_test.go` - Integration tests using the AWS SDK

## Test Files

The test data includes:

- **test.txt** - A simple text file for testing text file retrieval
  - SHA256: `8a71e72cef7867d59c08ee233df6d2b7c35369734d0b6dae702857176a1d69f8`

- **binary.dat** - A binary file for testing binary file retrieval
  - SHA256: `ce9d16a33fc9a53f592206c0cd23497632e78d3f6219dcd077ec9a11f50e6e4e`

## Running the Tests

To run all tests, use the provided script:

```sh
./tests/run_tests.sh
```

This will:
1. Create the necessary test directories and files if they don't exist
2. Display the SHA256 hashes of test files
3. Run the unit tests in the `s3` package
4. Run the integration tests

## Unit Tests

The unit tests use the `s3.SetupRoutes()` function to create a Fiber app for testing:

- `s3/config_test.go` - Tests for configuration loading and bucket retrieval
- `s3/list_test.go` - Tests for bucket and object listing
- `s3/get_test.go` - Tests for object retrieval
- `s3/put_test.go` - Tests for object upload

Unit tests don't require a running server and use the Fiber app test functionality.

## Integration Tests

The integration tests also use the `s3.SetupRoutes()` function but start a real HTTPS server in a background goroutine:

- The server is started in a goroutine with a TLS connection
- Tests use the AWS SDK to interact with the server
- Graceful shutdown is implemented with context
- Testing includes:
  - Bucket listing
  - Object listing
  - Object retrieval (including range requests)
  - Object uploading
  - Error handling

The integration tests demonstrate how the S3 server would be used by real clients through the AWS SDK 