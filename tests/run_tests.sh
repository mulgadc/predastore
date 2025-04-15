#!/bin/bash

# Make sure we're in the project root
cd "$(dirname "$0")/.." || exit 1

# Create test data if it doesn't exist
mkdir -p tests/data
mkdir -p tests/config

# Ensure test files exist
if [ ! -f tests/data/test.txt ]; then
  echo "This is a sample text file for testing the S3 server.
It contains multiple lines to test retrieving text files from the S3 server.
End of file." > tests/data/test.txt
fi

if [ ! -f tests/data/binary.dat ]; then
  dd if=/dev/urandom of=tests/data/binary.dat bs=1024 count=10
fi

# Ensure test config exists
if [ ! -f tests/config/server.toml ]; then
  cat > tests/config/server.toml << EOF
# Test configuration file in TOML format.
version = "1.0"
region = "ap-southeast-2"

[[buckets]]
name = "testbucket"
region = "ap-southeast-2"
type = "fs"
pathname = "./tests/data"
public = true
encryption = ""

[[buckets.acl]]
access_key_id = "test_access_key"
secret_access_key = "test_secret_key"
permissions = 0
EOF
fi

# Print file SHA256 hashes
echo "Test file SHA256 hashes:"
sha256sum tests/data/test.txt tests/data/binary.dat

echo "Running unit tests..."
echo "---------------------"
go test ./s3 -v

echo ""
echo "Running integration tests..."
echo "---------------------------"
go test ./tests -v -count=1

echo "All tests complete!" 