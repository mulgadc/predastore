# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with the Predastore codebase.

## Project Overview

Predastore is a high-performance, S3-compatible object storage platform designed for on-premise, edge, and hybrid deployments. It provides full compatibility with the Amazon S3 API while being optimized for local and edge infrastructure.

## Core Features

### S3 API Compatibility
- **Full S3 API Support**: Seamlessly works with AWS CLI, SDKs, and tools
- **Signature V4 Authentication**: Complete AWS authentication compatibility
- **Multipart Uploads**: Support for large file uploads with streaming
- **Bucket Management**: Standard S3 bucket operations and lifecycle management

### Performance & Reliability
- **Reed-Solomon Erasure Coding**: Data redundancy and fault tolerance
- **Chunked Distribution**: Data distributed across multiple nodes
- **High Throughput**: Optimized for low-latency and high-bandwidth scenarios
- **Edge Optimization**: Designed for resource-constrained environments

## Build Commands

```bash
make build          # Build s3d binary
make test           # Run Go tests with LOG_IGNORE=1
make bench          # Run benchmarks
make dev            # Run with air for hot reloading development
make docker         # Build Docker images
make clean          # Clean build artifacts (removes ./bin/s3d)
```

### Docker Commands
```bash
make docker_s3d                 # Build predastore Docker image
make docker_compose_up          # Start services with docker-compose
make docker_compose_down        # Stop docker-compose services
make docker_clean               # Remove Docker images and volumes
make docker_test                # Full Docker test cycle
```

## Usage Examples

### Basic S3 Operations

#### Create and List Buckets
```bash
aws --no-verify-ssl --endpoint-url https://localhost:8443/ s3 mb s3://my-bucket
aws --no-verify-ssl --endpoint-url https://localhost:8443/ s3 ls
```

#### Upload and Download Files
```bash
# Upload file
aws --endpoint-url https://localhost:8443/ s3 cp /local/file.txt s3://my-bucket/

# Download file
aws --endpoint-url https://localhost:8443/ s3 cp s3://my-bucket/file.txt /local/download.txt

# List bucket contents
aws --no-verify-ssl --endpoint-url https://localhost:8443/ s3 ls s3://my-bucket/
```

#### Large File Operations
```bash
# Upload large file (uses multipart upload automatically)
aws --endpoint-url https://localhost:8443/ s3 cp s3://downloads/ubuntu-22.04.2-desktop-amd64.iso /tmp/ubuntu.iso
```

## Architecture

### Service Structure
```bash
predastore/
├── cmd/s3d/                # Main S3 service daemon
├── s3/                     # S3 API implementation
│   ├── auth/              # Authentication and authorization
│   ├── handlers/          # HTTP request handlers
│   ├── multipart/         # Multipart upload support
│   └── storage/           # Storage backend interface
├── config/                # Configuration management
├── docker/                # Docker configurations
└── tests/                 # Test suites
```

### Key Components

#### S3 API Server
- **HTTP/HTTPS Server**: TLS-enabled server for S3 API endpoints
- **Request Routing**: AWS S3 API path and method routing
- **Authentication**: AWS Signature V4 validation
- **Response Formatting**: S3-compatible XML/JSON responses

#### Storage Backend
- **Chunked Storage**: Large objects split into manageable chunks
- **Reed-Solomon Encoding**: Fault-tolerant data distribution
- **Metadata Management**: Object and bucket metadata storage
- **Versioning Support**: Object versioning and lifecycle management

## Configuration

### Environment Variables
```bash
# Server Configuration
PREDASTORE_HOST=localhost
PREDASTORE_PORT=8443
PREDASTORE_TLS_CERT=/path/to/cert.pem
PREDASTORE_TLS_KEY=/path/to/key.pem

# Storage Configuration
PREDASTORE_DATA_DIR=/var/lib/predastore
PREDASTORE_CHUNK_SIZE=4MB

# Authentication
PREDASTORE_ACCESS_KEY=your-access-key
PREDASTORE_SECRET_KEY=your-secret-key
```

### Configuration File (config.yaml)
```yaml
server:
  host: "0.0.0.0"
  port: 8443
  tls:
    cert_file: "config/server.pem"
    key_file: "config/server.key"

storage:
  backend: "file"
  data_dir: "/var/lib/predastore"
  chunk_size: "4MB"
  erasure:
    data_shards: 4
    parity_shards: 2

auth:
  access_key: "EXAMPLEKEY"
  secret_key: "EXAMPLEKEY"
```

## Development Patterns

### S3 API Handler Structure
```go
// HTTP handler for S3 operations
func (s *S3Server) HandlePutObject(w http.ResponseWriter, r *http.Request) {
    // 1. Parse S3 request (bucket, key, headers)
    // 2. Validate authentication (Signature V4)
    // 3. Process object data (chunking, encoding)
    // 4. Store to backend
    // 5. Return S3-compatible response
}
```

### Authentication Flow
```go
// AWS Signature V4 validation
func ValidateSignature(r *http.Request) error {
    // 1. Extract authorization header
    // 2. Parse signature components
    // 3. Reconstruct canonical request
    // 4. Verify signature against stored credentials
}
```

### Storage Backend Interface
```go
type StorageBackend interface {
    PutObject(bucket, key string, data []byte) error
    GetObject(bucket, key string) ([]byte, error)
    DeleteObject(bucket, key string) error
    ListObjects(bucket, prefix string) ([]ObjectInfo, error)
}
```

## Integration with Hive Platform

### NATS Integration
When used with Hive, Predastore integrates via NATS messaging for S3 operations:

```bash
# S3 Service Topics:
s3.createbucket              # Create S3 bucket
s3.putobject                 # Store object
s3.getobject                 # Retrieve object
s3.listbuckets               # List buckets
s3.deleteobject              # Delete object
s3.multipart.init            # Initialize multipart upload
s3.multipart.upload          # Upload part
s3.multipart.complete        # Complete multipart upload
```

### System Integration
- **EC2 Images**: Stores AMI images for EC2 instance launches
- **EBS Snapshots**: Backend storage for Viperblock volume snapshots
- **User Data**: Stores cloud-init configurations and user data
- **System Artifacts**: SSH keys, configuration files, and logs

## Testing and Development

### Development Server
```bash
# Start development server with hot reloading
make dev

# The server will restart automatically when Go files change
# Default endpoint: https://localhost:8443/
```

### Testing with AWS CLI
```bash
# Configure AWS CLI for local development
aws configure set aws_access_key_id EXAMPLEKEY
aws configure set aws_secret_access_key EXAMPLEKEY
aws configure set default.region ap-southeast-2

# Test basic operations
aws --endpoint-url https://localhost:8443/ --no-verify-ssl s3 ls
```

### Docker Development
```bash
# Full Docker development environment
make docker_compose_up

# Run tests in Docker
make docker_test

# Clean up
make docker_compose_down
```

## Performance Optimization

### Chunking Strategy
- **Default Chunk Size**: 4MB for optimal network and storage performance
- **Large File Handling**: Automatic chunking for files > chunk size
- **Parallel Processing**: Concurrent chunk processing for uploads/downloads

### Caching
- **Metadata Caching**: In-memory caching of bucket and object metadata
- **Connection Pooling**: HTTP connection reuse for better performance
- **Compression**: Optional compression for small objects

### Erasure Coding
- **Data Shards**: Configurable number of data chunks
- **Parity Shards**: Redundancy for fault tolerance
- **Recovery**: Automatic data recovery from available shards

## API Compatibility

### Supported S3 Operations
- **Bucket Operations**: CreateBucket, DeleteBucket, ListBuckets, HeadBucket
- **Object Operations**: PutObject, GetObject, DeleteObject, HeadObject
- **Listing**: ListObjects, ListObjectsV2 with pagination
- **Multipart**: InitiateMultipartUpload, UploadPart, CompleteMultipartUpload
- **Authentication**: AWS Signature V4 for all operations

### AWS CLI Compatibility
Predastore works with standard AWS CLI commands by setting the endpoint:
```bash
aws --endpoint-url https://localhost:8443/ s3 <command>
```

### SDK Compatibility
Compatible with AWS SDKs in various languages by configuring the endpoint:
```go
// Go SDK example
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithEndpointResolver(aws.EndpointResolverFunc(
        func(service, region string) (aws.Endpoint, error) {
            return aws.Endpoint{
                URL: "https://localhost:8443",
            }, nil
        })),
)
```

## Error Handling

### S3 Error Responses
Predastore returns standard S3 error codes and messages:
- **NoSuchBucket**: Bucket does not exist
- **NoSuchKey**: Object does not exist
- **InvalidAccessKeyId**: Authentication failure
- **SignatureDoesNotMatch**: Signature validation failure

### Logging and Monitoring
- **Structured Logging**: JSON format logs for better parsing
- **Request Tracing**: Unique request IDs for debugging
- **Metrics**: Performance and usage metrics collection
- **Health Checks**: Service health and readiness endpoints

## Security

### Authentication
- **AWS Signature V4**: Full compatibility with AWS authentication
- **Access Keys**: Secure access key and secret key management
- **Request Validation**: Complete request signature validation

### TLS/SSL
- **HTTPS Only**: All communications encrypted
- **Certificate Management**: Support for custom certificates
- **Security Headers**: Appropriate security headers in responses

## Use Cases

### Primary Use Cases
- **Edge AI and IoT**: Data collection and storage at edge locations
- **Private Cloud**: On-premise object storage for enterprise
- **Media Storage**: Video and media file archiving and streaming
- **Backup Solutions**: Reliable backup and disaster recovery storage
- **Hybrid Cloud**: Bridge between on-premise and cloud storage