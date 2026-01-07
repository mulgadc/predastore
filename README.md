# PredaStore

PredaStore is a lightweight, high-performance S3-compatible object storage server designed for on-premise, edge, and hybrid deployments. It implements the most commonly used S3 API operations, making it a practical alternative to Amazon S3 for local and private cloud environments.

PredaStore serves as the S3 storage backend for [Hive](https://github.com/mulgadc/hive), but can also be run as a standalone S3-compatible server.

## Storage Backends

### Filesystem (Default)

The default storage backend uses the local filesystem with a simple, efficient layout. This mode is production-ready and suitable for single-node deployments.

```bash
make build
./bin/s3d
```

### Distributed (Under Development)

A distributed storage backend is currently under active development, featuring:

- **Reed-Solomon Erasure Coding**: Data redundancy and fault tolerance through configurable data/parity shards
- **Consistent Hash Ring**: Deterministic shard placement across nodes
- **Multi-Node Architecture**: Data distributed across multiple storage nodes via QUIC transport
- **Automatic Reconstruction**: Recovery of corrupted or missing shards from parity data

> **Note**: The distributed backend is experimental and not yet recommended for production use.

## S3 API Compatibility

PredaStore implements key S3 API operations compatible with AWS CLI, SDKs, and existing S3 tools:

| Operation | Status |
|-----------|--------|
| CreateBucket | Supported |
| DeleteBucket | Supported |
| ListBuckets | Supported |
| HeadBucket | Supported |
| PutObject | Supported |
| GetObject | Supported |
| DeleteObject | Supported |
| HeadObject | Supported |
| ListObjects / ListObjectsV2 | Supported |
| Multipart Upload | Supported |
| AWS Signature V4 | Supported |

## Quick Start

### Build

```bash
make build
```

### Run

```bash
./bin/s3d
```

### Configuration

Configure via environment variables or `config.yaml`:

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

auth:
  access_key: "your-access-key"
  secret_key: "your-secret-key"
```

## Usage

### AWS CLI Examples

```bash
# Create a bucket
aws --no-verify-ssl --endpoint-url https://localhost:8443/ s3 mb s3://my-bucket

# List buckets
aws --no-verify-ssl --endpoint-url https://localhost:8443/ s3 ls

# Upload a file
aws --no-verify-ssl --endpoint-url https://localhost:8443/ s3 cp ./file.txt s3://my-bucket/

# Download a file
aws --no-verify-ssl --endpoint-url https://localhost:8443/ s3 cp s3://my-bucket/file.txt ./downloaded.txt

# List bucket contents
aws --no-verify-ssl --endpoint-url https://localhost:8443/ s3 ls s3://my-bucket/
```

## Integration with Hive

PredaStore is the default S3 storage provider for the [Hive](https://github.com/mulgadc/hive) platform, providing object storage for:

- EC2 AMI images
- EBS volume snapshots
- User data and cloud-init configurations
- System artifacts

When running as part of Hive, PredaStore integrates via NATS messaging for S3 operations.

### Viperblock

[Viperblock](https://github.com/mulgadc/viperblock) is a high-performance block storage system that uses PredaStore as its S3-compatible backend for edge to on-premise deployments.

Key features:
- Log-structured writes for optimized performance
- Block-to-object mapping with configurable chunk sizes
- Checksumming and intelligent caching
- Designed for VM and container storage needs

Together, PredaStore and Viperblock provide the complete storage stack for Hive's EBS-compatible block storage service.

## Development

```bash
make build          # Build s3d binary
make test           # Run tests
make bench          # Run benchmarks
make dev            # Run with hot reloading (requires air)
make clean          # Clean build artifacts
```

### Docker

```bash
make docker_s3d             # Build Docker image
make docker_compose_up      # Start with docker-compose
make docker_compose_down    # Stop services
```

## Performance Tuning

PredaStore uses QUIC for inter-node communication in distributed mode. For optimal performance, increase the system socket buffer sizes:

```bash
sudo sysctl -w net.core.rmem_max=7500000
sudo sysctl -w net.core.wmem_max=7500000
```

## Use Cases

- Edge AI and IoT data collection
- Private cloud object storage
- Media and video archiving
- Backup and disaster recovery
- Development and testing (local S3 replacement)

## Roadmap

- [x] S3 API core support
- [x] AWS Signature V4 authentication
- [x] Multipart uploads
- [ ] Distributed storage with erasure coding
- [ ] Built-in IAM
- [ ] Real-time metrics and dashboard
- [ ] Bucket versioning
- [ ] Lifecycle policies

## License

Apache 2.0 License. See [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) to get started.
