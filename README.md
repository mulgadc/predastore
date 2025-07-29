# PredaStore

PredaStore is a high-performance, on-premise and edge-ready object storage platform, fully compatible with the Amazon S3 API. Designed for environments where speed, redundancy, and resiliency are critical, PredaStore is the ideal solution for edge data centers, private clouds, and hybrid deployments that demand low latency and high availability.

# 🚀 Key Features

📦 S3-Compatible API
Seamlessly integrates with existing S3 clients, tools, and SDKs.

⚡ Blazing Fast Performance
Optimized for low-latency operations and high throughput on local or edge infrastructure.

🛡️ Built-in Redundancy & Resiliency
Data is automatically replicated and protected against node failures and hardware issues.

🌐 Edge & On-Premise First
PredaStore is designed to run efficiently in disconnected, resource-constrained, or bandwidth-sensitive environments.

🧩 Modular Architecture
Pluggable backends and extensible components make it easy to adapt to different workloads and infrastructures.

📊 Lightweight & Scalable
Minimal footprint, yet scalable from a single-node to multi-site deployments.

## 💡 Use Cases

* Edge AI and IoT data collection
* Private cloud object storage
* Media and video archiving
* Backup and disaster recovery
* Hybrid cloud caching

## 📣 Roadmap

✅ S3 API Core Support

🚧 Improved authentication with inbuilt IAM

🚧 Multi-node redundancy

🚧 Erasure coding

🚧 Real-time metrics and dashboard

## 🤝 Contributing

We welcome contributions! Check out CONTRIBUTING.md to get started.

## 📄 License

Apache 2.0 License. See LICENSE for details.

## Usage

List files

```
aws --no-verify-ssl --endpoint-url https://localhost:8443/ s3 ls s3://downloads/
```

Get file

```
aws --endpoint-url https://localhost:8443/ s3 cp s3://downloads/ubuntu-22.04.2-desktop-amd64.iso /tmp/ubuntu.iso
```
