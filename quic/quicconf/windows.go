// Package quicconf holds shared QUIC transport tuning used by both the
// quicclient and quicserver packages. Centralising these avoids drift
// between the client and server sides of the same connection.
package quicconf

// QUIC flow-control window sizes. Interim tuning for the multipart upload
// deadlock — see docs/development/bugs/multipart-upload-deadlock.md (Bug C).
//
// Sizing targets a worst case of ~30 concurrent streams per pooled
// connection (AWS CLI default 10 concurrent parts × 3 shards) carrying
// 4 MiB shards, so the connection window covers every in-flight shard
// and no sender blocks on flow control. The per-stream window is set
// above shard size to avoid stream-level throttling. Initial windows are
// raised above the quic-go 512 KiB defaults to skip slow-start.
const (
	InitialStreamReceiveWindow     uint64 = 2 * 1024 * 1024
	MaxStreamReceiveWindow         uint64 = 8 * 1024 * 1024
	InitialConnectionReceiveWindow uint64 = 16 * 1024 * 1024
	MaxConnectionReceiveWindow     uint64 = 128 * 1024 * 1024
)
