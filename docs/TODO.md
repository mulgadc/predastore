# Predastore Roadmap

Work not yet done. [DESIGN.md](./DESIGN.md) describes what the code does today, and its §15 lists the gaps between that and the intended architecture; this file is the plan for closing them, plus longer-horizon work.

Items become `bd` issues when claimed. The ordering below is rough priority, not a queue.

---

## Performance

The measurements below were taken on the pre-refactor tree (3 nodes, warp mixed, c=10, 1 MiB objects): PUT ~53 MiB/s, p50 ~83 ms. Per-PUT instrumentation put the floor at **two synchronous Raft commits** on the critical path of every PUT, ~40 ms median combined. The per-shard segment fsync (~9 ms) and the per-node index writes (~60 µs) were not the bottleneck.

Transport and package names have changed since — the two commits are now `metaPut` calls from `internal/gate/handlers/put_object.go` — but nothing in the refactor moved the metadata commits off the critical path, so the ordering below still holds. Re-measure with `./scripts/bench.sh 3host` before claiming any of it.

### Batch the two metadata commits

`PutObject` writes two keys after the shards land: `objects/<hash> → placement` and `objects/<arn> → hash`. They go as two separate `Put` calls, which is two Raft round-trips and two majority fsyncs for one object.

- Add a batch opcode alongside `OpMetaPut`, carrying several key-value pairs in one header.
- Add the matching FSM command type, applying all pairs in a single badger `Update` txn.
- Extend `meta.Client` with the batch call, and route both keys through it.
- Update `PutObject` and `CompleteMultipartUpload`.

Expected saving: ~20 ms median per PUT, one Raft round-trip.

### Overlap the parity send with the data send

In `writeObject` (`internal/gate/handlers/shards.go`), the parity goroutines and `enc.Encode` only start once every data shard has finished uploading, even though the encoder reads the already-populated `dataShardBuffers` and the parity sends are independent of the data sends.

- Spawn the parity goroutines and start `enc.Encode` as soon as `enc.Split` returns, in parallel with the data-shard send loop.
- Wait on both sets at the end, keeping the current first-error semantics.

Expected saving: ~20 ms median.

### Async metadata commit

The largest single lever, and the one with the most surface area. Reply 200 once the shards commit and write the metadata in the background, taking the whole ~40 ms Raft cost off the critical path.

- Requires the orphan scrubber below: the crash window between shard commit and metadata commit becomes routine rather than exceptional.
- Worth doing after the batch commit lands, since batching halves the cost this removes.

### Periodic fsync

`shardWriter.Close` fsyncs the segment per value (`internal/blob/engine/writer.go`). Bench data showed this costs ~9 ms median but is **not** the throughput cap: removing it entirely tied throughput while p99 got 4× worse from kernel writeback storms. A 200 ms periodic syncer would likely be a Pareto improvement on latency without hurting p99, but the payoff is small next to the Raft items.

- Add a 200 ms ticker to `Store` that fsyncs dirty segments.
- Drop `seg.Sync()` from `shardWriter.Close()`, keeping the `.idx` fsync ordering intact — the sidecar must still be durable before the index row commits.
- Final sync on `Store.Close()`.

### Stop buffering whole objects in memory

The PUT path holds every data shard in memory before sending, and the GET path assembles the whole object before responding. Multipart completion additionally stages the assembled object through a temp file. Object size is therefore bounded by gate memory, and concurrency multiplies it.

- PUT: stream `enc.Split` output straight into the per-shard `Put` bodies rather than into `[]byte` buffers.
- GET: stream the join to the response writer; only the reconstruction path needs the shards resident.
- Multipart: concatenate parts into the shard writers directly instead of a temp file.

---

## Design–implementation gaps

These are the items behind [DESIGN.md §15](./DESIGN.md#15-known-gaps).

### Node-local healer

Nothing reconstructs a shard that a blob node lost or never received. A GET rebuilds the object from parity on the fly but does not write the missing shard back, so a replaced blob node leaves every object it held permanently down one shard.

- Periodically ask the meta plane for the shards the ring assigns to this node, and reconstruct any that are locally absent by fetching `K` valid peers and RS decoding.
- Write the result as a normal reservation, so a heal is indistinguishable from a write on disk.
- Optionally feed an in-memory recent-failures queue from failed reads for faster repair.

### Startup scrubber for orphaned shards

Shards commit before metadata. A crash in between leaves shards that nothing references and nothing tombstones, so compaction never reclaims them. This is also the precondition for the async metadata commit above.

- Walk the node's index, reconcile each key against the metadata plane, and either backfill the missing placement or tombstone the extent.
- Needs care against in-flight writes: a key with no placement may be a PUT still in progress, not an orphan.

### Index rebuild from the .idx sidecars

If a blob node's badger index is lost, its data is unreachable through that node even though the bytes are intact. The `.idx` sidecars record the 36-byte key of every extent ever allocated, so a rebuild is mechanically possible.

- Walk each segment's sidecar, validate each extent by opening its first fragment (a failed GCM tag rejects a torn or superseded row), and rebuild the index rows.
- Resolve duplicate allocations for one key: the sidecar records every allocation, including the ones an overwrite superseded, and only one is live.
- Rebuild the tombstone namespace as dead space rather than trying to recover it.

### Cold-storage tiering

Compaction is implemented and always on; tiering is not.

- Migrate aged segments to long-term cold storage, rehydrating on demand during a GET (rehydration may complete asynchronously with the GET).
- Policy for "aged" and "cold" is TBD.

### Envelope encryption and key rotation

One cluster-wide master key, no envelope layer. Rotating it would mean re-encrypting every fragment, which is not viable.

- Introduce a per-data-dir derived key wrapped by a true cluster master held in NATS KV; rotating the master re-wraps the derived keys without re-encrypting any ciphertext.
- Per-bucket and per-tenant keys ride the same envelope layer.
- Collapses the cluster-wide `storeID` collision domain, since each data dir gets its own per-key nonce space.

### Extend the property-based harness past the engine

`internal/storetest` holds a reference model and `internal/blob/engine` drives it with `rapid`, including fault injection. The layers above it are not covered.

- Extend the model through the blob rpc server, so the protocol framing and the range paths are exercised against the same invariant.
- A separate model for the meta FSM: apply, snapshot and restore should round-trip arbitrary binary keys and values.
- Invariant throughout: reads see a linearisation of commits, and nothing else.

---

## S3 API

- **Pagination for `ListObjects`.** The handler scans and returns everything under the prefix, hardcodes `IsTruncated: false`, and does not enforce `max-keys`. A large bucket returns an unbounded response.
- **`LastModified` is not stored.** Listings report the current time for every object. Needs a timestamp in the placement record, or a parallel metadata key.
- **ETag backfill for pre-v3 objects.** `PutObject` and `CompleteMultipartUpload` store the body's MD5 in placement record v3 — a plain hex digest for a single part, `"<md5>-<N>"` for a multipart object assembled from N parts — and `GetObject`, `HeadObject` and `ListObjectsV2` return it as the ETag. An object written under an older placement version has no stored digest, so these surfaces omit its ETag rather than serve a stale value, and there is no migration to add one.
- **Checksum support.** `internal/gate/chunked` computes CRC64NVME and can verify the trailer, but no handler calls `VerifyTrailerChecksum`, the `x-amz-checksum-*` request headers are not honoured, and `ChecksumCRC64NVME` in the multipart response is never populated.
- **End-to-end authorization tests.** Policy evaluation is unit tested in `internal/gate/policy_test.go`, but no test drives PUT, GET, LIST or DELETE through the full handler stack with a credential that lacks the action.

---

## Future work

Longer-horizon, not planned for this cycle.

- **Cluster rebalancing**: redistribute shards on node join or leave, and when the RS configuration changes. Today the ring change applies to new objects only.
- **Process separation**: run each node as its own process rather than as a goroutine inside one `s3d`, to reduce blast radius and let roles be scheduled independently. The rpc layer already makes this mostly a matter of how nodes are launched, since a colocated pair would simply move from the pipe transport to QUIC.
- **Gossip protocol**: dynamic node discovery in place of a static config file on every host.
- **mTLS between nodes**: client certificates so a peer is authenticated, not just the ALPN and the server certificate.
- **Storage classes**: per-object redundancy selection.
- **Object versioning**: S3-compatible versioning.
- **Lifecycle policies**: automatic object expiration.
- **Compression**: optional LZ4/Snappy applied by the compactor to segments it rewrites.
- **Event notifications**: S3-compatible hooks.
- **Metrics**: `pkg/otelsetup` covers traces, metrics and structured logging; what is missing is coverage of the storage internals — segment counts, live fraction, compaction cycles, free-space watermarks.

---

## Recently closed

Tracked here so an old reference to one of these is not mistaken for outstanding work.

- **Background compaction** — `internal/blob/engine/compaction.go`. Always on, interval from `[compaction] interval_seconds`, with an out-of-cycle pass on nearfull.
- **Deletion tracking** — tombstones keyed by physical slot, written in the same transaction as the index removal.
- **The per-PUT temp file** — the body is split straight into the shard buffers. Only multipart completion still stages a temp file.
- **`DeleteBucket`** — implemented, with the bucket record removed from the meta plane.
- **Dynamic bucket operations** — `CreateBucket` and `DeleteBucket` write to the meta plane. Config-defined buckets remain as a static set known at startup.
- **Read replicas** — `meta.Client` reads from the cached leader first and then any replica; only writes need the leader.
- **Session expiry** — session credentials carry `ExpiresAt`, are never cached, and expiry is re-checked on every request.
- **A reference model and property tests** — `internal/storetest` plus the `rapid` suites in `internal/blob/engine`, including fault injection.
