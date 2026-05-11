# Predastore Roadmap

Gaps between [DESIGN.md](./DESIGN.md) and the current codebase, plus
longer-horizon work. DESIGN covers the intended architecture and rationale;
this file tracks the distance to it.

Items become `bd` issues when claimed. Priority below is rough ordering, not a
queue.

---

## Performance

The lock-free segment write path is sound at 3-node, c=10, 1 MiB warp-mixed:
PUT ~53 MiB/s, p50 ~83 ms. Per-PUT timing instrumentation (since reverted)
showed the floor is **two synchronous Raft writes** (`globalState.Set` ×2)
to the s3db cluster on the critical path of every PUT, ~40 ms median
combined. The per-shard segment fsync (~9 ms) and per-node store-index
Badger writes (~60 µs) are not the bottleneck. Items below are ordered by
expected ROI.

### Raft-side BatchSet for metadata commits

S3 PutObject commits two keys to the s3db cluster after shards land:
`objectHash → shard metadata` and `arnKey → objectHash`. Today they go as
two separate `client.Put` calls — two Raft round-trips, two majority
fsyncs. Add a server-side batch endpoint and route both keys through a
single Raft proposal.

- Extend `s3db.Client` with a batch Put.
- Add the matching FSM apply (single `Update` txn writing both keys).
- Wire `GlobalState.BatchSet` (interface already sketched and reverted)
  through `DistributedState`.
- Update `PutObject` and `CompleteMultipartUpload` callers.

Expected savings: ~20 ms median per PUT (one Raft round-trip).

### Eliminate the per-PUT temp file in `distributed.PutObject`

The body is currently spooled to `/tmp/distributed-put-*`, then re-opened
inside `putObjectViaQUIC` and read back to populate `dataShardBuffers` for
RS split. On tmpfs this is invisible; on a real disk it's two unnecessary
disk hops on every PUT. Stream the request body directly into the data
shard buffers (the RS streaming encoder accepts an `io.Reader`).

- Remove `os.CreateTemp` + `io.Copy(tmpFile, reader)` from `PutObject`.
- Pass the body reader to a new `putObjectViaQUIC` signature that takes
  `io.Reader` and `size int64` instead of a path.
- Make the existing `bytesBufferWriter` path the only path.

### Overlap parity send with data send

In `putObjectViaQUIC`, parity goroutines and `enc.Encode` start only after
all data shards have fully uploaded, even though the encoder reads the
already-populated `dataShardBuffers` and the parity sends are independent
of data sends.

- Spawn parity goroutines + start `enc.Encode` immediately after
  `enc.Split` returns, in parallel with the data-shard send loop.
- Wait on both sets at the end.

Expected savings: ~20 ms on `quic_us` median.

### Async metadata commit

Largest single lever. Reply 200 OK to the client after QUIC shards commit;
write `globalState` metadata in a background goroutine. Removes the full
~40 ms metadata Raft cost from the critical path.

- Cost: a crash window between shard commit and metadata commit leaves
  orphaned shards. Needs a startup scrubber that scans the per-node store
  index, reconciles against the metadata table, and either backfills
  missing metadata or garbage-collects orphans.
- Likely the right answer long-term, but bigger surface area than the
  items above.

### Periodic fsync (deferred)

Originally tracked as Stage 3 of the store rewrite. Bench data shows the
per-close `seg.Sync()` costs ~9 ms median latency but is **not** the
throughput cap (removing it entirely tied throughput, while p99 latency
got 4× worse from kernel writeback storms). A 200 ms periodic syncer
would likely be a Pareto improvement on latency without hurting p99, but
the throughput payoff is small relative to the Raft items above. Reorder
once those land.

- Add a 200 ms ticker to `Store` that fsyncs dirty segments.
- Drop `seg.Sync()` from `shardWriter.Close()`.
- Final sync on `store.Close()`.

### Deletion tracking in Store

`store.Delete` removes the index entry but leaves the on-disk extent as
dead space. Compaction needs a record of freed extents to reclaim them.

- Persist a freed-extents log (segment-local or store-global).
- Surface it to the future compactor.

---

## Design–Implementation Gaps

### Background Compaction & Cold-Storage Tiering

DESIGN §6 treats compaction as live; not implemented.

- Scan closed segments, rewrite live extents, reclaim dead space,
  atomically update index entries.
- Migrate aged segments to long-term cold storage; rehydrate on demand
  during GET (rehydration may complete asynchronously with the GET).
- Policy for "aged" and "cold" is TBD.

### Node-Local Healer

DESIGN §11 describes pull-based repair; not implemented.

- Periodically query s3db for ring-assigned shards; reconstruct any that
  are absent locally by fetching `K` valid peers and RS decoding, writing
  the result as a new reservation.
- Optional in-memory recent-failures queue for faster repair.

### Envelope Encryption (Master Key Rotation)

The current encryption-at-rest implementation uses a single cluster-wide
master key with no rotation. Rotating the master would require re-encrypting
every fragment, which is not viable.

- Introduce a per-data-dir derived key wrapped by a true cluster master held
  in NATS KV; rotating the master re-wraps the derived keys without
  re-encrypting any ciphertext.
- Per-bucket / per-tenant keys ride the same envelope layer.
- Collapses the cluster-wide `storeID` collision domain (each data dir gets
  its own per-key collision space).

### Formal Verification Harness

Reference model + stateful PBT against the QUIC server + Store.

- Reference model in Go (~50 lines): state is `committed: map[ShardID]ShardBytes`;
  ops `Append`/`Write`/`Close`/`Lookup`/`Read`.
- Driver via `pgregory.net/rapid`.
- Invariant: reads see a linearisation of commits, nothing else.
- Scope: QUIC server + Store only. s3db and the S3 API layer are out of
  scope for this first pass.

---

## S3 API

- `POST /{bucket}?delete=` (S3 batch DeleteObjects). Currently returns 405
  because no route exists; warp's mixed test cleanup hits it. Routing-only
  change.
- Unit tests for put/get/list/delete without the correct permission set.
- Auth header session invalidation after 15 minutes.
- Checksum support (CRC32, crc64nvme, etc.) for `aws s3 cp` and multipart.
- `DeleteBucket`.

---

## Future Work

Longer-horizon, not planned for this cycle.

- **Process separation**: split `s3d` into independent HTTP, s3db (Raft),
  and QUIC storage binaries to enable independent scaling and reduce blast
  radius.
- **Gossip protocol**: replace static `cluster.toml` with dynamic node
  discovery.
- **Dynamic bucket operations**: create/delete buckets via s3db, not config.
- **Cluster rebalancing**: redistribute shards on node join/leave or when
  the RS configuration changes.
- **Read replicas**: serve reads from s3db followers.
- **Storage classes**: per-object redundancy selection.
- **Object versioning**: S3-compatible versioning.
- **Lifecycle policies**: automatic object expiration.
- **Compression**: optional LZ4/Snappy applied by the compactor to closed
  WAL files.
- **Event notifications**: S3-compatible hooks.
- **Metrics & observability**: Prometheus, structured logging.
