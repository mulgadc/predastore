# Predastore Roadmap

Gaps between [DESIGN.md](./DESIGN.md) and the current codebase, plus
longer-horizon work. DESIGN covers the intended architecture and rationale;
this file tracks the distance to it.

Items become `bd` issues when claimed. Priority below is rough ordering, not a
queue.

---

## Design–Implementation Gaps

### WAL Refactor

Permanent fix for the multipart upload deadlock. See
[docs/development/bugs/multipart-upload-deadlock.md](./docs/development/bugs/multipart-upload-deadlock.md)
for context; the interim ships on `fix/interim-multipart-deadlock`.

- Reservation protocol: allocate fragment slots, pre-create WAL files via
  `Truncate`, assign SeqNums, increment per-file `refCount` — all under a
  short `wal.mu` critical section, no I/O.
- Lock-free fragment writes via `(*os.File).WriteAt` at arithmetic offsets.
  Tunable fragment-batch size `N` per writer goroutine.
- Commit sequence: `WriteAt` → `fsync` → Badger put → atomic `refCount`
  decrement → close-if-refcount-zero-and-full.
- Abort = skip Badger put; fragments become dead space reclaimable by the
  compactor.
- Drop `bufferedWALFile`, the pre-lock body drain in `handlePUTShard`, and
  the widened quic-go windows in `quicconf`.

### Filesystem Backend Removal

DESIGN no longer references it; the code still has it.

- Delete `backend/filesystem/`.
- Remove `-backend` / `BACKEND` / `BackendType`.
- Collapse backend selection in `s3/server.go` and `cmd/s3d/main.go`.
- Update test configs.

### Background Compaction & Cold-Storage Tiering

DESIGN §6 treats compaction as live; not implemented.

- Scan closed WAL files, rewrite live fragments, reclaim dead space,
  atomically update Badger entries.
- Migrate aged WAL files to long-term cold storage; rehydrate on demand
  during GET (rehydration may complete asynchronously with the GET).
- Policy for "aged" and "cold" is TBD.

### Node-Local Healer

DESIGN §11 describes pull-based repair; not implemented.

- Periodically query s3db for ring-assigned shards; reconstruct any that
  are absent locally by fetching `K` valid peers and RS decoding, writing
  the result as a new reservation.
- Optional in-memory recent-failures queue for faster repair.

### Badger Rebuild from WAL

DESIGN §6 promises this path. Verify it still works after the WAL refactor
and that incomplete shard groups (no `FlagEndOfShard`) are discarded.

### Formal Verification Harness

Reference model + stateful PBT against the QUIC server + WAL.

- Reference model in Go (~50 lines): state is `committed: map[ShardID]ShardBytes`;
  ops `Reserve`/`WriteFragment`/`Commit`/`Abort`/`Read`.
- Driver via `pgregory.net/rapid`.
- Invariant: reads see a linearisation of commits, nothing else.
- Scope: QUIC server + WAL only. s3db and the S3 API layer are out of
  scope for this first pass.

---

## S3 API

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
