# Predastore Roadmap

Gaps between [DESIGN.md](./DESIGN.md) and the current codebase, plus
longer-horizon work. DESIGN covers the intended architecture and rationale;
this file tracks the distance to it.

Items become `bd` issues when claimed. Priority below is rough ordering, not a
queue.

---

## Design–Implementation Gaps

### Store Rewrite (replaces WAL)

The `store` package (`predastore/store/`) replaces the WAL. The write path
and `store.go` are drafted; `objectReader` is not done. Fixes, reader
completion, periodic fsync, and QUIC integration are tracked below.

#### Stage 1: Fix Store Issues

**Critical (won't work without these)**

- [x] 1. ~~Export `extent` fields~~ — fields are now `SegNum`/`Off`/`Size`.
- [x] 2. ~~Initialize `segCache`~~ — `Open` now uses `make(map[uint64]*segment)`.
- [x] 3. ~~Separate local frag index from global `fragNum` in flush~~ — writer redesign: `off`/`flushed` track byte position within the extent (headers included). `fragNum` is only used for the header field, incremented in `writeHeader()`. Offset is `extent.Off + flushed`.
- [x] 4. ~~Fix `flush()` infinite loop~~ — writer redesign: flush writes `buf[0:writeLen]` in a single `WriteAt`. No per-fragment loop, no advancing `pos`.
- [x] 5. ~~Add final flush before `Close()`~~ — `Close()` now calls `flush(true)` when `off > flushed` before syncing.
- [x] 6. ~~Fix `Close()` deadlock~~ — `onClose` no longer acquires the store mutex.

**Serious (incorrect behavior)**

- [x] 7. ~~Reader `Close()` must call `onClose`~~ — fixed in Stage 2 rewrite. Sets `closed = true`, calls `onClose()`.
- [x] 8. ~~Make `refs` atomic~~ — now `atomic.Int32` with `.Add()`/`.Load()`.
- [x] 9. ~~Fix `Delete` to construct shard key~~ — now uses `makeShardKey(objectHash, shardIndex)` with `[32]byte` signature.
- [x] 10. ~~Fix `flagEndOfShard` check~~ — writer redesign: `flagEndOfShard` set on the last frag when `final && i == nFrags-1`. No dependence on global `fragNum`.

**Logic errors / consistency**

- [x] 11. ~~Fix `flagSegFull` type~~ — now `flagFull segmentFlags`.
- [x] 12. ~~Fix `ReadFrom` data loss on EOF~~ — writer redesign: `ReadFrom` flushes whenever `dataWritten() >= dataSize`, so partial-buffer + EOF always triggers a flush before returning.
- [x] 13. ~~Remove unused `sync/atomic` import~~ — now used by `atomic.Int32`.

**Defensive / minor**

- [x] 14. ~~Add max iteration limit to `Append` rotation loop~~ — now has `maxAttempts = 100`.
- [x] 15. ~~Fix `Open` error message with nil error~~ — inner closure now returns meaningful errors.
- [x] 16. ~~Close leaked file descriptors in `Open` error path~~ — defer in inner closure closes on error.
- [x] 17. ~~Remove `O_APPEND` from `openSegment`~~ — now `O_CREATE|O_RDWR`, header written with `WriteAt`.

**New issues (introduced during Stage 1 work)**

- [x] 18. ~~`fmt.Errorf` format string bugs~~ — all four call sites fixed.
- [x] 19. ~~`Append` rotation loop missing `store.segNum += 1`~~ — now incremented at end of loop.
- [x] 20. ~~`Delete` doesn't acquire mutex~~ — now locks/defers `store.mutex`.

#### Stage 2: Finish shardReader

Complete rewrite matching shardWriter. Single-extent model, `pos` tracks
logical read position, `ReadAt` translates logical offset → disk offset,
CRC validated as `crc32.ChecksumIEEE(frag[0:totalFragSize])`. Added
`DataSize` field to extent for logical size; `extent.Size` is on-disk.

- [x] 21. ~~Strip multi-extent scaffolding from `ReadAt`~~ — single-extent: `fragIndex = logicalPos / fragBodySize`, disk read from `ext.Off + fragIndex*totalFragSize`.
- [x] 22. ~~Fix `Size()`~~ — returns `ext.DataSize` (logical size stored in extent).
- [x] 23. ~~Fix `Close()`~~ — calls `onClose()`, sets `closed = true`. Also fixes Stage 1 #7.
- [x] 24. ~~Fix CRC validation to match writer~~ — validates `crc32.ChecksumIEEE(buf[:totalFragSize])` with CRC field zeroed.
- [x] 25. ~~Fix `WriteTo`~~ — uses `io.NewSectionReader(r, 0, r.ext.DataSize)`.
- [x] 26. ~~Remove undefined references~~ — `byteExtent`, `locateByteOffset`, `readBufferFragments` all deleted.
- [x] 27. ~~Validate payload length from frag header~~ — bounds-checked against `fragBodySize` before use.

#### Stage 3: Periodic Fsync

- [ ] 28. **Add a background fsync goroutine to Store** — 200ms ticker matching the WAL's `syncWALIfDirty` pattern. Track dirty segments with an atomic flag.
- [ ] 29. **Remove per-close `file.Sync()` from `shardWriter.Close()`** — the periodic syncer handles durability.
- [ ] 30. **Add `StopSyncer()` / final sync to `store.Close()`** — ensure all data is fsynced before shutdown.

#### Stage 4: QUIC Server Integration

- [x] 31. ~~Replace `*wal.WAL` with `*store.Store` in `QuicServer`~~ — `wal`/`walMu` replaced with `store *store.Store`. `NewWithRetry` calls `store.Open`, `Close` calls `store.Close`.
- [x] 32. ~~Rewrite `handlePUTShard`~~ — `store.Append` reserves under short lock, `writer.ReadFrom(br)` streams directly from QUIC stream lock-free (no pre-buffer), `writer.Close()` commits.
- [x] 33. ~~Rewrite `handleGET`~~ — ~100 lines of manual fragment walking replaced with `store.Lookup` → `io.NewSectionReader` → `io.Copy`. Range reads use `SectionReader` directly.
- [x] 34. ~~Rewrite `handleDELETEShard`~~ — replaced direct `walInstance.DB` access with `store.Delete(hash, shardIndex)`. `DeleteRequest` gained `ShardIndex` field. Compaction tracking deferred (see #36).
- [x] 35. ~~Simplify `PutResponse`~~ — `WriteResult wal.WriteResult` replaced with `ShardSize int64`. `quicclient` updated to match.
- [ ] 36. **Add deletion tracking to Store** — Store needs a mechanism to record freed extents for future compaction. Currently `store.Delete` just removes the index entry; the on-disk extent becomes dead space.
- [x] 37. ~~Remove WAL imports~~ — all `wal.*` references removed from `quicserver`, `quicclient`. Handlers split to `get.go`, `put.go`, `delete.go`.
- [x] 38. ~~Update distributed backend~~ — deleted `putObjectToWAL` (~180 LOC), removed `useQUIC`/`UseQUIC` from `Config`/`Backend`, `putObjectViaQUIC` returns `(size, error)`, `ObjectShardReader` deleted, `shardWriteOutcome` simplified, `deleteObjectViaQUIC` sends per-shard deletes with `ShardIndex`. `s3/wal` import removed from all production code.
- [ ] 39. **Delete filesystem backend** — remove `backend/filesystem/` and any config/selection logic that references it.

**Tests not yet updated** — `distributed_test.go`, `bucket_test.go`, `multipart_test.go`, `distributed_putobject_test.go`, `backend_test.go` still reference removed `UseQUIC`/`putObjectToWAL`/`wal.*` types.

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

### Index Rebuild from Segments

DESIGN §6 promises this path. Walk all segments, parse slot headers, rebuild
the index from committed objects (those with `flagSlotFinal` set). Discard
incomplete writes.

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
