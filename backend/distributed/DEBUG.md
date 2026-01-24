# Distributed Backend Debug Notes

## Session: 2026-01-24

### Issue: Shard Collision on Same Node

**Symptom:** "shard num mismatch: expected X, got Y" errors during `CompleteMultipartUpload`, causing "Failed to retrieve part data" errors.

**Root Cause:** All shards of the same object used the same `objectHash` as the storage key on QUIC nodes. When the hash ring placed multiple shards on the same node (common with few nodes), one shard's metadata would overwrite another's.

**Solution:** Added `ShardIndex` to create a compound storage key (`objectHash + shardIndex`) for unique shard identification.

**Files Modified:**
- `quic/quicserver/server.go`: Added `ShardIndex` field to `PutRequest` and `ObjectRequest`, added `makeShardKey()` helper
- `s3/wal/wal.go`: Added `UpdateShardToWAL()` method for storing shard metadata with custom key
- `backend/distributed/distributed.go`: Updated `putObjectViaQUIC` and `shardReaders` to include `ShardIndex`

**Key Code:**
```go
// Compound key: objectHash (32 bytes) + shardIndex (4 bytes)
func makeShardKey(objectHash [32]byte, shardIndex int) []byte {
    key := make([]byte, 36)
    copy(key[:32], objectHash[:])
    binary.BigEndian.PutUint32(key[32:], uint32(shardIndex))
    return key
}
```

---

### Issue: Hash Ring Placement Mismatch

**Symptom:** `TestPutGet_ReconstructionValidation_CorruptionAndMissingWAL` failing with "get: status 404" - shards stored but not found during retrieval.

**Root Cause:** Inconsistent hash ring key usage between storage and retrieval paths:
- `putObjectViaQUIC` used `objectHash` (full bucket+key path)
- `PutObjectFromPath` used `key` from `filepath.Split` (filename only)
- Retrieval always used `objectHash`

This caused shards to be stored on nodes determined by filename-only hash, but looked up on nodes determined by full-path hash.

**Solution:** Changed `PutObjectFromPath` to use `objectHash` for hash ring placement:

```go
// Before (mismatched):
_, file := filepath.Split(objectPath)
key := s3db.GenObjectHash(bucket, file)
hashRingShards, err := b.hashRing.GetClosestN(key[:], ...)

// After (consistent):
hashRingShards, err := b.hashRing.GetClosestN(objectHash[:], ...)
```

**Files Modified:**
- `backend/distributed/put.go`: Lines 172-176

---

### Issue: Multipart Upload Hash Ring Consistency

**Symptom:** CompleteMultipartUpload failed to retrieve uploaded parts.

**Root Cause:** `CompleteMultipartUpload` was using temp filename for hash ring placement instead of `objectHash`.

**Solution:** Changed hash ring lookup to use `objectHash`:
```go
// Use objectHash for hash ring placement - must match what putObjectViaQUIC uses
hashRingShards, err := b.hashRing.GetClosestN(objectHash[:], b.rsDataShard+b.rsParityShard)
```

**Files Modified:**
- `backend/distributed/multipart.go`: Lines 364-366

---

### Issue: WAL File Size Limit (RESOLVED)

**Symptom:** "WAL file 0 exceeded ShardSize: 4202478 > 4194304" during large uploads.

**Root Cause:** WAL was comparing total file size (including fragment headers) against `ShardSize` which is data-only. Fragment headers add ~16KB overhead per 4MB shard.

**Solution:** Added `MaxFileSize()` function that calculates actual max including headers:
```go
func (wal *WAL) MaxFileSize() int64 {
    dataSize := int64(wal.Shard.ShardSize)
    numFragments := (dataSize + int64(wal.Shard.ChunkSize) - 1) / int64(wal.Shard.ChunkSize)
    headerOverhead := int64(wal.WALHeaderSize()) + numFragments*int64(FragmentHeaderBytes)
    return dataSize + headerOverhead
}
```

**Files Modified:**
- `s3/wal/wal.go`: Added `MaxFileSize()`, updated all size comparisons

---

### Issue: Concurrent Write Race Condition in WAL (RESOLVED)

**Symptom:** "shard num mismatch" errors during multipart uploads - fragments from different shards interleaved in WAL files.

**Root Cause:** Multiple concurrent `Write()` calls could read the same file offset before any writes completed, causing fragment interleaving.

**Solution:** Added exclusive lock for entire `Write()` operation:
```go
func (wal *WAL) Write(r io.Reader, totalSize int) (*WriteResult, error) {
    wal.mu.Lock()         // Exclusive lock prevents interleaving
    defer wal.mu.Unlock()
    // ... write fragments
}
```

**Files Modified:**
- `s3/wal/wal.go`: Added exclusive locking to `Write()`, removed internal lock calls to prevent deadlock

---

### Issue: Missing ShardIndex in Range Requests (RESOLVED)

**Symptom:** Range requests for shard 1+ returned 404, triggering expensive full object reconstructions. Download of 626MB file caused ~600MB memory allocation per 8MB range request.

**Root Cause:** `readRangeFromSingleShard()` was not passing `ShardIndex` in QUIC requests:
```go
// Before (broken):
objectRequest := quicserver.ObjectRequest{
    Bucket:     bucket,
    Object:     key,
    RangeStart: offsetInShard,
    RangeEnd:   endInShard,
    // ShardIndex missing! Defaults to 0
}
```

**Solution:** Added `ShardIndex` to range requests:
```go
// After (fixed):
objectRequest := quicserver.ObjectRequest{
    Bucket:     bucket,
    Object:     key,
    ShardIndex: shardIdx,  // Critical for correct shard lookup
    RangeStart: offsetInShard,
    RangeEnd:   endInShard,
}
```

**Files Modified:**
- `backend/distributed/get.go`: Lines 157-163

---

### Optimization: Parallel Part Retrieval in CompleteMultipartUpload

**Problem:** Sequential part retrieval made CompleteMultipartUpload slow for many parts, causing AWS CLI timeouts.

**Solution:** Added parallel part fetching with semaphore-controlled concurrency:
```go
const maxParallelPartFetches = 10
semaphore := make(chan struct{}, maxParallelPartFetches)

for i, part := range req.Parts {
    wg.Add(1)
    go func(idx int, partNum int) {
        defer wg.Done()
        semaphore <- struct{}{}
        defer func() { <-semaphore }()
        data, err := b.getPartData(ctx, ...)
        resultChan <- partResult{index: idx, data: data, err: err}
    }(i, part.PartNumber)
}
```

**Files Modified:**
- `backend/distributed/multipart.go`: Lines 317-365

---

## E2E Test Results (2026-01-24)

**Test File:** Ubuntu Noble Server Cloud Image (598MB)

**Upload:**
```bash
aws --no-verify-ssl --cli-read-timeout 600 --endpoint-url https://localhost:8443/ \
    s3 cp noble-server-cloudimg-amd64.img s3://predastore/
```
- 75 parts uploaded successfully
- CompleteMultipartUpload: ~2m45s (bottleneck: final object storage)

**Download:**
```bash
aws --no-verify-ssl --endpoint-url https://localhost:8443/ \
    s3 cp s3://predastore/noble-server-cloudimg-amd64.img downloaded.img
```
- Speed: ~350 MiB/s with range requests
- No full reconstructions needed (ShardIndex fix)

**Data Integrity:**
```
Original:   618d678ddaedf69e5aa2b36ec0199b4e
Downloaded: 618d678ddaedf69e5aa2b36ec0199b4e
```

---

## Key Insights

1. **Hash Consistency is Critical:** Any function that stores or retrieves shards must use the exact same hash for hash ring placement. The hash must incorporate both bucket and full key path.

2. **Compound Keys for Multi-Shard Objects:** When storing multiple shards of the same object, append the shard index to the object hash to create unique storage keys.

3. **ShardIndex Must Be Passed in All Requests:** Both PUT and GET/Range requests must include `ShardIndex` for correct shard identification on QUIC nodes.

4. **Test with Few Nodes:** Issues become apparent when testing with 3 nodes (RS 2+1 config) since multiple shards often land on the same node.

5. **WAL Locking:** The WAL `Write()` function must hold an exclusive lock for the entire operation to prevent fragment interleaving from concurrent writes.

6. **AWS CLI Timeout:** For large files, use `--cli-read-timeout 600` to prevent timeout during CompleteMultipartUpload.

7. **Debug Logging:** The `handlePUTShard` and `handleGET` logging with `shardIndex` and `shardNum` was essential for tracking shard placement.

## Test Commands

```bash
# Run all distributed tests
cd /home/ben/Development/mulga/predastore-rewrite && go test ./backend/distributed/... -v -count=1

# Run specific reconstruction test
go test ./backend/distributed/... -run TestPutGet_ReconstructionValidation -v -count=1

# Run multipart tests
go test ./backend/distributed/... -run TestDistributed_Multipart -v -count=1
```

## E2E Testing with AWS CLI

**IMPORTANT: Always stop dev environment before running unit tests!**

```bash
# Stop dev environment first (CRITICAL before running unit tests)
cd ~/Development/mulga/hive && ./scripts/stop-dev.sh

# Then run tests
cd ~/Development/mulga/predastore-rewrite && go test ./... -count=1
```

```bash
# Start dev environment for E2E testing
cd ~/Development/mulga/hive && ./scripts/start-dev.sh

# Upload large file (use extended timeout for CompleteMultipartUpload)
aws --no-verify-ssl --cli-read-timeout 600 --endpoint-url https://localhost:8443/ \
    s3 cp large-file.img s3://predastore/

# Download and verify
aws --no-verify-ssl --endpoint-url https://localhost:8443/ \
    s3 cp s3://predastore/large-file.img /tmp/downloaded.img
md5sum large-file.img /tmp/downloaded.img

# List bucket
aws --no-verify-ssl --endpoint-url https://localhost:8443/ s3 ls s3://predastore/

# Stop dev environment when done (required before running unit tests again)
./scripts/stop-dev.sh
```

---

# Performance

## Session: 2026-01-25

### Problem Statement

Distributed backend is **~10x slower** than filesystem backend. Uploading a ~4GB AMI file exposes significant performance bottlenecks that need optimization.

**Profile:** `tests/ami-import.prof` (457s duration, 97.7s CPU sampled - 21.36% utilization)

### Profiling Analysis

**Top 10 Bottlenecks by Cumulative CPU Time:**

| Rank | Function | Cum Time | % Total | Category |
|------|----------|----------|---------|----------|
| 1 | `syscall.Syscall6` | 48.12s | 49.25% | System Calls |
| 2 | `quicserver.handlePUTShard` | 29.33s | 30.02% | QUIC Server |
| 3 | `wal.Write` | 29.02s | 29.70% | WAL I/O |
| 4 | `wal.writeFragment` | 25.58s | 26.18% | WAL I/O |
| 5 | `os.File.Write` | 25.52s | 26.12% | File I/O |
| 6 | `quic-go.Conn.run` | 14.55s | 14.89% | QUIC Protocol |
| 7 | `runtime.findRunnable` | 11.34s | 11.61% | Scheduler |
| 8 | `crypto/md5.block` | 3.26s | 3.34% | Hashing |
| 9 | `runtime.memmove` | 3.36s | 3.44% | Memory Copy |
| 10 | `AES-GCM enc/dec` | 3.64s | 3.73% | QUIC Encryption |

**Key Observations:**

1. **WAL File I/O Dominates (29s)** - `wal.Write` and `writeFragment` are the biggest predastore-specific bottlenecks
2. **QUIC Protocol Overhead (14.5s+)** - Connection handling, packet processing, encryption
3. **Synchronous System Calls (48s)** - Excessive blocking syscalls suggest synchronous I/O patterns
4. **MD5 Hashing (3.26s)** - ETag generation adds noticeable overhead
5. **Memory Operations (4.87s)** - `memmove` + `memclr` indicate excessive copying
6. **Scheduler Overhead (11.34s)** - `runtime.findRunnable` suggests goroutine contention

### Performance Optimization TODO

#### Priority 1: WAL I/O Optimization (Target: 50%+ improvement)

- [ ] **1.1 Buffered WAL Writes** - Use `bufio.Writer` with larger buffer (64KB+)
  - Current: Direct `os.File.Write` for each fragment
  - Target: Batch writes, flush on shard completion
  - Files: `s3/wal/wal.go`

- [ ] **1.2 Async File Sync** - Defer `fsync` calls, batch multiple shards
  - Current: Sync after each write
  - Target: Periodic sync or sync on close
  - Risk: Data loss on crash (acceptable for temp WAL)

- [ ] **1.3 Pre-allocate WAL Files** - Use `fallocate` to avoid fragmentation
  - Pre-allocate ShardSize + header overhead on file creation
  - Files: `s3/wal/wal.go:createWALUnlocked`

- [ ] **1.4 Memory-Mapped WAL Option** - Use mmap for large files
  - Avoid syscalls for each write, let kernel manage paging
  - Consider for files > 1MB

#### Priority 2: QUIC Connection Optimization (Target: 30%+ improvement)

- [ ] **2.1 QUIC Connection Pooling** - Reuse connections to same nodes
  - Current: `quicclient.Dial()` creates new connection per PUT
  - Target: Pool of persistent connections per node
  - Files: `quic/quicclient/client.go`, `backend/distributed/put.go`

- [ ] **2.2 QUIC Stream Multiplexing** - Multiple shards per connection
  - Use QUIC's native stream multiplexing
  - Avoid connection setup overhead (TLS handshake)

- [ ] **2.3 Batch Shard Uploads** - Send multiple small shards in one batch
  - Reduce round-trips for small objects

- [ ] **2.4 Consider 0-RTT** - Enable QUIC 0-RTT resumption
  - Reduces handshake latency for repeated connections

#### Priority 3: Parallel Processing (Target: 20%+ improvement)

- [ ] **3.1 Parallel Shard Writes to QUIC** - Write all shards concurrently
  - Current: Sequential in some paths
  - Target: Use errgroup with controlled parallelism
  - Files: `backend/distributed/put.go:putObjectViaQUIC`

- [ ] **3.2 Pipeline Reed-Solomon + Upload** - Encode while uploading
  - Stream shards to QUIC as soon as encoded
  - Don't wait for all shards to be ready

- [ ] **3.3 Parallel WAL Writes per Shard** - Each shard writes to own file
  - Already somewhat parallel but verify no contention

#### Priority 4: Memory & Allocation (Target: 10%+ improvement)

- [ ] **4.1 Buffer Pooling** - Use `sync.Pool` for shard buffers
  - Reduce GC pressure from large allocations
  - Pool sizes: ChunkSize (64KB), ShardSize (4MB)
  - Files: `s3/wal/wal.go`, `backend/distributed/put.go`

- [ ] **4.2 Reduce Copies** - Pass slices instead of copying
  - Audit `memmove` sources in profile
  - Use `io.ReaderFrom` where possible

- [ ] **4.3 Pre-size Slices** - Use `make([]byte, 0, expectedSize)`
  - Avoid reallocation during append

#### Priority 5: Hashing Optimization (Target: 5%+ improvement)

- [ ] **5.1 Parallel MD5** - Compute ETag in parallel with upload
  - Don't block upload on hash computation
  - Use `io.TeeReader` to stream to hasher

- [ ] **5.2 Consider xxHash for Internal Use** - Faster non-crypto hash
  - MD5 only needed for S3 ETag compatibility
  - Use xxHash for internal checksums

- [ ] **5.3 Cache Object Hashes** - Store computed hashes in metadata
  - Avoid recomputation on read

#### Priority 6: Protocol Optimization

- [ ] **6.1 Larger QUIC Datagram Size** - Tune MTU settings
  - Reduce packet count for same data

- [ ] **6.2 Disable QUIC Encryption for Localhost** - Skip TLS for local dev
  - For dev/test mode only
  - Use insecure transport option

- [ ] **6.3 Binary Protocol for Metadata** - Replace JSON with protobuf/msgpack
  - Reduce parsing overhead for PutRequest/ObjectRequest

### Benchmark Commands

```bash
# Enable CPU profiling
PPROF_ENABLED=1 PPROF_OUTPUT=/tmp/predastore-cpu.prof ./hive service predastore start ...

# After test, analyze profile
go tool pprof -http=:8080 /tmp/predastore-cpu.prof

# Top functions by cumulative time
go tool pprof -top -cum tests/ami-import.prof

# Focus on predastore code
go tool pprof -text -focus="predastore" tests/ami-import.prof

# Generate flamegraph
go tool pprof -raw tests/ami-import.prof | flamegraph.pl > flame.svg
```

### Progress Tracking

| Optimization | Status | Before | After | Improvement |
|--------------|--------|--------|-------|-------------|
| 1.1 Periodic WAL Sync | DONE | O_SYNC per write | 200ms periodic fsync | ~10-50x (removes 5 syncs per block) |
| 2.1 Connection Pool | DONE | New conn per PUT | Pooled connections | TLS handshake reuse |
| 3.1 Parallel Shards | TODO | - | - | - |
| 4.1 Buffer Pooling | TODO | - | - | - |

#### Completed Optimizations

**1.1 Periodic WAL Sync (s3/wal/wal.go)**
- Removed `syscall.O_SYNC` from file open
- Added `DefaultWALSyncInterval = 200ms` (following PostgreSQL/BadgerDB/RocksDB patterns)
- Added `StartWALSyncer()` background goroutine for periodic fsync
- Added `dirty` atomic flag to track unflushed writes (avoids unnecessary syncs when idle)
- Added final sync on `Close()`

**2.1 QUIC Connection Pool (quic/quicclient/pool.go)**
- Created `Pool` type with connection reuse
- Added `DefaultPool` global instance
- Added `DialPooled()` function for pooled connections
- Updated distributed backend to use pooled connections in:
  - `distributed.go`: putObjectViaQUIC (data and parity shards)
  - `get.go`: readRangeFromSingleShard
  - `delete.go`: deleteObjectViaQUIC
- Background cleanup goroutine removes idle connections after 2 minutes

---

## Session: 2026-01-25 (Critical Bug Fixes)

### Issue: Image Import Hangs at ~364544 Blocks (QUIC Stream Exhaustion)

**Symptom:** Image import starts fast but hangs consistently at the same block number (~364544). Ctrl+C required to exit. Appears to be a deadlock.

**Root Cause:** QUIC streams were never being closed when `Get()` or `GetRange()` returned a body. With connection pooling enabled, streams accumulated on the reused connection until hitting QUIC's stream limit (~100 concurrent streams), causing `OpenStreamSync()` to block forever waiting for a free stream.

**The Bug in `quicclient.go`:**
```go
func (c *Client) do(...) (quicproto.Header, io.Reader, error) {
    s, err := c.conn.OpenStreamSync(ctx)  // Opens a new stream
    // ...
    if respHdr.BodyLen == 0 {
        _ = s.Close()  // Only closes when NO body!
    }
    return respHdr, br, nil  // Stream LEAKED when body exists!
}
```

**Why it wasn't caught before:** Without connection pooling, each request created a new connection. When the connection was closed, all its streams were automatically closed. With pooled connections staying open, leaked streams accumulated.

**Solution:** Return `io.ReadCloser` that closes the stream when the caller is done reading:

```go
type streamReadCloser struct {
    r      io.Reader
    closer io.Closer  // The QUIC stream
}

func (s *streamReadCloser) Close() error {
    return s.closer.Close()  // Releases stream back to connection
}

func (c *Client) do(...) (quicproto.Header, io.ReadCloser, error) {
    // ...
    // Return a ReadCloser that closes the stream when done
    return respHdr, &streamReadCloser{r: br, closer: s}, nil
}
```

**Files Modified:**
- `quic/quicclient/quicclient.go`:
  - Added `streamReadCloser` and `limitedReadCloser` wrapper types
  - Changed `do()` to return `io.ReadCloser`
  - Changed `Get()` and `GetRange()` to return `io.ReadCloser`
- `backend/distributed/get.go`: Added `defer reader.Close()` after `GetRange()`
- `backend/distributed/distributed.go`: Changed `c.Close()` to `reader.Close()` in `shardReaders()`

---

### Issue: shardReaders() Closing Connection Instead of Stream

**Symptom:** Connection pooling not working effectively; connections being closed after each shard read.

**Root Cause:** In `shardReaders()`, the code called `c.Close()` (closes the CLIENT CONNECTION) instead of closing the reader/stream:

```go
// BEFORE (wrong - defeats connection pooling):
data, err := io.ReadAll(reader)
c.Close()  // Closes the entire connection!

// AFTER (correct - only closes the stream):
data, err := io.ReadAll(reader)
reader.Close()  // Closes just the stream, connection stays pooled
```

**Files Modified:**
- `backend/distributed/distributed.go`: Line ~777

---

### Issue: WAL Sync Holding Lock During Disk I/O

**Symptom:** Under heavy write load, writes slow down progressively until they effectively stop.

**Root Cause:** `syncWALIfDirty()` held `RLock` while calling `file.Sync()`. Since `Sync()` can be slow (disk I/O), all `Write()` calls (which need exclusive `Lock()`) were blocked during sync.

```go
// BEFORE (deadlock-prone):
func (wal *WAL) syncWALIfDirty() {
    wal.mu.RLock()
    defer wal.mu.RUnlock()  // Held during slow I/O!
    activeWAL.Sync()  // Can take 10-100ms on slow disk
}

// AFTER (lock-free during I/O):
func (wal *WAL) syncWALIfDirty() {
    wal.mu.RLock()
    fileToSync = wal.Shard.DB[len(wal.Shard.DB)-1]
    wal.mu.RUnlock()  // Release BEFORE I/O

    fileToSync.Sync()  // Slow I/O doesn't block writes
}
```

**Files Modified:**
- `s3/wal/wal.go`: `syncWALIfDirty()` now releases lock before calling `Sync()`

**Tests Added:**
- `TestConcurrentWritesWithSync` - 10 concurrent writers with aggressive 10ms sync interval
- `TestSyncDoesNotBlockWrites` - Verifies writes complete during sync

---

### Issue: Connection Pool Not Detecting Closed Connections

**Symptom:** After QUIC server restart, pooled connections fail with "Application error 0x0 (local): done"

**Root Cause:** Pool checked `conn != nil` but not whether the connection was actually alive.

**Solution:** Check `conn.Context().Err()` which is canceled when connection closes:

```go
// Check connection is still alive
if pc.client.conn != nil && pc.client.conn.Context().Err() == nil {
    return pc.client, nil  // Connection is alive
}
// Connection is dead, create new one
```

**Files Modified:**
- `quic/quicclient/pool.go`: Added connection health check in `Get()` and `createConnection()`

---

## Key Lessons Learned

1. **Stream vs Connection:** In QUIC, closing a CONNECTION closes all streams. Closing a STREAM releases just that stream. With connection pooling, you must close STREAMS, not connections.

2. **io.ReadCloser Contract:** When returning readers that wrap resources (streams, file handles), always return `io.ReadCloser` and document that callers MUST call `Close()`.

3. **Lock Scope:** Never hold locks during I/O operations. Get what you need under lock, release lock, then do I/O.

4. **Test Connection Pooling:** Unit tests with single connections don't catch stream exhaustion. Need tests that reuse connections across many requests.

5. **Consistent Block Numbers:** When a hang occurs at the same point repeatedly, it's likely a resource exhaustion issue (streams, file descriptors, memory), not a race condition.

---

## Session: 2026-01-25 (Continued Investigation)

### Issue: Image Import Still Hanging Despite Stream Fixes

**Status:** INVESTIGATING

**Symptom:** Image import still hangs at same block (~364544) even after QUIC stream closing fixes.

**Debug Logging Added:**

To trace the issue, detailed logging was added throughout the PUT lifecycle:

1. **QUIC Client (`quic/quicclient/quicclient.go`)**
   - `Put()` - logs start, doPut result, completion
   - `doPut()` - logs stream open, body streaming, flush, response received
   - Pool logging - connection reuse count

2. **QUIC Server (`quic/quicserver/server.go`)**
   - `handleStream()` - tracks active stream count with atomic counter
   - `handlePUTShard()` - logs start, WAL write progress, response sent
   - Logs when activeStreams > 50 or every 100 streams

3. **S3 Router (`s3/routes.go`)**
   - `PutObject` route - logs request start, backend call, completion

4. **Distributed Backend (`backend/distributed/put.go`, `distributed.go`)**
   - `PutObject()` - logs start, temp file creation, shard distribution
   - `putObjectViaQUIC()` - logs file size, data/parity shard completion counts

**To Test:**
```bash
# Rebuild and restart
cd ~/Development/mulga/hive && ./scripts/start-dev.sh

# Run import with DEBUG logging
LOG_LEVEL=debug ./bin/hive admin images import --arch x86_64 --distro ubuntu \
    --file ~/isos/noble-server-cloudimg-amd64.img --version 24.04

# Check predastore logs
tail -f ~/Development/mulga/hive/data/logs/predastore.log | grep -E "(PUT|stream|shard)"
```

**What to Look For:**
- `handleStream: active streams` - if count keeps increasing, stream leak
- `doPut: body flushed, waiting for response` - if this logs but no `response header received`, server didn't respond
- `handlePUTShard: response sent` - if this logs but client doesn't receive, network/QUIC issue
- `putObjectViaQUIC: all data shards completed` - if this never logs, one shard goroutine is stuck

---

## Development Environment

**IMPORTANT: Always stop dev environment before running unit tests!**

```bash
# Stop all services (CRITICAL before running unit tests)
cd ~/Development/mulga/hive && ./scripts/stop-dev.sh

# Verify ports are free
lsof -i :9991  # Should return nothing

# Run unit tests
cd ~/Development/mulga/predastore-rewrite && go test ./... -count=1

# Start dev environment for E2E testing
cd ~/Development/mulga/hive && ./scripts/start-dev.sh

# Test image import
./bin/hive admin images import --arch x86_64 --distro ubuntu \
    --file ~/isos/noble-server-cloudimg-amd64.img --version 24.04

# Stop when done
./scripts/stop-dev.sh
```

---
