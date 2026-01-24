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

```bash
# Start dev environment
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

# Stop dev environment
./scripts/stop-dev.sh
```
