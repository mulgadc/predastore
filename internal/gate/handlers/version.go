package handlers

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
)

// VersioningEnabled and VersioningSuspended are the two states S3 names. A
// bucket that has never been versioned is neither, and reports no status at
// all — which is distinct from Suspended, because a suspended bucket still
// holds and serves the versions it accumulated.
const (
	VersioningEnabled   = "Enabled"
	VersioningSuspended = "Suspended"
)

// nullVersionID is the id S3 gives an object stored while a bucket was not
// versioned, and the id a suspended bucket writes under.
//
// It maps to the unversioned object hash, which is what makes both of those
// work without a migration: an object written before versioning was enabled is
// already stored under that hash, and a suspended write replaces the null
// version in place exactly as an unversioned write does today.
const nullVersionID = "null"

// versionIDBytes is the entropy behind a version id. Ids are opaque on S3 and
// opaque here: nothing may parse one, so this only has to be unique.
const versionIDBytes = 15

// versionIDEncoding renders an id without padding or case, so it survives a
// URL, a header and an XML document untouched.
var versionIDEncoding = base32.HexEncoding.WithPadding(base32.NoPadding)

// VersionRecord is one row of the version index: everything a listing reports
// about a version without reading its placement record.
//
// A delete marker carries no size and no etag because it has none — it is the
// record of a key having been deleted, not an object of length zero.
type VersionRecord struct {
	VersionID string `json:"version_id"`
	// WriteEpoch orders this version against every other version of the key.
	// LastModified cannot: it is the epoch truncated to a millisecond, so two
	// versions written in the same millisecond -- an SDK retry, a fast
	// overwrite -- would tie and their order would fall to the random id.
	WriteEpoch   uint64    `json:"write_epoch"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	LastModified time.Time `json:"last_modified"`
	DeleteMarker bool      `json:"delete_marker"`
}

// versionObjectHash names the shard set holding one version.
func versionObjectHash(bucket, key, versionID string) [32]byte {
	if versionID == nullVersionID {
		return model.ObjectHash(bucket, key)
	}
	return model.VersionHash(bucket, key, versionID)
}

// mintVersionID returns a fresh, opaque version id.
func mintVersionID() (string, error) {
	buf := make([]byte, versionIDBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return versionIDEncoding.EncodeToString(buf), nil
}

// versionKeyPrefix starts every index row for one key.
//
// The NUL terminator is what keeps one key's scan off its own neighbours: a
// prefix of "photo" would otherwise sweep up "photos/2024" as well. S3 keys are
// UTF-8 and cannot contain NUL, so nothing a caller can name reaches past it.
func versionKeyPrefix(bucket, key string) string {
	return bucket + "/" + key + "\x00"
}

// versionBucketPrefix starts every index row in a bucket.
func versionBucketPrefix(bucket string) string {
	return bucket + "/"
}

// versionIndexKey composes the index row for one version.
//
// The epoch is inverted so rows sort newest-first, which is the order
// ListObjectVersions reports and the order the current version is found in. The
// epoch rather than its millisecond because it is monotonic and structurally
// unique -- it carries the minting node and a per-millisecond sequence -- so two
// versions written in the same millisecond still sort in the order they were
// written. The id is appended so distinct versions cannot share a row.
func versionIndexKey(bucket, key string, epoch uint64, versionID string) string {
	inverted := math.MaxUint64 - epoch
	return fmt.Sprintf("%s%016x%s", versionKeyPrefix(bucket, key), inverted, versionID)
}

// splitVersionIndexKey recovers the object key from an index row. The bucket is
// already known by the caller that built the scan prefix.
func splitVersionIndexKey(bucket, stored string) (key string, ok bool) {
	rest, found := strings.CutPrefix(stored, versionBucketPrefix(bucket))
	if !found {
		return "", false
	}
	key, _, found = strings.Cut(rest, "\x00")
	return key, found
}

// bucketVersioning reports a bucket's versioning state, empty for a bucket that
// has never been versioned.
func bucketVersioning(ctx context.Context, mc MetaClient, bucket string) (string, error) {
	data, err := metaGet(ctx, mc, model.TableBucketVersioning, bucket)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return "", nil
		}
		return "", err
	}
	return string(data), nil
}

// setBucketVersioning records a bucket's versioning state.
func setBucketVersioning(ctx context.Context, mc MetaClient, bucket, status string) error {
	return metaPut(ctx, mc, model.TableBucketVersioning, bucket, []byte(status))
}

// putVersionRecord appends one version to the index.
func putVersionRecord(ctx context.Context, mc MetaClient, bucket, key string, rec VersionRecord) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&rec); err != nil {
		return err
	}
	return metaPut(ctx, mc, model.TableObjectVersions,
		versionIndexKey(bucket, key, rec.WriteEpoch, rec.VersionID), buf.Bytes())
}

// deleteVersionRecord drops one version from the index. A row that is not there
// is not an error: a delete that raced another delete has still reached the
// state it was asked for.
func deleteVersionRecord(ctx context.Context, mc MetaClient, bucket, key string, rec VersionRecord) error {
	err := metaDelete(ctx, mc, model.TableObjectVersions,
		versionIndexKey(bucket, key, rec.WriteEpoch, rec.VersionID))
	if err != nil && !errors.Is(err, meta.ErrNotFound) {
		return err
	}
	return nil
}

// keyVersions returns every version of one key, newest first.
//
// The null version is never indexed. It is defined as the placement record
// under the unversioned object hash, which is what makes an object written
// before versioning was enabled a version without a migration to create one,
// and what makes a suspended write replace the null version by the ordinary
// unversioned write path rather than by a second mechanism.
func keyVersions(ctx context.Context, mc MetaClient, bucket, key string) ([]VersionRecord, error) {
	items, err := metaScan(ctx, mc, model.TableObjectVersions, versionKeyPrefix(bucket, key), 0)
	if err != nil {
		return nil, err
	}

	records := make([]VersionRecord, 0, len(items)+1)
	for _, item := range items {
		rec, err := decodeVersionRecord(item.Value)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}

	null, ok, err := nullVersion(ctx, mc, bucket, key)
	if err != nil {
		return nil, err
	}
	if ok {
		records = append(records, null)
	}

	// The index rows already sort newest-first among themselves; the null
	// version is dated by its placement record and can fall anywhere among them.
	sort.SliceStable(records, func(i, j int) bool {
		return records[i].WriteEpoch > records[j].WriteEpoch
	})
	return records, nil
}

// nullVersion reports the null version of a key, if the key has one.
func nullVersion(ctx context.Context, mc MetaClient, bucket, key string) (VersionRecord, bool, error) {
	hash := model.ObjectHash(bucket, key)
	data, err := metaGet(ctx, mc, model.TableObjects, string(hash[:]))
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return VersionRecord{}, false, nil
		}
		return VersionRecord{}, false, err
	}

	place, err := DecodePlacement(data)
	if err != nil {
		return VersionRecord{}, false, err
	}

	rec := VersionRecord{VersionID: nullVersionID, Size: place.Size}
	if etag, ok := place.ETag(); ok {
		rec.ETag = etag
	}
	// A version 1 placement record carries no time: its epoch is eight random
	// bytes, which would sort anywhere. Leaving both at zero sorts it oldest,
	// which is the only claim that record supports.
	if at, ok := place.ModifiedAt(); ok {
		rec.LastModified = at
		rec.WriteEpoch = place.WriteEpoch
	}
	return rec, true, nil
}

// decodeVersionRecord reads one stored index row.
func decodeVersionRecord(data []byte) (VersionRecord, error) {
	var rec VersionRecord
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&rec); err != nil {
		return VersionRecord{}, err
	}
	return rec, nil
}

// writeTarget is where one write lands, decided before any byte moves: the
// shards are addressed by the hash, so it cannot be chosen after the fact.
type writeTarget struct {
	// versionID is empty on an unversioned bucket, so a handler can tell
	// "no versioning" from the null version without consulting the state again.
	versionID string
	hash      [32]byte
	// versioned is true only when the bucket is Enabled, which is the one case
	// that must not release the generation it replaced — there is nothing
	// superseded, the previous version is a different shard set and still live.
	versioned bool
}

// resolveWriteTarget decides which shard set a write lands in.
//
// Suspended is deliberately the unversioned path: it writes the null version,
// under the unversioned hash, replacing whatever null version was there. That
// is S3's semantics, and routing it through the existing path rather than a
// parallel one means a suspended bucket cannot drift from an unversioned one.
func resolveWriteTarget(ctx context.Context, mc MetaClient, bucket, key string) (writeTarget, error) {
	status, err := bucketVersioning(ctx, mc, bucket)
	if err != nil {
		return writeTarget{}, err
	}
	if status != VersioningEnabled {
		target := writeTarget{hash: model.ObjectHash(bucket, key)}
		// A suspended bucket names the version it wrote; a bucket that has never
		// been versioned has no version to name.
		if status == VersioningSuspended {
			target.versionID = nullVersionID
		}
		return target, nil
	}

	versionID, err := mintVersionID()
	if err != nil {
		return writeTarget{}, err
	}
	return writeTarget{
		versionID: versionID,
		hash:      model.VersionHash(bucket, key, versionID),
		versioned: true,
	}, nil
}

// indexWrite records a committed version in the index, so it survives the next
// write to the same key. Only a versioned write has anything to record: the
// null version is the placement record itself.
func indexWrite(ctx context.Context, mc MetaClient, bucket, key string, target writeTarget, place ObjectToShardNodes) error {
	if !target.versioned {
		return nil
	}

	rec := VersionRecord{VersionID: target.versionID, WriteEpoch: place.WriteEpoch, Size: place.Size}
	if etag, ok := place.ETag(); ok {
		rec.ETag = etag
	}
	if at, ok := place.ModifiedAt(); ok {
		rec.LastModified = at
	} else {
		rec.LastModified = time.Now().UTC()
	}
	return putVersionRecord(ctx, mc, bucket, key, rec)
}

// versionIDHeader is the header S3 returns naming the version a request wrote
// or read. It is omitted entirely on an unversioned bucket rather than sent
// empty, because "null" is a real version id and an empty one is not.
const versionIDHeader = "X-Amz-Version-Id"

// setVersionIDHeader names the version a response concerns, when there is one.
func setVersionIDHeader(h http.Header, versionID string) {
	if versionID != "" {
		h.Set(versionIDHeader, versionID)
	}
}

// errDeleteMarker reports that the current version of a key is a delete marker:
// the key is absent, but not because it was never written.
var errDeleteMarker = errors.New("current version is a delete marker")

// readTarget names the shard set a read should dial, and the version it is.
type readTarget struct {
	hash      [32]byte
	versionID string
}

// resolveReadTarget decides which version a read concerns.
//
// An unversioned bucket keeps the path it has always had — the hash straight
// from the key, no extra state read — because that is the hot path and nothing
// about it has changed. Only a bucket that has ever been versioned pays for the
// listing-key indirection, and only that bucket can need it.
func resolveReadTarget(ctx context.Context, mc MetaClient, bucket, key, versionID string) (readTarget, error) {
	if versionID != "" {
		return readTarget{hash: versionObjectHash(bucket, key, versionID), versionID: versionID}, nil
	}

	status, err := bucketVersioning(ctx, mc, bucket)
	if err != nil {
		return readTarget{}, err
	}
	if status == "" {
		return readTarget{hash: model.ObjectHash(bucket, key)}, nil
	}

	current, err := currentVersion(ctx, mc, bucket, key)
	if err != nil {
		return readTarget{}, err
	}
	return current, nil
}

// currentVersion resolves the current version of a key on a versioned bucket.
//
// The listing key is the record of what is current, and its absence is the
// record of the key having been deleted. Falling back to the unversioned hash
// when it is missing would resurrect an object a delete marker is hiding.
func currentVersion(ctx context.Context, mc MetaClient, bucket, key string) (readTarget, error) {
	data, err := metaGet(ctx, mc, model.TableObjects, objectARN(bucket, key))
	if err == nil && len(data) == sha256.Size {
		return readTarget{hash: [32]byte(data), versionID: versionIDOfHash(ctx, mc, bucket, key, [32]byte(data))}, nil
	}
	if err != nil && !errors.Is(err, meta.ErrNotFound) {
		return readTarget{}, err
	}

	// No listing key. Either a delete marker hides the key, or it was never
	// written; only the index can tell those apart.
	versions, err := keyVersions(ctx, mc, bucket, key)
	if err != nil {
		return readTarget{}, err
	}
	if len(versions) > 0 && versions[0].DeleteMarker {
		return readTarget{versionID: versions[0].VersionID}, errDeleteMarker
	}
	return readTarget{}, meta.ErrNotFound
}

// versionIDOfHash names the version a hash belongs to, for the headers a read
// reports. An unindexed hash is the null version, which is the unversioned
// placement record and carries no index row by construction.
func versionIDOfHash(ctx context.Context, mc MetaClient, bucket, key string, hash [32]byte) string {
	if hash == model.ObjectHash(bucket, key) {
		return nullVersionID
	}
	versions, err := keyVersions(ctx, mc, bucket, key)
	if err != nil {
		return ""
	}
	for _, rec := range versions {
		if versionObjectHash(bucket, key, rec.VersionID) == hash {
			return rec.VersionID
		}
	}
	return ""
}

// deleteMarkerHeader tells a client that the key is absent because it was
// deleted, not because it was never written. Without it a versioned 404 is
// indistinguishable from a key that never existed.
const deleteMarkerHeader = "X-Amz-Delete-Marker"

// handleVersionedReadErr answers a read that resolved to no readable object.
//
// A delete marker is still a 404 — the key has no current version — but it
// carries the marker header and the marker's own version id, which is what a
// client needs to delete the marker and bring the object back.
func handleVersionedReadErr(w http.ResponseWriter, r *http.Request, key string, target readTarget, err error) {
	if errors.Is(err, errDeleteMarker) {
		w.Header().Set(deleteMarkerHeader, "true")
		setVersionIDHeader(w.Header(), target.versionID)
		HandleError(w, r, model.ErrNoSuchKeyError.WithResource(key))
		return
	}
	if errors.Is(err, meta.ErrNotFound) {
		HandleError(w, r, model.ErrNoSuchKeyError.WithResource(key))
		return
	}
	HandleError(w, r, model.NewS3Error(model.ErrInternalError, err.Error(), 500))
}
