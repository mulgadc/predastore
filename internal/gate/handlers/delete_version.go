package handlers

import (
	"context"
	"errors"

	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/meta"
)

// deleteOutcome is what a delete did, which on a versioned bucket is not
// implied by the request: the same call either hides a key behind a new delete
// marker or destroys one named version.
type deleteOutcome struct {
	versionID    string
	deleteMarker bool
}

// deleteObjectVersion performs one delete, choosing between the three things
// DELETE means depending on the bucket's state and whether a version is named.
//
// On an unversioned bucket it is the delete it has always been. On a versioned
// bucket an unqualified delete destroys nothing: it appends a delete marker and
// drops the listing key, so the key leaves ListObjects while every version it
// ever had stays readable by id. Naming a version is the only permanent delete.
func deleteObjectVersion(ctx context.Context, mc MetaClient, bc BlobClient, cfg Config, bucket, key, versionID string) (deleteOutcome, error) {
	status, err := bucketVersioning(ctx, mc, bucket)
	if err != nil {
		return deleteOutcome{}, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}

	if versionID != "" {
		return deleteNamedVersion(ctx, mc, bc, bucket, key, versionID)
	}

	// Suspended still accumulates delete markers: what it stops is new versions,
	// not the record of a deletion. A suspended bucket's marker takes the null
	// id, replacing any null version, which is S3's rule and the reason a
	// suspended delete can destroy data where an enabled one cannot.
	switch status {
	case VersioningEnabled:
		return appendDeleteMarker(ctx, mc, cfg, bucket, key, "")
	case VersioningSuspended:
		if err := deleteStoredVersion(ctx, mc, bc, bucket, key, model.ObjectHash(bucket, key)); err != nil && !isNoSuchKey(err) {
			return deleteOutcome{}, err
		}
		return appendDeleteMarker(ctx, mc, cfg, bucket, key, nullVersionID)
	default:
		return deleteOutcome{}, deleteStoredObject(ctx, mc, bc, bucket, key)
	}
}

// appendDeleteMarker hides a key behind a new delete marker.
//
// The marker takes an epoch from the same minter an object write uses, because
// it is ordered against those writes: a marker dated any other way could sort
// behind a version written in the same millisecond, and the key would read as
// present when the last thing that happened to it was a delete.
//
// The marker is indexed before the listing key is dropped. The other order
// leaves a window in which the key is gone from ListObjects and nothing records
// why, which reads as data loss rather than as a delete.
func appendDeleteMarker(ctx context.Context, mc MetaClient, cfg Config, bucket, key, versionID string) (deleteOutcome, error) {
	if versionID == "" {
		minted, err := mintVersionID()
		if err != nil {
			return deleteOutcome{}, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
		}
		versionID = minted
	}

	epoch, err := cfg.Epochs.Next()
	if err != nil {
		return deleteOutcome{}, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}

	rec := VersionRecord{
		VersionID:    versionID,
		WriteEpoch:   epoch,
		LastModified: EpochTime(epoch),
		DeleteMarker: true,
	}
	if err := putVersionRecord(ctx, mc, bucket, key, rec); err != nil {
		return deleteOutcome{}, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}

	if err := metaDelete(ctx, mc, model.TableObjects, objectARN(bucket, key)); err != nil && !errors.Is(err, meta.ErrNotFound) {
		return deleteOutcome{}, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}

	return deleteOutcome{versionID: versionID, deleteMarker: true}, nil
}

// deleteNamedVersion destroys one version permanently.
//
// Removing the current version promotes the next-newest into the listing key,
// so a key whose newest version is deleted reappears at its predecessor rather
// than vanishing while it still has versions.
func deleteNamedVersion(ctx context.Context, mc MetaClient, bc BlobClient, bucket, key, versionID string) (deleteOutcome, error) {
	versions, err := keyVersions(ctx, mc, bucket, key)
	if err != nil {
		return deleteOutcome{}, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}

	index := -1
	for i, rec := range versions {
		if rec.VersionID == versionID {
			index = i
			break
		}
	}
	if index < 0 {
		return deleteOutcome{}, model.ErrNoSuchKeyError.WithResource(key)
	}
	target := versions[index]

	if target.DeleteMarker {
		if err := deleteVersionRecord(ctx, mc, bucket, key, target); err != nil {
			return deleteOutcome{}, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
		}
	} else {
		hash := versionObjectHash(bucket, key, versionID)
		if err := deleteStoredVersion(ctx, mc, bc, bucket, key, hash); err != nil && !isNoSuchKey(err) {
			return deleteOutcome{}, err
		}
		// The null version has no index row; it was the placement record itself.
		if versionID != nullVersionID {
			if err := deleteVersionRecord(ctx, mc, bucket, key, target); err != nil {
				return deleteOutcome{}, model.NewS3Error(model.ErrInternalError, err.Error(), 500)
			}
		}
	}

	if index == 0 {
		if err := promoteCurrent(ctx, mc, bucket, key, versions[1:]); err != nil {
			return deleteOutcome{}, err
		}
	}

	return deleteOutcome{versionID: versionID, deleteMarker: target.DeleteMarker}, nil
}

// promoteCurrent points the listing key at the newest surviving version, or
// removes it when the newest is a delete marker or nothing is left.
func promoteCurrent(ctx context.Context, mc MetaClient, bucket, key string, remaining []VersionRecord) error {
	if len(remaining) == 0 || remaining[0].DeleteMarker {
		if err := metaDelete(ctx, mc, model.TableObjects, objectARN(bucket, key)); err != nil && !errors.Is(err, meta.ErrNotFound) {
			return model.NewS3Error(model.ErrInternalError, err.Error(), 500)
		}
		return nil
	}

	hash := versionObjectHash(bucket, key, remaining[0].VersionID)
	if err := metaPut(ctx, mc, model.TableObjects, objectARN(bucket, key), hash[:]); err != nil {
		return model.NewS3Error(model.ErrInternalError, err.Error(), 500)
	}
	return nil
}

// isNoSuchKey reports whether an error is the absent-key error, which a delete
// treats as work already done rather than as a failure.
func isNoSuchKey(err error) bool {
	if s3err, ok := errors.AsType[*model.S3Error](err); ok {
		return s3err.Code == model.ErrNoSuchKey
	}
	return false
}
