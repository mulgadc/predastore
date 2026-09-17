package handlers

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"

	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/gate/model"
)

const (
	// maxDeleteObjects is the batch size S3 accepts. Beyond it the request is
	// rejected rather than trimmed, so a client cannot believe it deleted keys
	// the server never looked at.
	maxDeleteObjects = 1000

	// deleteObjectsBodyLimit bounds the request document. A full batch of keys
	// at S3's 1024-byte maximum is a little over a megabyte, so this is four
	// times the largest legitimate body and no basis for an allocation a
	// malformed length could drive.
	deleteObjectsBodyLimit = 4 << 20

	// deleteObjectsWorkers bounds how many keys are deleted at once. Each one
	// is a metadata read, a shard fan-out and two metadata writes, and a batch
	// of a thousand run one at a time is the round-trip cost this operation
	// exists to remove.
	deleteObjectsWorkers = 8
)

// DeleteObjects serves POST /{bucket}?delete: the batch delete.
//
// Every key is reported on independently. One that cannot be deleted produces
// an Error entry beside the Deleted entries for the rest and never fails the
// request, which is what lets a client emptying a bucket make progress.
func DeleteObjects(mc MetaClient, bc BlobClient, cache *BucketCache, cfg Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		resource, ok := routedBucket(w, r)
		if !ok {
			return
		}
		bucket := resource.Name

		if err := requireBucket(ctx, mc, cache, bucket); err != nil {
			HandleError(w, r, err)
			return
		}

		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, deleteObjectsBodyLimit))
		if err != nil {
			// A rewritten body keeps its own error rather than being reported as bad XML.
			if errors.Is(err, sigv4.ErrContentSHA256Mismatch) {
				HandleError(w, r, err)
				return
			}

			HandleError(w, r, model.NewS3Error(model.ErrMalformedXML,
				"The delete request could not be read", 400))
			return
		}

		var request DeleteRequest
		if err := xml.Unmarshal(body, &request); err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrMalformedXML,
				"The XML you provided was not well-formed or did not validate against our published schema", 400))
			return
		}
		if len(request.Objects) == 0 || len(request.Objects) > maxDeleteObjects {
			HandleError(w, r, model.NewS3Error(model.ErrMalformedXML,
				fmt.Sprintf("A delete request must name between 1 and %d objects", maxDeleteObjects), 400))
			return
		}

		outcomes := deleteBatch(ctx, mc, bc, cfg, bucket, request.Objects)

		result := DeleteResult{}
		for i, object := range request.Objects {
			key := object.Key
			// A key that was never there is reported as deleted: a client
			// emptying a bucket races its own listing, and failing it for a key
			// that is already gone gives it nothing to do about the answer.
			if outcomes[i].err != nil && !errors.Is(outcomes[i].err, model.ErrNoSuchKeyError) {
				code, message := deleteFailure(outcomes[i].err)
				result.Errors = append(result.Errors, DeleteError{Key: key, Code: code, Message: message})
				continue
			}
			if request.Quiet {
				continue
			}
			deleted := DeletedObject{Key: key}
			// A delete that appended a marker names the marker; one that
			// destroyed a version names the version. They are different answers
			// and a client emptying a versioned bucket has to tell them apart.
			if outcomes[i].deleteMarker {
				deleted.DeleteMarker = true
				deleted.DeleteMarkerVersionId = outcomes[i].versionID
			} else {
				deleted.VersionId = outcomes[i].versionID
			}
			result.Deleted = append(result.Deleted, deleted)
		}

		if len(result.Errors) > 0 {
			slog.WarnContext(ctx, "Batch delete reported failures",
				"bucket", bucket, "requested", len(request.Objects), "failed", len(result.Errors))
		}

		if err := writeXML(w, http.StatusOK, result); err != nil {
			slog.DebugContext(ctx, "failed to write XML response", "error", err)
		}
	})
}

// deleteBatch deletes each key and returns the outcomes in request order. The
// results are written by index rather than collected, so the answer follows the
// order the client asked in without a lock over it.
//
// Entries are dealt out by key, not one at a time. Deleting a version reconciles
// which version of that key is current, and two of those running concurrently
// each decide it from a view taken before the other's delete landed -- which is
// exactly the shape a client emptying a versioned bucket sends, every version of
// every key in one request. Keys still run in parallel with each other.
func deleteBatch(
	ctx context.Context, mc MetaClient, bc BlobClient, cfg Config, bucket string, objects []DeleteRequestObject,
) []batchOutcome {
	outcomes := make([]batchOutcome, len(objects))

	byKey := map[string][]int{}
	order := make([]string, 0, len(objects))
	for i, object := range objects {
		if _, seen := byKey[object.Key]; !seen {
			order = append(order, object.Key)
		}
		byKey[object.Key] = append(byKey[object.Key], i)
	}

	workers := min(deleteObjectsWorkers, len(order))
	keys := make(chan string)

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for key := range keys {
				for _, i := range byKey[key] {
					// A request that ran out of time reports the keys it never
					// reached rather than claiming them deleted.
					if err := ctx.Err(); err != nil {
						outcomes[i] = batchOutcome{err: err}
						continue
					}
					outcome, err := deleteObjectVersion(ctx, mc, bc, cfg, bucket, objects[i].Key, objects[i].VersionId)
					outcomes[i] = batchOutcome{deleteOutcome: outcome, err: err}
				}
			}
		})
	}

	for _, key := range order {
		keys <- key
	}
	close(keys)
	wg.Wait()

	return outcomes
}

// batchOutcome is one key's result: what the delete did, and whether it failed.
type batchOutcome struct {
	deleteOutcome

	err error
}

// deleteFailure renders one key's failure as the code and message its Error
// entry carries. An error the delete path did not classify is InternalError
// rather than the raw text, which can name internal state.
func deleteFailure(err error) (string, string) {
	if s3err, ok := model.IsS3Error(err); ok {
		return string(s3err.Code), s3err.Message
	}
	return string(model.ErrInternalError), "The object could not be deleted"
}
