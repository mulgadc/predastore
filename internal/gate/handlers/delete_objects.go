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
func DeleteObjects(mc MetaClient, bc BlobClient, cache *BucketCache) http.Handler {
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

		outcomes := deleteBatch(ctx, mc, bc, bucket, request.Objects)

		result := DeleteResult{}
		for i, object := range request.Objects {
			key := object.Key
			// A key that was never there is reported as deleted: a client
			// emptying a bucket races its own listing, and failing it for a key
			// that is already gone gives it nothing to do about the answer.
			if outcomes[i] != nil && !errors.Is(outcomes[i], model.ErrNoSuchKeyError) {
				code, message := deleteFailure(outcomes[i])
				result.Errors = append(result.Errors, DeleteError{Key: key, Code: code, Message: message})
				continue
			}
			if request.Quiet {
				continue
			}
			result.Deleted = append(result.Deleted, DeletedObject{Key: key})
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
func deleteBatch(
	ctx context.Context, mc MetaClient, bc BlobClient, bucket string, objects []DeleteRequestObject,
) []error {
	outcomes := make([]error, len(objects))

	workers := min(deleteObjectsWorkers, len(objects))
	indices := make(chan int)

	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for i := range indices {
				// A request that ran out of time reports the keys it never
				// reached rather than claiming them deleted.
				if err := ctx.Err(); err != nil {
					outcomes[i] = err
					continue
				}
				outcomes[i] = deleteStoredObject(ctx, mc, bc, bucket, objects[i].Key)
			}
		}()
	}

	for i := range objects {
		indices <- i
	}
	close(indices)
	wg.Wait()

	return outcomes
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
