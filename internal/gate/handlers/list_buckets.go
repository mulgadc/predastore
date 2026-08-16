package handlers

import (
	"bytes"
	"encoding/gob"
	"log/slog"
	"net/http"
	"regexp"

	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/model"
)

// OwnerAccountHeader asks for another account's buckets. It is honoured only
// for config-defined service accounts and ignored for everyone else, which is
// what keeps the tenant bucket namespace from becoming an enumeration oracle.
const OwnerAccountHeader = "X-Predastore-Owner-Account-Id"

// accountIDPattern is predastore's account id form. A value that is not one
// would list nothing, and a caller tearing an account down reads an empty
// listing as "no buckets" — so a malformed owner is refused rather than
// answered.
var accountIDPattern = regexp.MustCompile(`^[0-9]{12}$`)

// ListBuckets serves GET /: every bucket owned by the caller's account, or by
// the account named in OwnerAccountHeader when the caller is a service account.
func ListBuckets(mc MetaClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		owner, err := listBucketsOwner(r)
		if err != nil {
			HandleError(w, r, err)
			return
		}

		items, err := metaScan(ctx, mc, model.TableBuckets, "", 0)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "failed to list buckets: "+err.Error(), 500))
			return
		}

		result := ListBucketsResult{
			Owner: BucketOwner{ID: owner, DisplayName: owner},
		}

		// A corrupt row is skipped rather than failing the listing: one bad bucket
		// must not hide every other bucket the account owns.
		seen := make(map[string]bool, len(items))
		for _, item := range items {
			var metadata model.BucketMetadata
			if err := gob.NewDecoder(bytes.NewReader(item.Value)).Decode(&metadata); err != nil {
				slog.WarnContext(ctx, "Skipping corrupt bucket entry during scan", "key", item.Key, "error", err)
				continue
			}
			if metadata.AccountID != owner {
				continue
			}
			if seen[metadata.Name] {
				continue
			}
			seen[metadata.Name] = true
			result.Buckets = append(result.Buckets, ListBucket{
				Name:         metadata.Name,
				CreationDate: metadata.CreationDate,
			})
		}

		if err := writeXML(w, http.StatusOK, result); err != nil {
			slog.DebugContext(ctx, "failed to write XML response", "error", err)
		}
	})
}

// listBucketsOwner resolves whose buckets to list. There is deliberately no
// value that means "every account": the owner is always exactly one account,
// and a request that cannot name one is refused rather than answered broadly.
func listBucketsOwner(r *http.Request) (string, error) {
	ctx := r.Context()
	caller := auth.AccountID(ctx)

	requested := r.Header.Get(OwnerAccountHeader)
	if requested == "" {
		if caller == "" {
			return "", model.NewS3Error(model.ErrAccessDenied, "Access Denied", http.StatusForbidden)
		}
		return caller, nil
	}

	if !auth.IsServiceAccount(ctx) {
		// Ignored, not refused: an ordinary caller must not be able to tell a
		// non-existent account from one it is not allowed to see.
		slog.WarnContext(ctx, "Ignoring owner override from a non-service account",
			"callerAccountID", caller, "requestedAccountID", requested)
		if caller == "" {
			return "", model.NewS3Error(model.ErrAccessDenied, "Access Denied", http.StatusForbidden)
		}
		return caller, nil
	}

	if !accountIDPattern.MatchString(requested) {
		return "", model.NewS3Error(model.ErrInvalidArgument,
			OwnerAccountHeader+" must be a 12-digit account id", http.StatusBadRequest)
	}
	return requested, nil
}
