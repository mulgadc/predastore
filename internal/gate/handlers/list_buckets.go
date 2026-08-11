package handlers

import (
	"bytes"
	"encoding/gob"
	"log/slog"
	"net/http"

	"github.com/mulgadc/predastore/internal/gate/auth"
	"github.com/mulgadc/predastore/internal/gate/model"
)

// ListBuckets serves GET /: every bucket the caller's account owns.
func ListBuckets(mc MetaClient) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		accountID := auth.AccountID(ctx)

		items, err := metaScan(mc, model.TableBuckets, "", 0)
		if err != nil {
			HandleError(w, r, model.NewS3Error(model.ErrInternalError, "failed to list buckets: "+err.Error(), 500))
			return
		}

		displayName := "Predastore"
		if accountID != "" {
			displayName = accountID
		}
		result := ListBucketsResult{
			Owner: BucketOwner{ID: accountID, DisplayName: displayName},
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
			if accountID != "" && metadata.AccountID != accountID {
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
