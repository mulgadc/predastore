package s3

import (
	"bytes"
	"encoding/gob"
	"log/slog"
	"net/http"

	"github.com/mulgadc/predastore/internal/gateway/model"
)

// listBuckets serves GET /: every bucket the caller's account owns.
func (s *HTTP2Server) listBuckets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	accountID := ""
	if v := ctx.Value(ContextKeyAccountID); v != nil {
		accountID, _ = v.(string)
	}

	items, err := s.stateScan(model.TableBuckets, "", 0)
	if err != nil {
		s.handleError(w, r, model.NewS3Error(model.ErrInternalError, "failed to list buckets: "+err.Error(), 500))
		return
	}

	displayName := "Predastore"
	if accountID != "" {
		displayName = accountID
	}
	result := ListBuckets{
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

	if err := s.writeXML(w, http.StatusOK, result); err != nil {
		slog.DebugContext(ctx, "failed to write XML response", "error", err)
	}
}
