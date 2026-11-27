package handlers

import (
	"net/http"
	"strings"
	"time"
)

// conditionalWriteHeaders are the preconditions S3 evaluates on a write or a
// delete. Only PutObject evaluates them; elsewhere, ignoring one would let a
// write or delete through that the caller asked to be refused.
var conditionalWriteHeaders = []string{"If-Match", "If-None-Match"}

// refuseConditionalWrite answers 501 to a write or delete carrying a
// precondition, even an empty one, and reports whether it did.
func refuseConditionalWrite(w http.ResponseWriter, r *http.Request) bool {
	for _, h := range conditionalWriteHeaders {
		if _, present := r.Header[h]; present {
			WriteS3Error(w, r, http.StatusNotImplemented, "NotImplemented",
				"Conditional writes and deletes are not implemented")
			return true
		}
	}
	return false
}

// preconditions are a read's or a copy source's conditions, as sent.
type preconditions struct {
	ifMatch, ifNoneMatch               string
	ifModifiedSince, ifUnmodifiedSince string
}

// readPreconditions takes the conditions a GetObject or HeadObject carries.
// Repeated ETag headers are one list, as RFC 9110 reads them.
func readPreconditions(h http.Header) preconditions {
	return preconditions{
		ifMatch:           strings.Join(h.Values("If-Match"), ","),
		ifNoneMatch:       strings.Join(h.Values("If-None-Match"), ","),
		ifModifiedSince:   h.Get("If-Modified-Since"),
		ifUnmodifiedSince: h.Get("If-Unmodified-Since"),
	}
}

type preconditionResult int

const (
	preconditionsHold preconditionResult = iota
	// preconditionNotModified is 304 on a read and 412 on a copy source.
	preconditionNotModified
	preconditionFailed
)

// evalPreconditions applies S3's precedence: If-Match decides over
// If-Unmodified-Since, and If-None-Match over If-Modified-Since. A record with
// no ETag matches only "*", and one with no time skips the date conditions.
func evalPreconditions(etag string, hasETag bool, modified time.Time, hasModified bool, p preconditions) preconditionResult {
	// Last-Modified is served to the second, so it is compared to the second.
	modified = modified.Truncate(time.Second)

	if p.ifMatch != "" {
		if !etagListMatches(p.ifMatch, etag, hasETag) {
			return preconditionFailed
		}
	} else if since, err := http.ParseTime(p.ifUnmodifiedSince); err == nil && hasModified && modified.After(since) {
		return preconditionFailed
	}

	if p.ifNoneMatch != "" {
		if etagListMatches(p.ifNoneMatch, etag, hasETag) {
			return preconditionNotModified
		}
	} else if since, err := http.ParseTime(p.ifModifiedSince); err == nil && hasModified && !modified.After(since) {
		return preconditionNotModified
	}

	return preconditionsHold
}

// etagListMatches reports whether an ETag condition names the record. As S3
// does, the value is split on quotes, commas, spaces and tabs, and any piece
// equal to the ETag or "*" matches, so no value is rejected as malformed.
func etagListMatches(list, etag string, hasETag bool) bool {
	if hasPiece(list, "*") {
		return true
	}
	want := strings.Trim(etag, `"`)
	return hasETag && want != "" && !strings.ContainsAny(want, etagSeparators) && hasPiece(list, want)
}

const etagSeparators = "\", \t"

// hasPiece reports whether piece occurs in list bounded by separators or the
// ends, which is splitting on them without walking every byte. A hit without a
// leading separator resumes after the next separator, so the search stays linear.
func hasPiece(list, piece string) bool {
	for off := 0; ; {
		i := strings.Index(list[off:], piece)
		if i < 0 {
			return false
		}
		start, end := off+i, off+i+len(piece)
		if (start == 0 || isETagSeparator(list[start-1])) && (end == len(list) || isETagSeparator(list[end])) {
			return true
		}
		next := strings.IndexAny(list[start:], etagSeparators)
		if next < 0 {
			return false
		}
		off = start + next + 1
	}
}

func isETagSeparator(c byte) bool {
	return c == '"' || c == ',' || c == ' ' || c == '\t'
}

// preconditionFailedMessage is S3's text for a 412.
const preconditionFailedMessage = "At least one of the pre-conditions you specified did not hold"

// answerReadPreconditions writes the 304 or 412 a read's conditions call for
// and reports whether it did. A 304 names the object it did not send.
func answerReadPreconditions(w http.ResponseWriter, r *http.Request, place ObjectToShardNodes) bool {
	etag, hasETag := place.ETag()
	modified, hasModified := place.ModifiedAt()
	switch evalPreconditions(etag, hasETag, modified, hasModified, readPreconditions(r.Header)) {
	case preconditionNotModified:
		if hasETag {
			w.Header().Set("ETag", etag)
		}
		w.Header().Set("Last-Modified", lastModified(place))
		w.WriteHeader(http.StatusNotModified)
		return true
	case preconditionFailed:
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusPreconditionFailed)
			return true
		}
		WriteS3Error(w, r, http.StatusPreconditionFailed, "PreconditionFailed", preconditionFailedMessage)
		return true
	default:
		return false
	}
}

// copySourcePreconditions takes the x-amz-copy-source-if-* conditions a copy
// carries, which are evaluated against its source.
func copySourcePreconditions(h http.Header) preconditions {
	return preconditions{
		ifMatch:           strings.Join(h.Values("X-Amz-Copy-Source-If-Match"), ","),
		ifNoneMatch:       strings.Join(h.Values("X-Amz-Copy-Source-If-None-Match"), ","),
		ifModifiedSince:   h.Get("X-Amz-Copy-Source-If-Modified-Since"),
		ifUnmodifiedSince: h.Get("X-Amz-Copy-Source-If-Unmodified-Since"),
	}
}

// refuseCopySource answers 412 when a copy's source conditions do not hold,
// and reports whether it did. A copy has no 304: S3 answers 412 for both.
func refuseCopySource(w http.ResponseWriter, r *http.Request, source ObjectToShardNodes) bool {
	etag, hasETag := source.ETag()
	modified, hasModified := source.ModifiedAt()
	if evalPreconditions(etag, hasETag, modified, hasModified, copySourcePreconditions(r.Header)) == preconditionsHold {
		return false
	}
	WriteS3Error(w, r, http.StatusPreconditionFailed, "PreconditionFailed", preconditionFailedMessage)
	return true
}
