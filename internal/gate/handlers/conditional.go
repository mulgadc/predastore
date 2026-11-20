package handlers

import (
	"net/http"
	"strings"
	"time"
)

// conditionalWriteHeaders are the preconditions S3 evaluates on a write or a
// delete. This gate does not evaluate them, and ignoring one would let a write
// or delete through that the caller asked to be refused.
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

// etagListMatches reports whether any member of a comma-separated ETag list
// names the record, quoted or not. "*" names any record that exists.
func etagListMatches(list, etag string, hasETag bool) bool {
	for member := range strings.SplitSeq(list, ",") {
		member = strings.TrimSpace(member)
		if member == "*" {
			return true
		}
		if hasETag && member != "" && strings.Trim(member, `"`) == strings.Trim(etag, `"`) {
			return true
		}
	}
	return false
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
