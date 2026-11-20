package handlers

import "net/http"

// conditionalWriteHeaders are the preconditions S3 evaluates on a write or a
// delete. This gate does not evaluate them, and ignoring one would let a write
// or delete through that the caller asked to be refused.
var conditionalWriteHeaders = []string{"If-Match", "If-None-Match"}

// refuseConditionalWrite answers 501 to a write or delete carrying a
// precondition, and reports whether it did. Call it before reading the body.
func refuseConditionalWrite(w http.ResponseWriter, r *http.Request) bool {
	for _, h := range conditionalWriteHeaders {
		if r.Header.Get(h) != "" {
			WriteS3Error(w, r, http.StatusNotImplemented, "NotImplemented",
				"Conditional writes and deletes are not implemented")
			return true
		}
	}
	return false
}
