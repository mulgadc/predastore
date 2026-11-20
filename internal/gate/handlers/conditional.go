package handlers

import "net/http"

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
