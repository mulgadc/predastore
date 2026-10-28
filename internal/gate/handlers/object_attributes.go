package handlers

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/mulgadc/predastore/internal/gate/model"
)

// userMetadataPrefix starts every user metadata header, in the lowercase S3
// stores and returns it in.
const userMetadataPrefix = "x-amz-meta-"

// maxUserMetadataSize is S3's limit on user metadata: the bytes of every name
// and value together.
const maxUserMetadataSize = 2 << 10

// defaultContentType is what S3 serves for an object written without one.
const defaultContentType = "binary/octet-stream"

// ObjectAttributes are the headers S3 stores with an object and serves back on
// HEAD and GET. A header the writer did not send is empty.
type ObjectAttributes struct {
	ContentType        string
	CacheControl       string
	ContentDisposition string
	ContentEncoding    string
	ContentLanguage    string
	Expires            string

	// Metadata is the user metadata, keyed by the lowercased name that follows
	// x-amz-meta-.
	Metadata map[string]string
}

// storedHeader is a system header S3 keeps from a write, serves back on a read,
// and lets a signed read replace with a response-* query parameter.
type storedHeader struct {
	name     string // as sent and served
	record   string // as the placement record names it
	override string // the query parameter that replaces it in a response
	field    func(*ObjectAttributes) *string
}

var storedHeaders = []storedHeader{
	{"Content-Type", "content-type", "response-content-type",
		func(a *ObjectAttributes) *string { return &a.ContentType }},
	{"Cache-Control", "cache-control", "response-cache-control",
		func(a *ObjectAttributes) *string { return &a.CacheControl }},
	{"Content-Disposition", "content-disposition", "response-content-disposition",
		func(a *ObjectAttributes) *string { return &a.ContentDisposition }},
	{"Content-Encoding", "content-encoding", "response-content-encoding",
		func(a *ObjectAttributes) *string { return &a.ContentEncoding }},
	{"Content-Language", "content-language", "response-content-language",
		func(a *ObjectAttributes) *string { return &a.ContentLanguage }},
	{"Expires", "expires", "response-expires",
		func(a *ObjectAttributes) *string { return &a.Expires }},
}

// storedHeaderRecorded returns the stored header a placement record names.
func storedHeaderRecorded(record string) (storedHeader, bool) {
	for _, sh := range storedHeaders {
		if sh.record == record {
			return sh, true
		}
	}
	return storedHeader{}, false
}

func (a ObjectAttributes) empty() bool {
	for _, sh := range storedHeaders {
		if *sh.field(&a) != "" {
			return false
		}
	}
	return len(a.Metadata) == 0
}

// attributesFromRequest reads what a write says about its object. Names are
// lowercased, as S3 stores them, and a header sent more than once is joined
// with commas the way HTTP folds repeated fields.
//
// Only user metadata counts towards the metadata limit, as in S3.
func attributesFromRequest(h http.Header) (ObjectAttributes, error) {
	var a ObjectAttributes
	for _, sh := range storedHeaders {
		*sh.field(&a) = strings.Join(h.Values(sh.name), ",")
	}
	a.ContentEncoding = withoutAWSChunked(a.ContentEncoding)

	size := 0
	for name, values := range h {
		key, ok := strings.CutPrefix(strings.ToLower(name), userMetadataPrefix)
		if !ok {
			continue
		}
		value := strings.Join(values, ",")
		if a.Metadata == nil {
			a.Metadata = make(map[string]string)
		}
		// Two spellings of one name only reach here when a client bypassed
		// canonicalisation; folding them is what a single header would get.
		if prev, dup := a.Metadata[key]; dup {
			value = prev + "," + value
			size -= len(key) + len(prev)
		}
		a.Metadata[key] = value
		size += len(key) + len(value)
	}
	if size > maxUserMetadataSize {
		return ObjectAttributes{}, model.ErrMetadataTooLargeError
	}
	return a, nil
}

// withoutAWSChunked removes the aws-chunked coding from a Content-Encoding.
// It describes the upload's framing, not the object, so S3 does not store it;
// a value without it is kept exactly as sent.
func withoutAWSChunked(encoding string) string {
	var kept []string
	found := false
	for coding := range strings.SplitSeq(encoding, ",") {
		coding = strings.TrimSpace(coding)
		switch {
		case strings.EqualFold(coding, "aws-chunked"):
			found = true
		case coding != "":
			kept = append(kept, coding)
		}
	}
	if !found {
		return encoding
	}
	return strings.Join(kept, ", ")
}

// setAttributeHeaders serves an object's attributes. The metadata names are
// assigned rather than Set, because Set canonicalises them and SDKs keep the
// case of whatever follows the prefix as the metadata key.
func setAttributeHeaders(h http.Header, a ObjectAttributes) {
	if a.ContentType == "" {
		a.ContentType = defaultContentType
	}
	for _, sh := range storedHeaders {
		if v := *sh.field(&a); v != "" {
			h.Set(sh.name, v)
		}
	}
	for key, value := range a.Metadata {
		h[userMetadataPrefix+key] = []string{metadataHeaderValue(value)}
	}
}

// checkResponseOverrides refuses a read that names response-* parameters
// without being signed. A presigned URL is signed, so it may use them.
func checkResponseOverrides(r *http.Request) error {
	if SignedPayloadFrom(r.Context()).Signed {
		return nil
	}
	q := r.URL.Query()
	for _, sh := range storedHeaders {
		if q.Get(sh.override) != "" {
			return model.ErrAnonymousResponseOverrideError
		}
	}
	return nil
}

// withResponseOverrides returns the attributes a read serves once its
// response-* parameters replace the stored ones. The object is unchanged.
func withResponseOverrides(a ObjectAttributes, q url.Values) ObjectAttributes {
	for _, sh := range storedHeaders {
		if v := q.Get(sh.override); v != "" {
			*sh.field(&a) = v
		}
	}
	return a
}

// metadataHeaderValue renders a stored value the way S3 returns it. ASCII goes
// out as sent; anything else is read as the Latin-1 an HTTP header carries and
// returned as one RFC 2047 UTF-8 encoded word, so the header stays ASCII.
func metadataHeaderValue(v string) string {
	ascii := true
	for i := range len(v) {
		if v[i] >= utf8.RuneSelf {
			ascii = false
			break
		}
	}
	if ascii {
		return v
	}
	runes := make([]rune, len(v))
	for i := range len(v) {
		runes[i] = rune(v[i])
	}
	return "=?UTF-8?B?" + base64.StdEncoding.EncodeToString([]byte(string(runes))) + "?="
}
