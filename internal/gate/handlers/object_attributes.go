package handlers

import (
	"encoding/base64"
	"net/http"
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
// HEAD and GET.
type ObjectAttributes struct {
	// ContentType is empty when the writer sent none.
	ContentType string

	// Metadata is the user metadata, keyed by the lowercased name that follows
	// x-amz-meta-.
	Metadata map[string]string
}

func (a ObjectAttributes) empty() bool {
	return a.ContentType == "" && len(a.Metadata) == 0
}

// attributesFromRequest reads what a write says about its object. Names are
// lowercased, as S3 stores them, and a header sent more than once is joined
// with commas the way HTTP folds repeated fields.
func attributesFromRequest(h http.Header) (ObjectAttributes, error) {
	a := ObjectAttributes{ContentType: h.Get("Content-Type")}
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

// setAttributeHeaders serves an object's attributes. The metadata names are
// assigned rather than Set, because Set canonicalises them and SDKs keep the
// case of whatever follows the prefix as the metadata key.
func setAttributeHeaders(h http.Header, a ObjectAttributes) {
	contentType := a.ContentType
	if contentType == "" {
		contentType = defaultContentType
	}
	h.Set("Content-Type", contentType)
	for key, value := range a.Metadata {
		h[userMetadataPrefix+key] = []string{metadataHeaderValue(value)}
	}
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
