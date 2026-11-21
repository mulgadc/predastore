package handlers

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// A record written before digests and timestamps were stored still answers
// every condition: "*" tests existence, a literal ETag never names it, and the
// date conditions are skipped.
// Verifies: Q-68.
func TestPreconditionsOnALegacyRecord(t *testing.T) {
	when := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Format(http.TimeFormat)
	cases := []struct {
		name string
		p    preconditions
		want preconditionResult
	}{
		{"If-Match star", preconditions{ifMatch: "*"}, preconditionsHold},
		{"If-Match literal", preconditions{ifMatch: `"abc"`}, preconditionFailed},
		{"If-None-Match star", preconditions{ifNoneMatch: "*"}, preconditionNotModified},
		{"If-None-Match literal", preconditions{ifNoneMatch: `"abc"`}, preconditionsHold},
		{"If-Modified-Since", preconditions{ifModifiedSince: when}, preconditionsHold},
		{"If-Unmodified-Since", preconditions{ifUnmodifiedSince: when}, preconditionsHold},
	}
	// The earlier time would fail If-Modified-Since and the later one
	// If-Unmodified-Since, so only the missing flag can make both pass.
	for _, behind := range []time.Time{
		time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	} {
		for _, c := range cases {
			t.Run(c.name+" "+behind.Format("2006"), func(t *testing.T) {
				assert.Equal(t, c.want, evalPreconditions("", false, behind, false, c.p))
			})
		}
	}
}

// A comma inside one quoted entity tag is part of that tag, so a tag that
// merely contains the record's digest between commas does not name it.
// Verifies: Q-68.
func TestPreconditionQuotedTagsAreCompared(t *testing.T) {
	etag := `"abc"`
	for _, list := range []string{`"other,abc,tag"`, `"abc,"`, `",abc"`} {
		assert.Equal(t, preconditionFailed, evalPreconditions(etag, true, time.Time{}, false, preconditions{ifMatch: list}), list)
		assert.Equal(t, preconditionsHold, evalPreconditions(etag, true, time.Time{}, false, preconditions{ifNoneMatch: list}), list)
	}
	for _, list := range []string{`"x", "abc"`, `"x,y" ,abc`, `abc`, ` "abc" `} {
		assert.Equal(t, preconditionsHold, evalPreconditions(etag, true, time.Time{}, false, preconditions{ifMatch: list}), list)
	}
}

// Last-Modified is served to the second, so a condition dated with the value a
// client was given must compare equal to the sub-second write time behind it.
// Verifies: Q-68.
func TestPreconditionDatesCompareToTheSecondServed(t *testing.T) {
	modified := time.Date(2026, 1, 2, 3, 4, 5, 900_000_000, time.UTC)
	served := modified.Format(http.TimeFormat)

	assert.Equal(t, preconditionNotModified,
		evalPreconditions(`"e"`, true, modified, true, preconditions{ifModifiedSince: served}))
	assert.Equal(t, preconditionsHold,
		evalPreconditions(`"e"`, true, modified, true, preconditions{ifUnmodifiedSince: served}))
}

// Every HTTP date form RFC 9110 requires a recipient to accept is honoured,
// and repeated If-Match headers are read as one list.
// Verifies: Q-68.
func TestPreconditionHeaderForms(t *testing.T) {
	modified := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, form := range []string{http.TimeFormat, time.RFC850, time.ANSIC} {
		p := preconditions{ifUnmodifiedSince: modified.Add(-time.Hour).Format(form)}
		assert.Equal(t, preconditionFailed, evalPreconditions(`"e"`, true, modified, true, p), form)
	}

	h := http.Header{}
	h.Add("If-Match", `"other"`)
	h.Add("If-Match", `"e"`)
	assert.Equal(t, preconditionsHold, evalPreconditions(`"e"`, true, modified, true, readPreconditions(h)))
}

// A malformed list fails either condition rather than being read as a
// non-match, so it can never let a read or a copy through.
// Verifies: Q-68.
func TestPreconditionMalformedListsFail(t *testing.T) {
	etag := `"abc"`
	for _, list := range []string{`"x""abc"`, `"abc" "x"`, `"abc`, `"abc"x`, `"abc", "x`, `other"unterminated`, `"abc", x"y`} {
		assert.Equal(t, preconditionFailed, evalPreconditions(etag, true, time.Time{}, false, preconditions{ifMatch: list}), list)
		assert.Equal(t, preconditionFailed, evalPreconditions(etag, true, time.Time{}, false, preconditions{ifNoneMatch: list}), list)
	}
}
