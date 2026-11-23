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

// An ETag condition is matched as S3 matches it: the value is split on quotes,
// commas, spaces and tabs, and any piece equal to the ETag or "*" matches.
// Verifies: Q-68.
func TestPreconditionETagListsMatchAsS3Does(t *testing.T) {
	etag := `"abc"`
	matching := []string{`abc`, `"abc"`, ` "abc" `, `"x", "abc"`, `, ,"abc"`, `W/"abc"`,
		`"abc", "x`, `"x""abc"`, `"abc" "x"`, `"abc"x`, `x"abc"y`, `junk "abc"`, `"abc`,
		`"other,abc,tag"`, `"abc,"`, `"x,y" ,abc`, "\tabc", `*, "x"`, `"x", *`}
	for _, list := range matching {
		assert.Equal(t, preconditionsHold, evalPreconditions(etag, true, time.Time{}, false, preconditions{ifMatch: list}), list)
		assert.Equal(t, preconditionNotModified, evalPreconditions(etag, true, time.Time{}, false, preconditions{ifNoneMatch: list}), list)
	}
	missing := []string{`other"unterminated`, `xabc`, `abcx`, `"xabc"`, `"ab"`, `abc;x`, `"x", "y"`}
	for _, list := range missing {
		assert.Equal(t, preconditionFailed, evalPreconditions(etag, true, time.Time{}, false, preconditions{ifMatch: list}), list)
		assert.Equal(t, preconditionsHold, evalPreconditions(etag, true, time.Time{}, false, preconditions{ifNoneMatch: list}), list)
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
