package sigv4_test

import (
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mulgadc/predastore/pkg/sigv4"
	"pgregory.net/rapid"
)

// A fault mutates a signed request in place and returns the sentinel Verify must then yield. It
// returns skip=true when it does not apply to the request's auth mode, telling the test to move
// on. It receives presigned/region/service to pick the mutation and error for the mode and endpoint.
type fault func(t *rapid.T, r *http.Request, presigned bool, region, service string) (skip bool, want error)

// amzTime formats t as an X-Amz-Date value.
func amzTime(t time.Time) string { return t.UTC().Format("20060102T150405Z") }

// faults corrupts a signed request in every way Verify must reject. A single fault covers both
// auth modes, branching on presigned, or skips the mode it cannot touch.
var faults = map[string]fault{
	"tampered signature": func(t *rapid.T, r *http.Request, presigned bool, _, _ string) (bool, error) {
		// Flip a hex digit of the signature, wherever the mode carries it.
		if presigned {
			setQuery(r, "X-Amz-Signature", corruptHexDigit(t, r.URL.Query().Get("X-Amz-Signature")))
		} else {
			const marker = "Signature="
			auth := r.Header.Get("Authorization")
			i := strings.LastIndex(auth, marker)
			r.Header.Set("Authorization", auth[:i+len(marker)]+corruptHexDigit(t, auth[i+len(marker):]))
		}
		return false, sigv4.ErrSignatureMismatch
	},
	"changed method": func(t *rapid.T, r *http.Request, _ bool, _, _ string) (bool, error) {
		// Any standard method other than the signed one.
		var others []string
		for _, m := range []string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodHead} {
			if m != r.Method {
				others = append(others, m)
			}
		}
		r.Method = rapid.SampledFrom(others).Draw(t, "newMethod")
		return false, sigv4.ErrSignatureMismatch
	},
	"injected query param": func(t *rapid.T, r *http.Request, _ bool, _, _ string) (bool, error) {
		// "inj-" keeps the key off the request's own harsh query parameters.
		key := rapid.StringMatching(`inj-[a-z]{1,6}`).Draw(t, "injectKey")
		setQuery(r, key, rapid.StringMatching(`[a-zA-Z0-9]{1,6}`).Draw(t, "injectVal"))
		return false, sigv4.ErrSignatureMismatch
	},
	"added unsigned x-amz header": func(t *rapid.T, r *http.Request, _ bool, _, _ string) (bool, error) {
		// The "Inj-" infix keeps the name off any signed X-Amz-Meta-* header.
		suffix := rapid.StringMatching(`[a-z]{1,6}`).Draw(t, "injectHeader")
		r.Header.Set("X-Amz-Meta-Inj-"+suffix, rapid.StringMatching(`[a-zA-Z0-9]{1,6}`).Draw(t, "injectHeaderValue"))
		return false, sigv4.ErrUnsignedHeader
	},
	"removed authentication": func(_ *rapid.T, r *http.Request, presigned bool, _, _ string) (bool, error) {
		// Drop the marker that names the scheme; without it neither path authenticates.
		if presigned {
			delQuery(r, "X-Amz-Algorithm")
		} else {
			r.Header.Del("Authorization")
		}
		return false, sigv4.ErrMissingAuthentication
	},
	"unsupported algorithm": func(t *rapid.T, r *http.Request, presigned bool, _, _ string) (bool, error) {
		algo := rapid.SampledFrom([]string{"AWS4-ECDSA-P256-SHA256", "AWS4-HMAC-SHA1", "AWS3-HMAC-SHA256", "BOGUS-ALGO"}).Draw(t, "algorithm")
		// Overwrite the algorithm, wherever the mode carries it.
		if presigned {
			setQuery(r, "X-Amz-Algorithm", algo)
		} else {
			_, rest, _ := strings.Cut(r.Header.Get("Authorization"), " ")
			r.Header.Set("Authorization", algo+" "+rest)
		}
		return false, sigv4.ErrUnsupportedAlgorithm
	},
	"wrong credential region": func(_ *rapid.T, r *http.Request, presigned bool, region, _ string) (bool, error) {
		// A region that cannot equal the signed one, so Verify's scope check fires.
		rewriteCredential(r, presigned, func(p []string) []string { p[2] = "x" + region; return p })
		return false, sigv4.ErrMalformedAuthorization
	},
	"wrong credential service": func(_ *rapid.T, r *http.Request, presigned bool, _, service string) (bool, error) {
		rewriteCredential(r, presigned, func(p []string) []string { p[3] = "x" + service; return p })
		return false, sigv4.ErrMalformedAuthorization
	},
	"malformed credential scope": func(t *rapid.T, r *http.Request, presigned bool, _, _ string) (bool, error) {
		kind := rapid.SampledFrom([]string{"drop", "extra", "bad-terminator"}).Draw(t, "scopeMutation")
		keep := rapid.IntRange(1, 4).Draw(t, "keepParts")
		rewriteCredential(r, presigned, func(p []string) []string {
			switch kind {
			case "drop":
				return p[:keep]
			case "extra":
				return append(p, "extra")
			default:
				p[4] = "not_aws4_request"
				return p
			}
		})
		return false, sigv4.ErrMalformedAuthorization
	},
	"unparseable date": func(t *rapid.T, r *http.Request, presigned bool, _, _ string) (bool, error) {
		bad := rapid.StringMatching(`[A-Za-z]{3,10}`).Draw(t, "badDate")
		// A garbage date is a malformed URL when presigned, an invalid request time otherwise.
		if presigned {
			setQuery(r, "X-Amz-Date", bad)
			return false, sigv4.ErrMalformedPresignedURL
		}
		r.Header.Set("X-Amz-Date", bad)
		return false, sigv4.ErrRequestTimeInvalid
	},
	"tampered payload hash": func(t *rapid.T, r *http.Request, presigned bool, _, _ string) (bool, error) {
		// x-amz-content-sha256 is a header-auth concern; presigned signs UNSIGNED-PAYLOAD.
		if presigned {
			return true, nil
		}
		r.Header.Set("X-Amz-Content-Sha256", corruptHexDigit(t, r.Header.Get("X-Amz-Content-Sha256")))
		return false, sigv4.ErrSignatureMismatch
	},
	"removed content-sha256": func(_ *rapid.T, r *http.Request, presigned bool, _, service string) (bool, error) {
		// Presigned requests carry no content-sha256 header to remove.
		if presigned {
			return true, nil
		}
		r.Header.Del("X-Amz-Content-Sha256")
		// Mandatory only for S3; elsewhere its removal merely breaks the signature.
		if service == "s3" {
			return false, sigv4.ErrMissingContentSHA256
		}
		return false, sigv4.ErrSignatureMismatch
	},
	"skewed date": func(t *rapid.T, r *http.Request, presigned bool, _, _ string) (bool, error) {
		// Header auth enforces clock skew; presigned uses the expiry window (see "expired").
		if presigned {
			return true, nil
		}
		d := sigv4.MaxClockSkew + time.Duration(rapid.IntRange(1, 2880).Draw(t, "skewMinutes"))*time.Minute
		if !rapid.Bool().Draw(t, "skewAhead") {
			d = -d
		}
		r.Header.Set("X-Amz-Date", amzTime(oracleTime.Add(d)))
		return false, sigv4.ErrRequestTimeTooSkewed
	},
	"expired": func(t *rapid.T, r *http.Request, presigned bool, _, _ string) (bool, error) {
		if !presigned {
			return true, nil
		}
		// Either well past expiry (behind) or too far ahead; both expire. The behind offset is
		// measured from the request's own X-Amz-Expires window, which reqGen randomizes.
		extra := time.Duration(rapid.IntRange(1, 48).Draw(t, "expiredHours")) * time.Hour
		if rapid.Bool().Draw(t, "expiredAhead") {
			setQuery(r, "X-Amz-Date", amzTime(oracleTime.Add(sigv4.MaxClockSkew+extra)))
			return false, sigv4.ErrPresignedURLExpired
		}
		expires, _ := strconv.Atoi(r.URL.Query().Get("X-Amz-Expires"))
		setQuery(r, "X-Amz-Date", amzTime(oracleTime.Add(-time.Duration(expires)*time.Second-extra)))
		return false, sigv4.ErrPresignedURLExpired
	},
	"expiry beyond max": func(t *rapid.T, r *http.Request, presigned bool, _, _ string) (bool, error) {
		if !presigned {
			return true, nil
		}
		maxExpires := int(sigv4.MaxPresignAge / time.Second)
		setQuery(r, "X-Amz-Expires", strconv.Itoa(maxExpires+rapid.IntRange(1, 100000).Draw(t, "expiryExcess")))
		return false, sigv4.ErrMalformedPresignedURL
	},
	"removed expires": func(_ *rapid.T, r *http.Request, presigned bool, _, _ string) (bool, error) {
		if !presigned {
			return true, nil
		}
		delQuery(r, "X-Amz-Expires")
		return false, sigv4.ErrMalformedPresignedURL
	},
}

// TestVerifyRejectsFaults checks that every fault rejects with its sentinel, proving the
// signature covers what it claims and that scope, time, and framing checks fire. Each fault
// runs against a fresh signature over the same randomized request (any service, any auth mode).
func TestVerifyRejectsFaults(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		gr := reqGen().Draw(t, "req")

		// Stable fault order so rapid's draw sequence stays deterministic across replays.
		names := make([]string, 0, len(faults))
		for name := range faults {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, name := range names {
			// Fresh signed request per fault: the mutation is destructive.
			req := sign(t, gr)
			skip, want := faults[name](t, req, gr.Presigned, gr.Region, gr.Service)
			if skip {
				continue
			}

			if got := parseVerify(req, gr.Region, gr.Service, oracleTime); !errors.Is(got, want) {
				t.Fatalf("fault %q: got %v, want %v", name, got, want)
			}
		}
	})
}

// rewriteCredential rewrites the request's 5-part credential scope through fn, for either auth mode.
func rewriteCredential(r *http.Request, presigned bool, fn func(parts []string) []string) {
	if presigned {
		// Presigned carries the scope in the X-Amz-Credential query parameter.
		parts := strings.Split(r.URL.Query().Get("X-Amz-Credential"), "/")
		setQuery(r, "X-Amz-Credential", strings.Join(fn(parts), "/"))
		return
	}

	// Header auth carries it in the Authorization header's Credential= element.
	const prefix = "Credential="
	auth := r.Header.Get("Authorization")
	i := strings.Index(auth, prefix)
	rest := auth[i+len(prefix):]
	j := strings.Index(rest, ",")
	scope := strings.Join(fn(strings.Split(rest[:j], "/")), "/")
	r.Header.Set("Authorization", auth[:i+len(prefix)]+scope+rest[j:])
}

// corruptHexDigit replaces one byte of s with a different hex digit, yielding a guaranteed-different
// string; s need not be entirely hex.
func corruptHexDigit(t *rapid.T, s string) string {
	const hexits = "0123456789abcdef"
	i := rapid.IntRange(0, len(s)-1).Draw(t, "hexIndex")
	// A non-hex byte maps to index 0; the non-zero offset still lands on a different digit.
	cur := max(strings.IndexByte(hexits, s[i]), 0)
	offset := rapid.IntRange(1, 15).Draw(t, "hexOffset")
	b := []byte(s)
	b[i] = hexits[(cur+offset)%16]

	return string(b)
}

// setQuery sets a query parameter on r and re-encodes the URL.
func setQuery(r *http.Request, key, value string) {
	q := r.URL.Query()
	q.Set(key, value)
	r.URL.RawQuery = q.Encode()
}

// delQuery removes a query parameter from r and re-encodes the URL.
func delQuery(r *http.Request, key string) {
	q := r.URL.Query()
	q.Del(key)
	r.URL.RawQuery = q.Encode()
}
