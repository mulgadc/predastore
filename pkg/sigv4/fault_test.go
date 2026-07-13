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

// A fault mutates a signed request in place and returns the sentinel Verify must then yield.
// Because a fault is a request edit, Parse's early checks (time, credential, framing) run
// before the signature check, so each fault's sentinel is deterministic. Each fault draws
// its own randomness, so the exact corruption varies across rapid iterations.
type fault func(t *rapid.T, r *http.Request) error

// amzTime formats t as an X-Amz-Date value.
func amzTime(t time.Time) string { return t.UTC().Format("20060102T150405Z") }

// bogusRegions and bogusServices are values guaranteed to differ from the ones the fault
// tests sign with, so a scope rewrite always mismatches.
var (
	bogusRegions  = []string{"ap-south-1", "sa-east-1", "ca-central-1", "me-south-1"}
	bogusServices = []string{"iam", "dynamodb", "sqs", "lambda", "ec2"}
)

// headerFaults perturb a header-authenticated request signed for service s3.
var headerFaults = map[string]fault{
	"tampered signature": func(t *rapid.T, r *http.Request) error {
		r.Header.Set("Authorization", corruptAuthSignature(t, r.Header.Get("Authorization")))
		return sigv4.ErrSignatureMismatch
	},
	"changed method": func(t *rapid.T, r *http.Request) error {
		r.Method = rapid.SampledFrom(methodsExcept(r.Method)).Draw(t, "newMethod")
		return sigv4.ErrSignatureMismatch
	},
	"injected query param": func(t *rapid.T, r *http.Request) error {
		key := rapid.StringMatching(`[a-z]{1,6}`).Draw(t, "injectKey")
		val := rapid.StringMatching(`[a-zA-Z0-9]{1,6}`).Draw(t, "injectVal")
		if r.URL.RawQuery == "" {
			r.URL.RawQuery = key + "=" + val
		} else {
			r.URL.RawQuery += "&" + key + "=" + val
		}
		return sigv4.ErrSignatureMismatch
	},
	"tampered payload hash": func(t *rapid.T, r *http.Request) error {
		r.Header.Set("X-Amz-Content-Sha256", corruptHexDigit(t, r.Header.Get("X-Amz-Content-Sha256")))
		return sigv4.ErrSignatureMismatch
	},
	"added unsigned x-amz header": func(t *rapid.T, r *http.Request) error {
		// "Inj-" prefix keeps the name off the signed X-Amz-Meta-Foo header.
		suffix := rapid.StringMatching(`[a-z]{1,6}`).Draw(t, "injectHeader")
		r.Header.Set("X-Amz-Meta-Inj-"+suffix, rapid.StringMatching(`[a-zA-Z0-9]{1,6}`).Draw(t, "injectHeaderValue"))
		return sigv4.ErrUnsignedHeader
	},
	"removed content-sha256": func(_ *rapid.T, r *http.Request) error {
		r.Header.Del("X-Amz-Content-Sha256")
		return sigv4.ErrMissingContentSHA256
	},
	"removed authorization": func(_ *rapid.T, r *http.Request) error {
		r.Header.Del("Authorization")
		return sigv4.ErrMissingAuthentication
	},
	"unsupported algorithm": func(t *rapid.T, r *http.Request) error {
		algo := rapid.SampledFrom([]string{"AWS4-ECDSA-P256-SHA256", "AWS4-HMAC-SHA1", "AWS3-HMAC-SHA256", "BOGUS-ALGO"}).Draw(t, "algorithm")
		_, rest, _ := strings.Cut(r.Header.Get("Authorization"), " ")
		r.Header.Set("Authorization", algo+" "+rest)
		return sigv4.ErrUnsupportedAlgorithm
	},
	"wrong credential region": func(t *rapid.T, r *http.Request) error {
		bogus := rapid.SampledFrom(bogusRegions).Draw(t, "bogusRegion")
		rewriteCredential(r, func(p []string) []string { p[2] = bogus; return p })
		return sigv4.ErrMalformedAuthorization
	},
	"wrong credential service": func(t *rapid.T, r *http.Request) error {
		bogus := rapid.SampledFrom(bogusServices).Draw(t, "bogusService")
		rewriteCredential(r, func(p []string) []string { p[3] = bogus; return p })
		return sigv4.ErrMalformedAuthorization
	},
	"malformed credential scope": func(t *rapid.T, r *http.Request) error {
		kind := rapid.SampledFrom([]string{"drop", "extra", "bad-terminator"}).Draw(t, "scopeMutation")
		keep := rapid.IntRange(1, 4).Draw(t, "keepParts")
		rewriteCredential(r, func(p []string) []string {
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
		return sigv4.ErrMalformedAuthorization
	},
	"skewed date": func(t *rapid.T, r *http.Request) error {
		d := sigv4.MaxClockSkew + time.Duration(rapid.IntRange(1, 2880).Draw(t, "skewMinutes"))*time.Minute
		if !rapid.Bool().Draw(t, "skewAhead") {
			d = -d
		}
		r.Header.Set("X-Amz-Date", amzTime(oracleTime.Add(d)))
		return sigv4.ErrRequestTimeTooSkewed
	},
	"unparseable date": func(t *rapid.T, r *http.Request) error {
		r.Header.Set("X-Amz-Date", rapid.StringMatching(`[A-Za-z]{3,10}`).Draw(t, "badDate"))
		return sigv4.ErrRequestTimeInvalid
	},
}

// presignFaults perturb a presigned-URL request signed for service s3.
var presignFaults = map[string]fault{
	"tampered signature": func(t *rapid.T, r *http.Request) error {
		setQuery(r, "X-Amz-Signature", corruptHexDigit(t, r.URL.Query().Get("X-Amz-Signature")))
		return sigv4.ErrSignatureMismatch
	},
	"injected query param": func(t *rapid.T, r *http.Request) error {
		key := rapid.StringMatching(`[a-z]{1,6}`).Draw(t, "injectKey")
		setQuery(r, key, rapid.StringMatching(`[a-zA-Z0-9]{1,6}`).Draw(t, "injectVal"))
		return sigv4.ErrSignatureMismatch
	},
	"expired": func(t *rapid.T, r *http.Request) error {
		// Either well past expiry (behind) or too far in the future (ahead); both expire.
		offset := time.Duration(rapid.IntRange(2, 48).Draw(t, "expiredHours")) * time.Hour
		ts := oracleTime.Add(-offset)
		if rapid.Bool().Draw(t, "expiredAhead") {
			ts = oracleTime.Add(sigv4.MaxClockSkew + offset)
		}
		setQuery(r, "X-Amz-Date", amzTime(ts))
		return sigv4.ErrPresignedURLExpired
	},
	"expiry beyond max": func(t *rapid.T, r *http.Request) error {
		max := int(sigv4.MaxPresignAge / time.Second)
		setQuery(r, "X-Amz-Expires", strconv.Itoa(max+rapid.IntRange(1, 100000).Draw(t, "expiryExcess")))
		return sigv4.ErrMalformedPresignedURL
	},
	"removed expires": func(_ *rapid.T, r *http.Request) error {
		delQuery(r, "X-Amz-Expires")
		return sigv4.ErrMalformedPresignedURL
	},
	"wrong credential region": func(t *rapid.T, r *http.Request) error {
		bogus := rapid.SampledFrom(bogusRegions).Draw(t, "bogusRegion")
		parts := strings.Split(r.URL.Query().Get("X-Amz-Credential"), "/")
		parts[2] = bogus
		setQuery(r, "X-Amz-Credential", strings.Join(parts, "/"))
		return sigv4.ErrMalformedAuthorization
	},
}

// TestVerifyRejectsHeaderFaults checks that every header fault rejects with its sentinel,
// proving the signature covers what it claims and that scope, time, and framing checks fire.
func TestVerifyRejectsHeaderFaults(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		method := rapid.SampledFrom([]string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete}).Draw(t, "method")
		region := rapid.SampledFrom([]string{"us-east-1", "eu-west-1"}).Draw(t, "region")
		path := "/" + rapid.StringMatching(`[a-zA-Z0-9][a-zA-Z0-9._~-]{0,15}`).Draw(t, "path")
		rawURL := "https://" + oracleHost + path

		for _, name := range sortedFaults(headerFaults) {
			// Fresh request per fault: the mutation is destructive.
			req := signHeader(t, method, rawURL, nil, map[string]string{"X-Amz-Meta-Foo": "bar"}, region, "s3", "")
			want := headerFaults[name](t, req)
			if got := parseVerify(req, region, "s3", oracleTime); !errors.Is(got, want) {
				t.Fatalf("fault %q: got %v, want %v", name, got, want)
			}
		}
	})
}

// TestVerifyRejectsPresignFaults is the presigned-URL counterpart.
func TestVerifyRejectsPresignFaults(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		region := rapid.SampledFrom([]string{"us-east-1", "eu-west-1"}).Draw(t, "region")
		path := "/" + rapid.StringMatching(`[a-zA-Z0-9][a-zA-Z0-9._~-]{0,15}`).Draw(t, "path")
		rawURL := "https://" + oracleHost + path

		for _, name := range sortedFaults(presignFaults) {
			// Fresh request per fault: the mutation is destructive.
			req := presign(t, rawURL, 3600, region, "s3")
			want := presignFaults[name](t, req)
			if got := parseVerify(req, region, "s3", oracleTime); !errors.Is(got, want) {
				t.Fatalf("fault %q: got %v, want %v", name, got, want)
			}
		}
	})
}

// sortedFaults returns the fault names in a stable order so the sequence of rapid draws is
// deterministic across runs — a requirement for rapid to replay and shrink failures.
func sortedFaults(m map[string]fault) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// methodsExcept returns the standard methods minus cur, so a mutation always changes it.
func methodsExcept(cur string) []string {
	var out []string
	for _, m := range []string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete, http.MethodHead} {
		if m != cur {
			out = append(out, m)
		}
	}
	return out
}

// corruptHexDigit replaces one randomly chosen hex digit of s with a different one, so the
// result is a valid but guaranteed-different hex string.
func corruptHexDigit(t *rapid.T, s string) string {
	const hexits = "0123456789abcdef"
	i := rapid.IntRange(0, len(s)-1).Draw(t, "hexIndex")
	cur := max(strings.IndexByte(hexits, s[i]), 0)
	offset := rapid.IntRange(1, 15).Draw(t, "hexOffset")
	b := []byte(s)
	b[i] = hexits[(cur+offset)%16]
	return string(b)
}

// corruptAuthSignature corrupts the Signature= value inside an Authorization header.
func corruptAuthSignature(t *rapid.T, auth string) string {
	const marker = "Signature="
	i := strings.LastIndex(auth, marker)
	return auth[:i+len(marker)] + corruptHexDigit(t, auth[i+len(marker):])
}

// rewriteCredential rewrites the 5-part credential scope in the Authorization header.
func rewriteCredential(r *http.Request, fn func(parts []string) []string) {
	const prefix = "Credential="
	auth := r.Header.Get("Authorization")
	i := strings.Index(auth, prefix)
	rest := auth[i+len(prefix):]
	j := strings.Index(rest, ",")
	scope := strings.Join(fn(strings.Split(rest[:j], "/")), "/")
	r.Header.Set("Authorization", auth[:i+len(prefix)]+scope+rest[j:])
}

func setQuery(r *http.Request, key, value string) {
	q := r.URL.Query()
	q.Set(key, value)
	r.URL.RawQuery = q.Encode()
}

func delQuery(r *http.Request, key string) {
	q := r.URL.Query()
	q.Del(key)
	r.URL.RawQuery = q.Encode()
}
