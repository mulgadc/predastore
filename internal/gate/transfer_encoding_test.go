package gate

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mulgadc/bluebottle/pkg/sigv4"
	"github.com/mulgadc/predastore/internal/gate/chunked"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// signWithHeaders signs req the way botocore does, covering exactly the headers
// named in values. The Go SDK refuses to sign transfer-encoding, so the canonical
// request is built here from the spec.
func signWithHeaders(t *testing.T, req *http.Request, values map[string]string,
	payloadHash, key, secret, region string, now time.Time) {
	t.Helper()

	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)

	var canonHeaders strings.Builder
	for _, name := range names {
		fmt.Fprintf(&canonHeaders, "%s:%s\n", name, values[name])
	}
	signed := strings.Join(names, ";")

	canonical := strings.Join([]string{
		req.Method, req.URL.EscapedPath(), req.URL.RawQuery,
		canonHeaders.String(), signed, payloadHash,
	}, "\n")
	sum := sha256.Sum256([]byte(canonical))

	date := now.Format("20060102")
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", date, region)
	sts := strings.Join([]string{
		"AWS4-HMAC-SHA256", now.Format(sigv4.AmzTimeFormat), scope, hex.EncodeToString(sum[:]),
	}, "\n")
	mac := hmac.New(sha256.New, deriveSigningKey(secret, date, region, "s3"))
	mac.Write([]byte(sts))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		key, scope, signed, hex.EncodeToString(mac.Sum(nil))))
}

// awsChunkedUnsignedTrailer frames payload as an unsigned aws-chunked body with
// a CRC32 trailer, the shape STREAMING-UNSIGNED-PAYLOAD-TRAILER uploads carry.
func awsChunkedUnsignedTrailer(payload string) string {
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE([]byte(payload)))
	return fmt.Sprintf("%x\r\n%s\r\n0\r\nx-amz-checksum-crc32:%s\r\n\r\n",
		len(payload), payload, base64.StdEncoding.EncodeToString(crc[:]))
}

// TestSigV4TransferEncodingRoundTrip sends requests that sign transfer-encoding
// over a real connection, so net/http strips the header on the server exactly
// as it does for the AWS CLI's streaming uploads.
func TestSigV4TransferEncodingRoundTrip(t *testing.T) {
	const (
		payload = "hello world"
		region  = "ap-southeast-2"
		key     = "AKIAIOSFODNN7EXAMPLE"
		secret  = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	)

	// result is what the handler observed, sent back rather than asserted in
	// place: a failed assertion inside a handler aborts the wrong goroutine.
	type result struct {
		body string
		err  error
	}
	results := make(chan result, 1)

	s := newTestGate(t, newAuthTestConfig())
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body io.Reader = r.Body
		var dec *chunked.Decoder
		if handlers.SignedPayloadFrom(r.Context()).Framed() {
			dec = chunked.NewDecoder(r.Body, -1,
				chunked.WithTrailerChecksums(r.Header.Values("X-Amz-Trailer")))
			body = dec
		}
		got, err := io.ReadAll(body)
		if err == nil && dec != nil {
			err = dec.VerifyTrailerChecksum()
		}
		results <- result{body: string(got), err: err}
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(resolveRouter(next, s.sigV4AuthMiddleware))
	t.Cleanup(srv.Close)

	// send signs and sends a PUT. A chunked request hides its length from the
	// client, which is what makes net/http frame it with Transfer-Encoding.
	send := func(t *testing.T, body, contentHash string, extra map[string]string, chunkedTE bool) *http.Response {
		t.Helper()

		req, err := http.NewRequest(http.MethodPut, srv.URL+"/private/o.txt", nil)
		require.NoError(t, err)
		now := time.Now().UTC()
		values := map[string]string{
			"host":                 req.URL.Host,
			"transfer-encoding":    "chunked",
			"x-amz-content-sha256": contentHash,
			"x-amz-date":           now.Format(sigv4.AmzTimeFormat),
		}
		for name, v := range extra {
			values[name] = v
			req.Header.Set(name, v)
		}
		req.Header.Set("X-Amz-Content-Sha256", contentHash)
		req.Header.Set("X-Amz-Date", values["x-amz-date"])

		req.Body = io.NopCloser(strings.NewReader(body))
		req.ContentLength = int64(len(body))
		if chunkedTE {
			req.ContentLength = -1
			req.TransferEncoding = []string{"chunked"}
		}

		signWithHeaders(t, req, values, contentHash, key, secret, region, now)
		resp, err := srv.Client().Do(req)
		require.NoError(t, err)
		t.Cleanup(func() { _ = resp.Body.Close() })
		return resp
	}

	t.Run("signed digest", func(t *testing.T) {
		sum := sha256.Sum256([]byte(payload))
		resp := send(t, payload, hex.EncodeToString(sum[:]), nil, true)

		respBody, _ := io.ReadAll(resp.Body)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(respBody))
		require.Len(t, results, 1, "the request never reached the handler")
		res := <-results
		require.NoError(t, res.err)
		assert.Equal(t, payload, res.body)
	})

	t.Run("unsigned payload with trailing checksum", func(t *testing.T) {
		resp := send(t, awsChunkedUnsignedTrailer(payload), string(sigv4.StreamingUnsignedTrailer),
			map[string]string{
				"content-encoding":             "aws-chunked",
				"x-amz-decoded-content-length": strconv.Itoa(len(payload)),
				"x-amz-trailer":                "x-amz-checksum-crc32",
			}, true)

		respBody, _ := io.ReadAll(resp.Body)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(respBody))
		require.Len(t, results, 1, "the request never reached the handler")
		res := <-results
		require.NoError(t, res.err)
		assert.Equal(t, payload, res.body)
	})

	t.Run("signed transfer-encoding the request did not use", func(t *testing.T) {
		// The header is restored only from what arrived, never invented, so a
		// signature over a coding the request lacks still fails as it does on S3.
		sum := sha256.Sum256([]byte(payload))
		resp := send(t, payload, hex.EncodeToString(sum[:]), nil, false)

		respBody, _ := io.ReadAll(resp.Body)
		assert.Equal(t, http.StatusForbidden, resp.StatusCode)
		assert.Contains(t, string(respBody), "SignatureDoesNotMatch")
		assert.Empty(t, results, "a forged signature must not reach the handler")
	})
}
