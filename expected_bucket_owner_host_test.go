package predastore_test

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/xml"
	"flag"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/testcerts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Two accounts, both on config credentials, which is what the account reaper
// runs on: the gate's cross-account check waves one through before it reads an
// owner, so the expected-owner precondition is the only comparison left.
const (
	accountA   = "111122223333"
	accountB   = "444455556666"
	keyA       = "AKIAEXPECTEDOWNERAAA"
	secretA    = "expected-owner-test-secret-aaaaaaaaaaaaaa"
	keyB       = "AKIAEXPECTEDOWNERBBB"
	secretB    = "expected-owner-test-secret-bbbbbbbbbbbbbb"
	raceBucket = "handed-over-bucket"
)

// s3Client drives one account's requests at a gate over real HTTP.
type s3Client struct {
	t         *testing.T
	base      string
	region    string
	accessKey string
	secretKey string
	http      *http.Client
}

// send signs and issues one request, optionally asserting an expected bucket
// owner the way a teardown does.
func (c *s3Client) send(method, path, expectOwner string, body []byte) (int, []byte) {
	c.t.Helper()
	req, err := http.NewRequestWithContext(c.t.Context(), method, c.base+path, bytes.NewReader(body))
	require.NoError(c.t, err)

	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", hash)
	if expectOwner != "" {
		req.Header.Set("X-Amz-Expected-Bucket-Owner", expectOwner)
	}

	signer := v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true })
	require.NoError(c.t, signer.SignHTTP(c.t.Context(),
		aws.Credentials{AccessKeyID: c.accessKey, SecretAccessKey: c.secretKey},
		req, hash, "s3", c.region, time.Now().UTC()))

	resp, err := c.http.Do(req)
	require.NoError(c.t, err)
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	require.NoError(c.t, err)
	return resp.StatusCode, got
}

// versionsResult is the part of ListObjectVersions a teardown reads.
type versionsResult struct {
	XMLName  xml.Name `xml:"ListVersionsResult"`
	Versions []struct {
		Key       string `xml:"Key"`
		VersionID string `xml:"VersionId"`
	} `xml:"Version"`
	DeleteMarkers []struct {
		Key       string `xml:"Key"`
		VersionID string `xml:"VersionId"`
	} `xml:"DeleteMarker"`
}

// startTwoAccountHost runs a gate with two config credentials and returns a
// client for each.
func startTwoAccountHost(t *testing.T) (*s3Client, *s3Client) {
	t.Helper()
	certPath, keyPath, _ := testcerts.Generate(t)
	adminPort, gatePort := freePort(t), freePort(t)

	cfg := &config.Config{
		Version: config.Version,
		Region:  "ap-southeast-2",
		RS:      config.RS{Data: 1, Parity: 0},
		Hosts: []config.Host{{
			ID:        1,
			Addr:      "127.0.0.1",
			DataDir:   t.TempDir(),
			TLSCert:   certPath,
			TLSKey:    keyPath,
			AdminPort: adminPort,
			Nodes: []config.Node{
				{ID: 1, Role: config.RoleGate, Port: gatePort, BindAddr: "127.0.0.1"},
				{ID: 2, Role: config.RoleMeta, Port: freePort(t)},
				{ID: 3, Role: config.RoleBlob, Port: freePort(t)},
			},
		}},
		Auth: []config.AuthEntry{
			{AccessKeyID: keyA, SecretAccessKey: secretA, AccountID: accountA},
			{AccessKeyID: keyB, SecretAccessKey: secretB, AccountID: accountB},
		},
	}
	runHost(t, cfg, newMasterKey(t))

	awaitProbe(t, "http://127.0.0.1:"+strconv.Itoa(adminPort)+"/readyz", http.StatusOK)
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}}
	base := "https://127.0.0.1:" + strconv.Itoa(gatePort)
	awaitGate(t, client, base+"/")

	newClient := func(accessKey, secretKey string) *s3Client {
		return &s3Client{t: t, base: base, region: cfg.Region,
			accessKey: accessKey, secretKey: secretKey, http: client}
	}
	return newClient(keyA, secretA), newClient(keyB, secretB)
}

// seedVersionedBucket creates a versioned bucket and writes one version of
// each key given.
func seedVersionedBucket(t *testing.T, c *s3Client, bucket string, keys ...string) {
	t.Helper()
	status, body := c.send(http.MethodPut, "/"+bucket, "", nil)
	require.Equal(t, http.StatusOK, status, "CreateBucket: %s", body)

	status, body = c.send(http.MethodPut, "/"+bucket+"?versioning", "",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))
	require.Equal(t, http.StatusOK, status, "PutBucketVersioning: %s", body)

	for _, key := range keys {
		status, body = c.send(http.MethodPut, "/"+bucket+"/"+key, "", []byte("body of "+key))
		require.Equal(t, http.StatusOK, status, "PutObject %s: %s", key, body)
	}
}

// A bucket name that changes hands part-way through a teardown must not be
// destroyed. The recreate lands between the reaper's requests, which is the
// window the precondition closes: ownership is decided once when the buckets
// are listed, and every abort, delete and bucket delete is a separate request
// afterwards.
//
// Verifies: ADR-0003:S7.
func TestS7_HandedOverBucketSurvivesTheDrainOfItsPredecessor(t *testing.T) {
	a, b := startTwoAccountHost(t)

	seedVersionedBucket(t, a, raceBucket, "first.txt", "second.txt")

	// The drain begins. Account A's teardown lists versions and deletes one,
	// asserting the owner it was given when the bucket was listed.
	status, body := a.send(http.MethodGet, "/"+raceBucket+"?versions", accountA, nil)
	require.Equal(t, http.StatusOK, status, "ListObjectVersions: %s", body)

	var listed versionsResult
	require.NoError(t, xml.Unmarshal(body, &listed), "body: %s", body)
	require.Len(t, listed.Versions, 2, "body: %s", body)

	first := listed.Versions[0]
	status, body = a.send(http.MethodDelete,
		"/"+raceBucket+"/"+first.Key+"?versionId="+first.VersionID, accountA, nil)
	require.Equal(t, http.StatusNoContent, status, "DeleteObject: %s", body)

	// The name changes hands mid-drain: A finishes emptying and removes the
	// bucket, and B creates one with the same name.
	remaining := listed.Versions[1]
	status, body = a.send(http.MethodDelete,
		"/"+raceBucket+"/"+remaining.Key+"?versionId="+remaining.VersionID, accountA, nil)
	require.Equal(t, http.StatusNoContent, status, "DeleteObject: %s", body)
	status, body = a.send(http.MethodDelete, "/"+raceBucket, accountA, nil)
	require.Equal(t, http.StatusNoContent, status, "DeleteBucket: %s", body)

	seedVersionedBucket(t, b, raceBucket, "b-object.txt")

	// A's teardown resumes against the name it still believes it owns. Every
	// destructive request it has left must be refused.
	status, body = b.send(http.MethodGet, "/"+raceBucket+"?versions", accountB, nil)
	require.Equal(t, http.StatusOK, status, "ListObjectVersions for B: %s", body)
	var bVersions versionsResult
	require.NoError(t, xml.Unmarshal(body, &bVersions))
	require.Len(t, bVersions.Versions, 1, "body: %s", body)
	bVersion := bVersions.Versions[0]

	refused := []struct {
		name   string
		method string
		path   string
	}{
		{"ListObjectVersions", http.MethodGet, "/" + raceBucket + "?versions"},
		{"DeleteObject", http.MethodDelete,
			"/" + raceBucket + "/" + bVersion.Key + "?versionId=" + bVersion.VersionID},
		{"DeleteObject versionless", http.MethodDelete, "/" + raceBucket + "/" + bVersion.Key},
		{"ListMultipartUploads", http.MethodGet, "/" + raceBucket + "?uploads"},
		{"DeleteBucket", http.MethodDelete, "/" + raceBucket},
	}
	for _, tc := range refused {
		status, body = a.send(tc.method, tc.path, accountA, nil)
		assert.Equal(t, http.StatusForbidden, status, "%s was not refused: %s", tc.name, body)
		assert.Contains(t, string(body), "AccessDenied", "%s: %s", tc.name, body)
	}

	// B's bucket and its version are intact, which is the property that matters.
	status, body = b.send(http.MethodGet, "/"+raceBucket+"/b-object.txt", accountB, nil)
	require.Equal(t, http.StatusOK, status, "B's object was destroyed: %s", body)
	assert.Equal(t, []byte("body of b-object.txt"), body)

	status, body = b.send(http.MethodGet, "/"+raceBucket+"?versions", accountB, nil)
	require.Equal(t, http.StatusOK, status, "%s", body)
	var after versionsResult
	require.NoError(t, xml.Unmarshal(body, &after))
	assert.Len(t, after.Versions, 1, "B lost a version: %s", body)
}

// The wire format a teardown reads, over real HTTP, so the hermetic fake's
// rules are checked against the bytes predastore actually serves.
func TestVersionedBucketEmptiesByVersionOverHTTP(t *testing.T) {
	a, _ := startTwoAccountHost(t)
	const bucket = "emptied-by-version"

	seedVersionedBucket(t, a, bucket, "a.txt")

	// A versionless delete hides the key behind a marker rather than removing
	// it, which is the prod failure: the bucket then lists as empty and still
	// refuses to delete.
	status, body := a.send(http.MethodDelete, "/"+bucket+"/a.txt", accountA, nil)
	require.Equal(t, http.StatusNoContent, status, "DeleteObject: %s", body)

	status, body = a.send(http.MethodGet, "/"+bucket+"?list-type=2", accountA, nil)
	require.Equal(t, http.StatusOK, status, "%s", body)
	assert.NotContains(t, string(body), "<Key>a.txt</Key>",
		"the hidden key still lists as current: %s", body)

	status, body = a.send(http.MethodDelete, "/"+bucket, accountA, nil)
	require.Equal(t, http.StatusConflict, status,
		"an emptied-looking versioned bucket must still refuse: %s", body)
	assert.Contains(t, string(body), "BucketNotEmpty")

	// ListObjectVersions is what makes progress possible: it reports both the
	// retained version and the marker, each addressable by id.
	status, body = a.send(http.MethodGet, "/"+bucket+"?versions", accountA, nil)
	require.Equal(t, http.StatusOK, status, "%s", body)

	// Pin the bytes, not only the decode. The spinifex reaper reads this
	// response through the aws-sdk-go v1 decoder, and its test is fed from this
	// same fixture, so a drift here shows up as a diff rather than silently.
	pinVersionsFixture(t, body)

	var listed versionsResult
	require.NoError(t, xml.Unmarshal(body, &listed), "body: %s", body)
	require.Len(t, listed.Versions, 1, "body: %s", body)
	require.Len(t, listed.DeleteMarkers, 1, "body: %s", body)
	assert.Equal(t, "a.txt", listed.Versions[0].Key)
	assert.NotEmpty(t, listed.Versions[0].VersionID)
	assert.Equal(t, "a.txt", listed.DeleteMarkers[0].Key)
	assert.NotEmpty(t, listed.DeleteMarkers[0].VersionID)

	for _, versionID := range []string{listed.Versions[0].VersionID, listed.DeleteMarkers[0].VersionID} {
		status, body = a.send(http.MethodDelete, "/"+bucket+"/a.txt?versionId="+versionID, accountA, nil)
		require.Equal(t, http.StatusNoContent, status, "delete %s: %s", versionID, body)
	}

	status, body = a.send(http.MethodDelete, "/"+bucket, accountA, nil)
	require.Equal(t, http.StatusNoContent, status, "DeleteBucket after emptying: %s", body)
}

// versionsFixturePath holds the ListObjectVersions body a teardown reads. The
// spinifex reaper decodes this response with aws-sdk-go v1, and its decoder
// test is fed from a copy of this file.
const versionsFixturePath = "testdata/list_object_versions.xml"

// pinVersionsFixture compares the served body against the committed fixture,
// with the ids and timestamps that change per run normalised away. Run with
// -update to rewrite it.
func pinVersionsFixture(t *testing.T, body []byte) {
	t.Helper()
	normalised := normaliseVersionsBody(body)

	if *updateFixtures {
		require.NoError(t, os.MkdirAll(filepath.Dir(versionsFixturePath), 0o750))
		require.NoError(t, os.WriteFile(versionsFixturePath, normalised, 0o600))
		return
	}

	want, err := os.ReadFile(versionsFixturePath)
	require.NoError(t, err, "the fixture is missing; regenerate it with -update")
	require.Equal(t, string(want), string(normalised),
		"the ListObjectVersions body changed; the spinifex decoder test reads a copy of this")
}

// normaliseVersionsBody replaces the parts of the body that differ per run.
// The element order, names and nesting are what the decoder depends on, and
// those are what stay.
func normaliseVersionsBody(body []byte) []byte {
	out := versionIDPattern.ReplaceAll(body, []byte("<VersionId>VERSIONID</VersionId>"))
	out = lastModifiedPattern.ReplaceAll(out, []byte("<LastModified>TIMESTAMP</LastModified>"))
	return append(out, '\n')
}

var (
	updateFixtures      = flag.Bool("update", false, "rewrite the committed response fixtures")
	versionIDPattern    = regexp.MustCompile(`<VersionId>[^<]*</VersionId>`)
	lastModifiedPattern = regexp.MustCompile(`<LastModified>[^<]*</LastModified>`)
)
