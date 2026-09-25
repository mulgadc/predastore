package gate

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/mulgadc/predastore/internal/blob"
	"github.com/mulgadc/predastore/internal/config"
	"github.com/mulgadc/predastore/internal/gate/handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memShard is one stored shard generation.
type memShard struct {
	data  []byte
	epoch uint64
}

type memShardID struct {
	node  config.NodeID
	key   [32]byte
	index uint32
}

// memBlob is an in-memory blob tier with the prepare/commit split, enough for
// objects to round-trip through the gate without a cluster.
type memBlob struct {
	mu       sync.Mutex
	live     map[memShardID]memShard
	prepared map[memShardID]memShard
}

var _ BlobClient = (*memBlob)(nil)

func newMemBlob() *memBlob {
	return &memBlob{live: map[memShardID]memShard{}, prepared: map[memShardID]memShard{}}
}

func (b *memBlob) Put(_ context.Context, node config.NodeID, req blob.PutRequest, body io.Reader) (*blob.PutResponse, error) {
	data, err := io.ReadAll(io.LimitReader(body, req.Size))
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prepared[memShardID{node, req.Key, req.Index}] = memShard{data: data, epoch: req.Epoch}
	return &blob.PutResponse{Size: int64(len(data)), Epoch: req.Epoch}, nil
}

func (b *memBlob) Commit(_ context.Context, node config.NodeID, req blob.CommitRequest) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := memShardID{node, req.Key, req.Index}
	if shard, ok := b.prepared[id]; ok && shard.epoch == req.Epoch {
		b.live[id] = shard
		delete(b.prepared, id)
		return false, nil
	}
	if shard, ok := b.live[id]; ok && shard.epoch == req.Epoch {
		return false, nil
	}
	return false, blob.ErrNotPrepared
}

func (b *memBlob) Abort(_ context.Context, node config.NodeID, req blob.CommitRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.prepared, memShardID{node, req.Key, req.Index})
	return nil
}

func (b *memBlob) Get(_ context.Context, node config.NodeID, req blob.GetRequest) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	shard, ok := b.live[memShardID{node, req.Key, req.Index}]
	if !ok {
		return nil, blob.ErrNotFound
	}
	if req.Epoch != 0 && shard.epoch != req.Epoch {
		return nil, blob.ErrEpochMismatch
	}
	data := shard.data
	if req.RangeStart >= 0 && req.RangeEnd >= 0 {
		data = data[req.RangeStart : req.RangeEnd+1]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (b *memBlob) Delete(_ context.Context, node config.NodeID, req blob.DeleteRequest) (*blob.DeleteResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := memShardID{node, req.Key, req.Index}
	_, ok := b.live[id]
	delete(b.live, id)
	delete(b.prepared, id)
	return &blob.DeleteResponse{Deleted: ok}, nil
}

func (b *memBlob) Stat(_ context.Context, node config.NodeID, req blob.StatRequest) (*blob.StatResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	shard, ok := b.live[memShardID{node, req.Key, req.Index}]
	if !ok {
		return nil, blob.ErrNotFound
	}
	return &blob.StatResponse{Epoch: shard.epoch, Size: int64(len(shard.data))}, nil
}

func (b *memBlob) Release(context.Context, config.NodeID, blob.ReleaseRequest) error { return nil }

// keyGate drives a gate backed by memBlob with SigV4-signed requests, so every
// request also passes signature verification over the path it was sent with.
type keyGate struct {
	t      *testing.T
	server *Server
	region string
}

func newKeyGate(t *testing.T) keyGate {
	t.Helper()
	cfg := newAuthTestConfig()
	cfg.Blob = newMemBlob()
	cfg.RS = RS{Data: 2, Parity: 1}
	cfg.BlobNodeIDs = []config.NodeID{1, 2, 3}
	return keyGate{t: t, server: newTestGate(t, cfg), region: cfg.Region}
}

// do sends target exactly as given: the escaping in it is the wire form.
func (g keyGate) do(method, target string, body []byte, header http.Header) *httptest.ResponseRecorder {
	g.t.Helper()
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	maps.Copy(req.Header, header)
	signTestReq(g.t, req, body, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", g.region, "s3")
	rr := httptest.NewRecorder()
	g.server.ServeHTTP(rr, req)
	return rr
}

// sdkPath escapes a key the way the AWS SDKs put it on the wire: each segment
// percent-encoded, + included, with the slashes left as separators.
func sdkPath(key string) string {
	segments := strings.Split(key, "/")
	for i, s := range segments {
		segments[i] = strings.ReplaceAll(url.PathEscape(s), "+", "%2B")
	}
	return "/local/" + strings.Join(segments, "/")
}

func (g keyGate) listV2(query url.Values) handlers.ListObjectsV2 {
	g.t.Helper()
	query.Set("list-type", "2")
	rr := g.do(http.MethodGet, "/local?"+query.Encode(), nil, nil)
	require.Equal(g.t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	var result handlers.ListObjectsV2
	require.NoError(g.t, xml.Unmarshal(rr.Body.Bytes(), &result))
	return result
}

func (g keyGate) listedKeys(query url.Values) []string {
	g.t.Helper()
	result := g.listV2(query)
	var keys []string
	if result.Contents != nil {
		for _, c := range *result.Contents {
			keys = append(keys, c.Key)
		}
	}
	sort.Strings(keys)
	return keys
}

// Every operation that names a key must agree on the decoded literal key, or
// sync tools see a different name on each side and recopy or delete forever.
func TestObjectKeysRoundTripDecodedThroughTheGate(t *testing.T) {
	const (
		spaced    = "dir with space/ü-file+plus.txt"
		plus      = "a+b"
		percent   = "100%.txt"
		copied    = "copy of/ß+.txt"
		multipart = "multi part/ç+.bin"
	)
	g := newKeyGate(t)

	// PUT, HEAD and GET over the SDK's escaping.
	for _, key := range []string{spaced, percent} {
		rr := g.do(http.MethodPut, sdkPath(key), []byte("body of "+key), nil)
		require.Equal(t, http.StatusOK, rr.Code, "put %q: %s", key, rr.Body.String())
		require.Equal(t, http.StatusOK, g.do(http.MethodHead, sdkPath(key), nil, nil).Code, "head %q", key)
		rr = g.do(http.MethodGet, sdkPath(key), nil, nil)
		require.Equal(t, http.StatusOK, rr.Code, "get %q", key)
		assert.Equal(t, "body of "+key, rr.Body.String())
	}

	// A raw + in the path is a plus, and names the same object as %2B.
	require.Equal(t, http.StatusOK, g.do(http.MethodPut, "/local/a+b", []byte("plus"), nil).Code)
	rr := g.do(http.MethodGet, "/local/a%2Bb", nil, nil)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "plus", rr.Body.String())
	assert.Equal(t, http.StatusNotFound, g.do(http.MethodHead, "/local/a%20b", nil, nil).Code)

	// CopyObject decodes x-amz-copy-source the same way.
	rr = g.do(http.MethodPut, sdkPath(copied), nil, http.Header{
		"X-Amz-Copy-Source": {strings.TrimPrefix(sdkPath(spaced), "/")},
	})
	require.Equal(t, http.StatusOK, rr.Code, "copy: %s", rr.Body.String())
	rr = g.do(http.MethodGet, sdkPath(copied), nil, nil)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "body of "+spaced, rr.Body.String())

	// A multipart upload keeps the key from initiation through completion.
	rr = g.do(http.MethodPost, sdkPath(multipart)+"?uploads", nil, nil)
	require.Equal(t, http.StatusOK, rr.Code, "initiate: %s", rr.Body.String())
	var initiated handlers.InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &initiated))
	assert.Equal(t, multipart, initiated.Key)

	uploadID := url.QueryEscape(initiated.UploadId)
	rr = g.do(http.MethodPut, sdkPath(multipart)+"?partNumber=1&uploadId="+uploadID, []byte("one part"), nil)
	require.Equal(t, http.StatusOK, rr.Code, "upload part: %s", rr.Body.String())
	complete := []byte(fmt.Sprintf(
		"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>",
		rr.Header().Get("ETag")))
	rr = g.do(http.MethodPost, sdkPath(multipart)+"?uploadId="+uploadID, complete, nil)
	require.Equal(t, http.StatusOK, rr.Code, "complete: %s", rr.Body.String())
	var completed handlers.CompleteMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &completed))
	assert.Equal(t, multipart, completed.Key)
	assert.True(t, strings.HasSuffix(completed.Location, "/local/multi%20part/%C3%A7%2B.bin"), completed.Location)
	rr = g.do(http.MethodGet, sdkPath(multipart), nil, nil)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "one part", rr.Body.String())

	// The listing returns the literal keys, and encodes them only on request.
	all := []string{percent, plus, copied, spaced, multipart}
	sort.Strings(all)
	assert.Equal(t, all, g.listedKeys(url.Values{}))

	encoded := g.listV2(url.Values{"encoding-type": {"url"}, "prefix": {"dir with space/"}})
	assert.Equal(t, "url", encoded.EncodingType)
	assert.Equal(t, "dir%20with%20space/", encoded.Prefix)
	require.NotNil(t, encoded.Contents)
	require.Len(t, *encoded.Contents, 1)
	assert.Equal(t, "dir%20with%20space/%C3%BC-file%2Bplus.txt", (*encoded.Contents)[0].Key)

	rr = g.do(http.MethodGet, "/local?prefix="+url.QueryEscape("a+"), nil, nil)
	require.Equal(t, http.StatusOK, rr.Code)
	var v1 handlers.ListObjectsV1
	require.NoError(t, xml.Unmarshal(rr.Body.Bytes(), &v1))
	require.NotNil(t, v1.Contents)
	require.Len(t, *v1.Contents, 1)
	assert.Equal(t, plus, (*v1.Contents)[0].Key)

	// DeleteObjects names keys in XML, raw; DELETE names one in the path.
	var names bytes.Buffer
	require.NoError(t, xml.EscapeText(&names, []byte(spaced)))
	del := []byte("<Delete><Object><Key>" + names.String() + "</Key></Object></Delete>")
	rr = g.do(http.MethodPost, "/local?delete=", del, nil)
	require.Equal(t, http.StatusOK, rr.Code, "delete objects: %s", rr.Body.String())
	assert.Equal(t, http.StatusNoContent, g.do(http.MethodDelete, sdkPath(copied), nil, nil).Code)
	assert.Equal(t, http.StatusNoContent, g.do(http.MethodDelete, "/local/100%25.txt", nil, nil).Code)

	assert.Equal(t, http.StatusNotFound, g.do(http.MethodHead, sdkPath(spaced), nil, nil).Code)
	assert.Equal(t, []string{plus, multipart}, g.listedKeys(url.Values{}))
}
