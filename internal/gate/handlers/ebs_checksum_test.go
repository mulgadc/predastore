package handlers

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/minio/crc64nvme"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func crc64Header(body []byte) string {
	sum := make([]byte, 8)
	binary.BigEndian.PutUint64(sum, crc64nvme.Checksum(body))
	return base64.StdEncoding.EncodeToString(sum)
}

func md5ETag(body []byte) string {
	return fmt.Sprintf("\"%x\"", md5.Sum(body))
}

func ebsPut(g *realStoreGate, key string, body []byte, header http.Header) *http.Response {
	rr := g.serve(PutObject(g.mc, g.bc, g.ring, g.cache, g.cfg), http.MethodPut, "/bkt/"+key, key, body, header)
	return rr.Result()
}

func TestEBSChunkPutIsVerifiedWithCRC64NVMEAndCarriesNoMD5(t *testing.T) {
	g := newRealStoreGate(t, rsProfiles[0])
	body := bytes.Repeat([]byte("chunk"), 1<<18)
	crc := crc64nvme.Checksum(body)

	resp := ebsPut(g, "c1", body, http.Header{
		"X-Spinifex-Ebs":           {"true"},
		"X-Amz-Checksum-Crc64nvme": {crc64Header(body)},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	want := fmt.Sprintf("\"%016x%016x\"", crc, len(body))
	assert.Equal(t, want, resp.Header.Get("ETag"), "the ETag is the CRC64NVME and length, not MD5")
	assert.Equal(t, crc64Header(body), resp.Header.Get("X-Amz-Checksum-Crc64nvme"))

	got := g.get("c1")
	require.Equal(t, http.StatusOK, got.Code)
	assert.Equal(t, body, got.Body.Bytes())
	assert.Equal(t, want, got.Header().Get("ETag"))
}

func TestEBSChunkPutWithAWrongCRC64NVMEIsRefusedAndStoresNothing(t *testing.T) {
	g := newRealStoreGate(t, rsProfiles[0])
	old := []byte("previous chunk")
	require.Equal(t, http.StatusOK, g.put("c1", old).Code)

	body := bytes.Repeat([]byte("chunk"), 1<<18)
	resp := ebsPut(g, "c1", body, http.Header{
		"X-Spinifex-Ebs":           {"true"},
		"X-Amz-Checksum-Crc64nvme": {crc64Header([]byte("other bytes"))},
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, readAll(t, resp), "<Code>BadDigest</Code>")

	got := g.get("c1")
	require.Equal(t, http.StatusOK, got.Code)
	assert.Equal(t, old, got.Body.Bytes(), "a refused chunk leaves the previous object current")
}

func TestOnlyBothHeadersTogetherReplaceTheMD5ETag(t *testing.T) {
	body := bytes.Repeat([]byte("chunk"), 1<<16)
	for name, header := range map[string]http.Header{
		"checksum only, as the AWS SDKs send by default": {"X-Amz-Checksum-Crc64nvme": {crc64Header(body)}},
		"opt-in only": {"X-Spinifex-Ebs": {"true"}},
		"opt-in not true": {
			"X-Spinifex-Ebs":           {"false"},
			"X-Amz-Checksum-Crc64nvme": {crc64Header(body)},
		},
	} {
		t.Run(name, func(t *testing.T) {
			g := newRealStoreGate(t, rsProfiles[0])
			resp := ebsPut(g, "o", body, header)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, md5ETag(body), resp.Header.Get("ETag"))
		})
	}

	t.Run("an aws-chunked body with both headers keeps the trailer path", func(t *testing.T) {
		g := newRealStoreGate(t, rsProfiles[0])
		payload := string(body)
		resp := ebsPut(g, "o", []byte(framedBodyChecksum(payload, crc64Trailer(payload))), http.Header{
			"Content-Encoding":             {"aws-chunked"},
			"X-Amz-Decoded-Content-Length": {fmt.Sprint(len(payload))},
			"X-Amz-Trailer":                {"x-amz-checksum-crc64nvme"},
			"X-Spinifex-Ebs":               {"true"},
			"X-Amz-Checksum-Crc64nvme":     {crc64Header(body)},
		})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, md5ETag(body), resp.Header.Get("ETag"))
	})
}

func TestEBSChunkPutWithAMalformedCRC64NVMEIsRefusedBeforeAnyShard(t *testing.T) {
	for name, value := range map[string]string{
		"not base64":  "%%%",
		"not 8 bytes": base64.StdEncoding.EncodeToString([]byte{1, 2, 3, 4}),
	} {
		t.Run(name, func(t *testing.T) {
			g := newRealStoreGate(t, rsProfiles[0])
			g.bc.arm(shardFaults{failPut: []uint32{0, 1, 2}})
			resp := ebsPut(g, "c1", []byte("chunk"), http.Header{
				"X-Spinifex-Ebs":           {"true"},
				"X-Amz-Checksum-Crc64nvme": {value},
			})
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Contains(t, readAll(t, resp), "<Code>InvalidDigest</Code>",
				"refused before placement, so the failing shards are never reached")
		})
	}
}

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(b)
}
