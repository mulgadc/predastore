package s3

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mulgadc/predastore/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListBucketsNoAuth(t *testing.T) {
	config := newAuthTestConfig()

	server := NewHTTP2ServerWithBackend(config, nil, NewConfigProvider(config.Auth))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")
}

func TestListObjectsV2HandlerPrivateBucketNoAuth(t *testing.T) {
	config := newAuthTestConfig()

	server := NewHTTP2ServerWithBackend(config, nil, NewConfigProvider(config.Auth))

	req := httptest.NewRequest(http.MethodGet, "/private", nil)
	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")

	var result S3Error
	err := xml.NewDecoder(rr.Body).Decode(&result)
	assert.NoError(t, err, "XML parsing should not error")
	assert.Equal(t, "AccessDenied", result.Code, "Error message should indicate access denied")
}

func TestListObjectsV2HandlerPrivateBucketBadAuth(t *testing.T) {
	config := newAuthTestConfig()

	server := NewHTTP2ServerWithBackend(config, nil, NewConfigProvider(config.Auth))

	req := httptest.NewRequest(http.MethodGet, "/private", nil)
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	err := auth.GenerateAuthHeaderReq("BADACCESSKEY", "BADSECRETKEY", timestamp, config.Region, "s3", req)
	assert.NoError(t, err, "Error generating auth header")

	rr := httptest.NewRecorder()
	server.GetHandler().ServeHTTP(rr, req)

	assert.Equal(t, 403, rr.Code, "Status code should be 403")

	var result S3Error
	err = xml.NewDecoder(rr.Body).Decode(&result)
	assert.NoError(t, err, "XML parsing should not error")
	assert.Equal(t, "InvalidAccessKeyId", result.Code, "Error message should indicate invalid access key")
}

func TestListObjectsWithPrefix(t *testing.T) {
	tb := setupDistributedBackend(t)
	defer tb.Cleanup()

	bucket := testBucket

	// Upload objects with different prefixes
	for _, obj := range []struct {
		key     string
		content []byte
	}{
		{"test-file.txt", []byte("test content")},
		{"test-other.txt", []byte("other test content")},
		{"data-file.txt", []byte("data content")},
	} {
		req := httptest.NewRequest(http.MethodPut, "/"+bucket+"/"+obj.key, bytes.NewReader(obj.content))
		if len(tb.Config.Auth) > 0 {
			authEntry := tb.Config.Auth[0]
			timestamp := time.Now().UTC().Format("20060102T150405Z")
			err := auth.GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
			require.NoError(t, err)
		}
		rr := httptest.NewRecorder()
		tb.Handler.ServeHTTP(rr, req)
		require.Equal(t, 200, rr.Code, "PUT %s should return 200", obj.key)
	}

	// List with prefix "test"
	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?prefix=test", nil)
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := auth.GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
		require.NoError(t, err)
	}
	rr := httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, req)

	assert.Equal(t, 200, rr.Code, "Status code should be 200")

	var result ListObjectsV2
	err := xml.NewDecoder(rr.Body).Decode(&result)
	assert.NoError(t, err, "XML parsing should not error")

	assert.Equal(t, bucket, result.Name, "Bucket name should match")
	assert.Equal(t, "test", result.Prefix, "Prefix should match")

	if result.Contents != nil {
		foundTest := false
		foundOther := false
		foundData := false
		for _, item := range *result.Contents {
			if item.Key == "test-file.txt" {
				foundTest = true
			}
			if item.Key == "test-other.txt" {
				foundOther = true
			}
			if item.Key == "data-file.txt" {
				foundData = true
			}
		}
		assert.True(t, foundTest, "test-file.txt should be in filtered results")
		assert.True(t, foundOther, "test-other.txt should be in filtered results")
		assert.False(t, foundData, "data-file.txt should NOT be in filtered results")
	}
}

func TestListInvalidBucket(t *testing.T) {
	tb := setupDistributedBackend(t)
	defer tb.Cleanup()

	req := httptest.NewRequest(http.MethodGet, "/invalidbucket", nil)
	if len(tb.Config.Auth) > 0 {
		authEntry := tb.Config.Auth[0]
		timestamp := time.Now().UTC().Format("20060102T150405Z")
		err := auth.GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, tb.Config.Region, "s3", req)
		require.NoError(t, err)
	}

	rr := httptest.NewRecorder()
	tb.Handler.ServeHTTP(rr, req)

	assert.Equal(t, 404, rr.Code, "Status code should be 404")

	body, err := io.ReadAll(rr.Body)
	assert.NoError(t, err, "Reading body should not error")
	var s3error S3Error
	err = xml.Unmarshal(body, &s3error)
	assert.NoError(t, err, "XML parsing failed")
	assert.Equal(t, "NoSuchBucket", s3error.Code, "Error message should indicate invalid bucket")
}
