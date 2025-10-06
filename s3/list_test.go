package s3

import (
	"encoding/xml"
	"io"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestListBuckets(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a request to list buckets
	req := httptest.NewRequest("GET", "/", nil)

	// Add authentication headers using the credentials from server.toml
	if len(s3.Auth) > 0 {
		// Use the first auth entry from the config
		authEntry := s3.Auth[0]

		// Use our utility function to generate a valid authorization header
		timestamp := time.Now().UTC().Format("20060102T150405Z")

		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, s3.Region, "s3", req)
		assert.NoError(t, err, "Error generating auth header")
	}

	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 200, resp.StatusCode, "Status code should be 200")

	// Parse the XML response
	var result ListBuckets
	err = xml.NewDecoder(resp.Body).Decode(&result)
	assert.NoError(t, err, "XML parsing should not error")

	// Verify the response contains our test bucket
	assert.Equal(t, len(result.Buckets), 5, "Should have 4 buckets")

	t.Log("Buckets", result.Buckets)

	if len(result.Buckets) == 5 {
		assert.Equal(t, result.Buckets[0].Name, "test-bucket01", "Test bucket should be in the list")
		assert.Equal(t, result.Buckets[1].Name, "private", "Private bucket should be in the list")
		assert.Equal(t, result.Buckets[2].Name, "secure", "Secure bucket should be in the list")
		assert.Equal(t, result.Buckets[3].Name, "local", "Local bucket should be in the list")
		assert.Equal(t, result.Buckets[4].Name, "predastore", "Predastore bucket should be in the list")
	}

}

// Test list buckets with no authentication, should fail.
func TestListBucketsNoAuth(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a request to list buckets
	req := httptest.NewRequest("GET", "/", nil)

	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 403, resp.StatusCode, "Status code should be 403")

}

func TestListObjectsV2Handler(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a request to list objects in the test bucket
	req := httptest.NewRequest("GET", "/test-bucket01", nil)

	// Add auth for non-public buckets (testbucket is public, but adding auth won't hurt)
	if len(s3.Auth) > 0 {
		// Use the first auth entry from the config
		authEntry := s3.Auth[0]

		// Use our utility function to generate a valid authorization header
		timestamp := time.Now().UTC().Format("20060102T150405Z")

		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, s3.Region, "s3", req)
		assert.NoError(t, err, "Error generating auth header")

	}

	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 200, resp.StatusCode, "Status code should be 200")

	// Parse the XML response
	var result ListObjectsV2
	err = xml.NewDecoder(resp.Body).Decode(&result)
	assert.NoError(t, err, "XML parsing should not error")

	// Verify response
	assert.Equal(t, "test-bucket01", result.Name, "Bucket name should match")
	assert.NotNil(t, result.Contents, "Contents should not be nil")

	// Check that our test files are in the results
	foundText := false
	foundBinary := false

	if result.Contents != nil {
		for _, item := range *result.Contents {
			if item.Key == "test.txt" {
				foundText = true
			}
			if item.Key == "binary.dat" {
				foundBinary = true
			}
		}
	}

	assert.True(t, foundText, "test.txt should be in the bucket")
	assert.True(t, foundBinary, "binary.dat should be in the bucket")
}

// Test list objects to a private bucket, with no auth
func TestListObjectsV2HandlerPrivateBucketNoAuth(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a request to list objects in the test bucket
	req := httptest.NewRequest("GET", "/private", nil)

	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 403, resp.StatusCode, "Status code should be 403")

	// Parse the XML response
	var result S3Error
	err = xml.NewDecoder(resp.Body).Decode(&result)
	assert.NoError(t, err, "XML parsing should not error")

	assert.Equal(t, result.Code, "AccessDenied", "Error message should indicate access denied")
}

func TestListObjectsV2HandlerPrivateBucketBadAuth(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a request to list objects in the test bucket
	req := httptest.NewRequest("GET", "/private", nil)

	// Use our utility function to generate a valid authorization header
	timestamp := time.Now().UTC().Format("20060102T150405Z")

	err = GenerateAuthHeaderReq("BADACCESSKEY", "BADSECRETKEY", timestamp, s3.Region, "s3", req)
	assert.NoError(t, err, "Error generating auth header")

	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 403, resp.StatusCode, "Status code should be 403")

	// Parse the XML response
	var result S3Error
	err = xml.NewDecoder(resp.Body).Decode(&result)
	assert.NoError(t, err, "XML parsing should not error")

	assert.Equal(t, result.Code, "AccessDenied", "Error message should indicate access denied")
}

// Test list objects to a public bucket, with no auth
func TestListObjectsV2HandlerPublicBucketNoAuth(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a request to list objects in the test bucket
	req := httptest.NewRequest("GET", "/test-bucket01", nil)

	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 200, resp.StatusCode, "Status code should be 200")

	// Parse the XML response
	var result ListObjectsV2
	err = xml.NewDecoder(resp.Body).Decode(&result)
	assert.NoError(t, err, "XML parsing should not error")

	foundText := false

	if result.Contents != nil {
		for _, item := range *result.Contents {
			if item.Key == "test.txt" {
				foundText = true
			}
		}
	}

	assert.True(t, foundText, "test.txt should be in the bucket")

}

func TestListObjectsWithPrefix(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a request to list objects with prefix
	req := httptest.NewRequest("GET", "/test-bucket01?prefix=test", nil)

	// Add auth for non-public buckets (testbucket is public, but adding auth won't hurt)
	if len(s3.Auth) > 0 {
		// Use the first auth entry from the config
		authEntry := s3.Auth[0]

		// Use our utility function to generate a valid authorization header
		timestamp := time.Now().UTC().Format("20060102T150405Z")

		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, s3.Region, "s3", req)
		assert.NoError(t, err, "Error generating auth header")

	}

	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 200, resp.StatusCode, "Status code should be 200")

	// Parse the XML response
	var result ListObjectsV2
	err = xml.NewDecoder(resp.Body).Decode(&result)
	assert.NoError(t, err, "XML parsing should not error")

	// Verify only test.txt is in the response
	assert.Equal(t, "test-bucket01", result.Name, "Bucket name should match")
	assert.Equal(t, "test", result.Prefix, "Prefix should match")

	if result.Contents != nil {
		foundText := false
		foundBinary := false

		for _, item := range *result.Contents {
			if item.Key == "test.txt" {
				foundText = true
			}
			if item.Key == "binary.dat" {
				foundBinary = true
			}
		}

		assert.True(t, foundText, "test.txt should be in the filtered results")
		assert.False(t, foundBinary, "binary.dat should not be in the filtered results")
	}
}

func TestListInvalidBucket(t *testing.T) {
	s3 := New(&Config{
		ConfigPath: filepath.Join("tests", "config", "server.toml"),
	})
	err := s3.ReadConfig()
	assert.NoError(t, err, "Should read config without error")

	// Setup Fiber app using SetupRoutes
	app := s3.SetupRoutes()

	// Make a request to list objects in an invalid bucket
	req := httptest.NewRequest("GET", "/invalidbucket", nil)

	// Add authentication headers since routes.go may require it
	if len(s3.Auth) > 0 {
		// Use the first auth entry from the config
		authEntry := s3.Auth[0]

		// Use our utility function to generate a valid authorization header
		timestamp := time.Now().UTC().Format("20060102T150405Z")

		err := GenerateAuthHeaderReq(authEntry.AccessKeyID, authEntry.SecretAccessKey, timestamp, s3.Region, "s3", req)
		assert.NoError(t, err, "Error generating auth header")
	}

	resp, err := app.Test(req)

	assert.NoError(t, err, "Request should not error")
	assert.Equal(t, 404, resp.StatusCode, "Status code should be 404")

	// Read response body
	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "Reading body should not error")
	var s3error S3Error

	err = xml.Unmarshal(body, &s3error)

	assert.NoError(t, err, "XML parsing failed")

	assert.Equal(t, s3error.Code, "NoSuchBucket", "Error message should indicate invalid bucket")
	assert.Equal(t, s3error.Message, "The specified bucket does not exist")
}
