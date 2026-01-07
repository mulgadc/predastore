package s3db

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestS3DB(t *testing.T) {

	tmpDir, err := os.MkdirTemp(os.TempDir(), "s3db-test")

	t.Log("tmpDir", tmpDir)

	assert.NoError(t, err, "MkdirTemp should not fail")

	//defer os.RemoveAll(tmpDir)

	db, err := New(tmpDir)

	assert.NoError(t, err)
	assert.NotNil(t, db)

	defer db.Close()

	err = db.Set([]byte("test"), []byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	value, err := db.Get([]byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, []byte("test"), value)

}

func TestS3DB_BucketsByAccountID(t *testing.T) {
	tmpDir, err := os.MkdirTemp(os.TempDir(), "s3db-test-account")
	assert.NoError(t, err, "MkdirTemp should not fail")
	defer os.RemoveAll(tmpDir)

	db, err := New(tmpDir)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	// Create 10 buckets with different account IDs
	// Account ID 123456789012 will have 5 buckets
	// Account ID 987654321098 will have 3 buckets
	// Account ID 555555555555 will have 2 buckets

	buckets := []struct {
		arn       string
		value     string
		accountID string
	}{
		{"arn:aws:s3::123456789012:devopscube-bucket", "bucket-data-1", "123456789012"},
		{"arn:aws:s3::123456789012:my-app-bucket", "bucket-data-2", "123456789012"},
		{"arn:aws:s3::123456789012:test-bucket", "bucket-data-3", "123456789012"},
		{"arn:aws:s3::123456789012:prod-bucket", "bucket-data-4", "123456789012"},
		{"arn:aws:s3::123456789012:staging-bucket", "bucket-data-5", "123456789012"},
		{"arn:aws:s3::987654321098:customer-bucket", "bucket-data-6", "987654321098"},
		{"arn:aws:s3::987654321098:archive-bucket", "bucket-data-7", "987654321098"},
		{"arn:aws:s3::987654321098:backup-bucket", "bucket-data-8", "987654321098"},
		{"arn:aws:s3::555555555555:shared-bucket", "bucket-data-9", "555555555555"},
		{"arn:aws:s3::555555555555:public-bucket", "bucket-data-10", "555555555555"},
	}

	// Insert all buckets
	for _, bucket := range buckets {
		err := db.Set([]byte(bucket.arn), []byte(bucket.value))
		assert.NoError(t, err, "Failed to set bucket: %s", bucket.arn)
	}

	// Test: Filter by account ID 123456789012
	accountPrefix := []byte("arn:aws:s3::123456789012:")
	keys, err := db.ListKeys(accountPrefix)
	assert.NoError(t, err, "Failed to list keys by account ID")

	expectedCount := 5
	assert.Equal(t, expectedCount, len(keys), "Expected %d buckets for account 123456789012, got %d", expectedCount, len(keys))

	// Verify all returned keys belong to the account
	for _, key := range keys {
		keyStr := string(key)
		assert.True(t, strings.HasPrefix(keyStr, "arn:aws:s3::123456789012:"),
			"Key %s should belong to account 123456789012", keyStr)
	}

	// Test: Filter by account ID 987654321098
	accountPrefix2 := []byte("arn:aws:s3::987654321098:")
	keys2, err := db.ListKeys(accountPrefix2)
	assert.NoError(t, err, "Failed to list keys by account ID")

	expectedCount2 := 3
	assert.Equal(t, expectedCount2, len(keys2), "Expected %d buckets for account 987654321098, got %d", expectedCount2, len(keys2))

	// Test: Filter by account ID 555555555555
	accountPrefix3 := []byte("arn:aws:s3::555555555555:")
	keys3, err := db.ListKeys(accountPrefix3)
	assert.NoError(t, err, "Failed to list keys by account ID")

	expectedCount3 := 2
	assert.Equal(t, expectedCount3, len(keys3), "Expected %d buckets for account 555555555555, got %d", expectedCount3, len(keys3))
}

func TestS3DB_ObjectsByPrefix(t *testing.T) {
	tmpDir, err := os.MkdirTemp(os.TempDir(), "s3db-test-prefix")
	assert.NoError(t, err, "MkdirTemp should not fail")

	t.Log("tmpDir", tmpDir)
	//defer os.RemoveAll(tmpDir)

	db, err := New(tmpDir)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	accountID := "123456789012"
	bucketName := "bucket1"
	baseARN := fmt.Sprintf("arn:aws:s3::%s:%s", accountID, bucketName)

	// Create 10 prefixes, each with 10-50 objects
	prefixes := []string{
		"prefix1",
		"prefix2",
		"prefix3",
		"prefix4",
		"prefix5",
		"prefix6",
		"prefix7",
		"prefix8",
		"prefix9",
		"prefix10",
	}

	// Track expected object counts per prefix
	expectedCounts := make(map[string]int)
	totalObjects := 0

	// Generate objects for each prefix
	for i, prefix := range prefixes {
		// Each prefix gets 10 + (i * 4) objects, so prefix1 gets 10, prefix2 gets 14, etc.
		// This gives us a range from 10 to 46 objects per prefix
		objectCount := 10 + (i * 4)
		if objectCount > 50 {
			objectCount = 50
		}
		expectedCounts[prefix] = objectCount

		// Create objects at different levels
		for j := 0; j < objectCount; j++ {
			var objectKey string
			var objectARN string

			// Distribute objects across different depth levels
			if j%3 == 0 {
				// Level 1: prefix/file.txt
				objectKey = fmt.Sprintf("%s/file%d.txt", prefix, j)
			} else if j%3 == 1 {
				// Level 2: prefix/level2/file.txt
				objectKey = fmt.Sprintf("%s/level2/file%d.txt", prefix, j)
			} else {
				// Level 3: prefix/level2/level3/file.txt
				objectKey = fmt.Sprintf("%s/level2/level3/file%d.txt", prefix, j)
			}

			objectARN = fmt.Sprintf("%s/%s", baseARN, objectKey)
			value := []byte(fmt.Sprintf("content-%s-%d", prefix, j))

			err := db.Set([]byte(objectARN), value)
			assert.NoError(t, err, "Failed to set object: %s", objectARN)
			totalObjects++
		}
	}

	t.Logf("Created %d total objects across %d prefixes", totalObjects, len(prefixes))

	// Test 1: List all objects for a specific prefix (e.g., prefix1/)
	prefix1ARN := fmt.Sprintf("%s/%s/", baseARN, "prefix1")
	keys, err := db.ListKeys([]byte(prefix1ARN))
	assert.NoError(t, err, "Failed to list keys for prefix1")

	expectedPrefix1Count := expectedCounts["prefix1"]
	assert.Equal(t, expectedPrefix1Count, len(keys),
		"Expected %d objects for prefix1, got %d", expectedPrefix1Count, len(keys))

	// Verify all keys belong to prefix1
	for _, key := range keys {
		keyStr := string(key)
		assert.True(t, strings.HasPrefix(keyStr, prefix1ARN),
			"Key %s should belong to prefix1", keyStr)
	}

	// Test 2: List all objects for a deeper prefix (e.g., prefix1/level2/level3/)
	deepPrefixARN := fmt.Sprintf("%s/%s/level2/level3/", baseARN, "prefix1")
	deepKeys, err := db.ListKeys([]byte(deepPrefixARN))
	assert.NoError(t, err, "Failed to list keys for deep prefix")

	// Count how many objects should be at level 3 for prefix1
	// Objects at level 3 are those where j%3 == 2
	expectedDeepCount := 0
	for j := 0; j < expectedCounts["prefix1"]; j++ {
		if j%3 == 2 {
			expectedDeepCount++
		}
	}

	assert.Equal(t, expectedDeepCount, len(deepKeys),
		"Expected %d objects at level3 for prefix1, got %d", expectedDeepCount, len(deepKeys))

	// Verify all keys belong to the deep prefix
	for _, key := range deepKeys {
		keyStr := string(key)
		assert.True(t, strings.HasPrefix(keyStr, deepPrefixARN),
			"Key %s should belong to deep prefix", keyStr)
	}

	// Test 3: List all objects for prefix1/level2/ (should include level2 and level3 objects)
	level2PrefixARN := fmt.Sprintf("%s/%s/level2/", baseARN, "prefix1")
	level2Keys, err := db.ListKeys([]byte(level2PrefixARN))
	assert.NoError(t, err, "Failed to list keys for level2 prefix")

	// Count objects at level2 and level3
	expectedLevel2Count := 0
	for j := 0; j < expectedCounts["prefix1"]; j++ {
		if j%3 == 1 || j%3 == 2 {
			expectedLevel2Count++
		}
	}

	assert.Equal(t, expectedLevel2Count, len(level2Keys),
		"Expected %d objects at level2+level3 for prefix1, got %d", expectedLevel2Count, len(level2Keys))

	// Test 4: Verify all prefixes have correct object counts
	for _, prefix := range prefixes {
		prefixARN := fmt.Sprintf("%s/%s/", baseARN, prefix)
		keys, err := db.ListKeys([]byte(prefixARN))
		assert.NoError(t, err, "Failed to list keys for prefix: %s", prefix)

		expectedCount := expectedCounts[prefix]
		assert.Equal(t, expectedCount, len(keys),
			"Expected %d objects for %s, got %d", expectedCount, prefix, len(keys))
	}

	// Test 5: List all objects for the entire bucket
	bucketARN := fmt.Sprintf("%s/", baseARN)
	allKeys, err := db.ListKeys([]byte(bucketARN))
	assert.NoError(t, err, "Failed to list all keys for bucket")

	assert.Equal(t, totalObjects, len(allKeys),
		"Expected %d total objects in bucket, got %d", totalObjects, len(allKeys))
}

func TestS3DB_MultipleBucketsMultiplePrefixes(t *testing.T) {
	tmpDir, err := os.MkdirTemp(os.TempDir(), "s3db-test-multi")
	assert.NoError(t, err, "MkdirTemp should not fail")

	t.Log("tmpDir", tmpDir)
	//defer os.RemoveAll(tmpDir)

	db, err := New(tmpDir)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close()

	accountID := "123456789012"

	// Create 3 buckets
	buckets := []string{"bucket1", "bucket2", "bucket3"}
	prefixes := []string{"prefix1", "prefix2", "prefix3", "prefix4", "prefix5",
		"prefix6", "prefix7", "prefix8", "prefix9", "prefix10"}

	// Track objects per bucket/prefix
	objectCounts := make(map[string]int)
	totalObjects := 0

	// Create objects across all buckets and prefixes
	for _, bucket := range buckets {
		baseARN := fmt.Sprintf("arn:aws:s3::%s:%s", accountID, bucket)

		for i, prefix := range prefixes {
			objectCount := 10 + (i * 4)
			if objectCount > 50 {
				objectCount = 50
			}

			for j := 0; j < objectCount; j++ {
				var objectKey string
				if j%3 == 0 {
					objectKey = fmt.Sprintf("%s/file%d.txt", prefix, j)
				} else if j%3 == 1 {
					objectKey = fmt.Sprintf("%s/level2/file%d.txt", prefix, j)
				} else {
					objectKey = fmt.Sprintf("%s/level2/level3/file%d.txt", prefix, j)
				}

				objectARN := fmt.Sprintf("%s/%s", baseARN, objectKey)
				value := []byte(fmt.Sprintf("content-%s-%s-%d", bucket, prefix, j))

				err := db.Set([]byte(objectARN), value)
				assert.NoError(t, err, "Failed to set object: %s", objectARN)

				key := fmt.Sprintf("%s/%s", bucket, prefix)
				objectCounts[key]++
				totalObjects++
			}
		}
	}

	t.Logf("Created %d total objects across %d buckets and %d prefixes",
		totalObjects, len(buckets), len(prefixes))

	// Test: Filter by account ID - should get all objects from all buckets
	accountPrefix := []byte(fmt.Sprintf("arn:aws:s3::%s:", accountID))
	allAccountKeys, err := db.ListKeys(accountPrefix)
	assert.NoError(t, err, "Failed to list all keys for account")

	assert.Equal(t, totalObjects, len(allAccountKeys),
		"Expected %d total objects for account, got %d", totalObjects, len(allAccountKeys))

	// Test: Filter by specific bucket
	bucket1ARN := fmt.Sprintf("arn:aws:s3::%s:bucket1/", accountID)
	bucket1Keys, err := db.ListKeys([]byte(bucket1ARN))
	assert.NoError(t, err, "Failed to list keys for bucket1")

	expectedBucket1Count := 0
	for _, prefix := range prefixes {
		key := fmt.Sprintf("bucket1/%s", prefix)
		expectedBucket1Count += objectCounts[key]
	}

	assert.Equal(t, expectedBucket1Count, len(bucket1Keys),
		"Expected %d objects in bucket1, got %d", expectedBucket1Count, len(bucket1Keys))

	// Test: Filter by specific bucket and prefix
	bucket1Prefix1ARN := fmt.Sprintf("arn:aws:s3::%s:bucket1/prefix1/", accountID)
	bucket1Prefix1Keys, err := db.ListKeys([]byte(bucket1Prefix1ARN))
	assert.NoError(t, err, "Failed to list keys for bucket1/prefix1")

	expectedCount := objectCounts["bucket1/prefix1"]
	assert.Equal(t, expectedCount, len(bucket1Prefix1Keys),
		"Expected %d objects in bucket1/prefix1, got %d", expectedCount, len(bucket1Prefix1Keys))

	// Test: Filter by deep prefix
	bucket1Prefix1Level3ARN := fmt.Sprintf("arn:aws:s3::%s:bucket1/prefix1/level2/level3/", accountID)
	deepKeys, err := db.ListKeys([]byte(bucket1Prefix1Level3ARN))
	assert.NoError(t, err, "Failed to list keys for deep prefix")

	// Calculate expected deep count (objects where j%3 == 2)
	expectedDeepCount := 0
	for j := 0; j < expectedCount; j++ {
		if j%3 == 2 {
			expectedDeepCount++
		}
	}

	assert.Equal(t, expectedDeepCount, len(deepKeys),
		"Expected %d objects at level3, got %d", expectedDeepCount, len(deepKeys))
}
