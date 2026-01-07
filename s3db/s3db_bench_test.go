// Package s3db provides benchmarks for S3DB performance testing.
//
// Benchmarks test realistic S3 usage patterns with ARN-based keys:
//   - Insert performance (single and batch)
//   - Prefix scan performance (different prefix lengths and result set sizes)
//   - Read performance (point lookups)
//
// Usage:
//
// Run all benchmarks:
//
//	go test ./s3db -run=^$ -bench=. -benchmem
//
// Run specific benchmark:
//
//	go test ./s3db -run=^$ -bench=BenchmarkInsert -benchmem
//
// Run with custom benchtime:
//
//	go test ./s3db -run=^$ -bench=. -benchtime=5s -benchmem
//
// Run quick benchmarks (reduced dataset):
//
//	go test ./s3db -run=^$ -bench=. -benchmem -short
package s3db

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// benchFixture holds pre-populated test data
type benchFixture struct {
	db       *S3DB
	tmpDir   string
	accounts []string
	buckets  map[string][]string // account -> bucket names
}

// setupFixture creates and populates a test database
func setupFixture(b *testing.B) *benchFixture {
	b.Helper()

	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("s3db-bench-%d-", time.Now().UnixNano()))
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}

	db, err := New(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		b.Fatalf("Failed to create DB: %v", err)
	}

	fixture := &benchFixture{
		db:       db,
		tmpDir:   tmpDir,
		accounts: make([]string, 0),
		buckets:  make(map[string][]string),
	}

	// Determine scale based on -short flag
	accountCount := 10
	if testing.Short() {
		accountCount = 2
	}

	// Populate test data
	objectCounts := map[string]int{
		"images":    1000,
		"documents": 5000,
		"archive":   10000,
	}

	if testing.Short() {
		objectCounts = map[string]int{
			"images":    100,
			"documents": 500,
			"archive":   1000,
		}
	}

	b.Logf("Populating fixture: %d accounts, %d object types", accountCount, len(objectCounts))
	startTime := time.Now()
	totalObjects := 0

	for acctIdx := 0; acctIdx < accountCount; acctIdx++ {
		accountID := fmt.Sprintf("%012d", acctIdx+1)
		fixture.accounts = append(fixture.accounts, accountID)
		fixture.buckets[accountID] = make([]string, 0)

		for bucketName, objCount := range objectCounts {
			fixture.buckets[accountID] = append(fixture.buckets[accountID], bucketName)
			baseARN := fmt.Sprintf("arn:aws:s3::%s:%s", accountID, bucketName)

			for objIdx := 0; objIdx < objCount; objIdx++ {
				objectKey := generateObjectKey(bucketName, objIdx, objCount)
				objectARN := fmt.Sprintf("%s/%s", baseARN, objectKey)
				value := []byte(fmt.Sprintf("content-%s-%d", bucketName, objIdx))

				if err := db.Set([]byte(objectARN), value); err != nil {
					b.Fatalf("Failed to populate: %v", err)
				}
				totalObjects++
			}
		}
	}

	elapsed := time.Since(startTime)
	b.Logf("Populated %d objects in %v (%.0f ops/sec)", totalObjects, elapsed, float64(totalObjects)/elapsed.Seconds())

	return fixture
}

// teardownFixture cleans up test resources
func (f *benchFixture) teardown() {
	if f.db != nil {
		f.db.Close()
	}
	if f.tmpDir != "" {
		os.RemoveAll(f.tmpDir)
	}
}

// generateObjectKey creates realistic S3 object keys with various patterns
func generateObjectKey(bucketName string, fileIdx int, totalFiles int) string {
	switch bucketName {
	case "images":
		// Pattern: image-00000.jpg, image-00001.jpg, etc.
		if fileIdx < 100 {
			return fmt.Sprintf("image-%05d.jpg", fileIdx)
		}
		// Pattern: thumbnails/image-N.jpg
		if fileIdx < 500 {
			return fmt.Sprintf("thumbnails/image-%d.jpg", fileIdx)
		}
		// Pattern: images/2024/01/image-N.jpg
		month := ((fileIdx - 500) % 12) + 1
		return fmt.Sprintf("images/2024/%02d/image-%d.jpg", month, fileIdx)

	case "documents":
		// Pattern: doc-2024-001-N.pdf
		if fileIdx < 365 {
			return fmt.Sprintf("doc-2024-%03d-%05d.pdf", fileIdx+1, fileIdx)
		}
		// Pattern: documents/user-N/doc-M.pdf
		userID := fileIdx / 100
		docID := fileIdx % 100
		return fmt.Sprintf("documents/user-%d/doc-%d.pdf", userID, docID)

	case "archive":
		// Pattern: archive/2024/MM/DD/file-N.dat
		dayOfYear := fileIdx % 365
		month := (dayOfYear / 30) + 1
		day := (dayOfYear % 30) + 1
		return fmt.Sprintf("archive/2024/%02d/%02d/file-%05d.dat", month, day, fileIdx)

	default:
		return fmt.Sprintf("file-%d.txt", fileIdx)
	}
}

// BenchmarkInsert tests insert performance
func BenchmarkInsert(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "s3db-bench-insert-")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	db, err := New(tmpDir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	testCases := []struct {
		name      string
		valueSize int
	}{
		{"small-100B", 100},
		{"medium-1KB", 1024},
		{"large-10KB", 10240},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			value := make([]byte, tc.valueSize)
			for i := range value {
				value[i] = byte(i % 256)
			}

			accountID := "000000000001"
			bucketName := "test-insert"

			b.ResetTimer()
			b.ReportAllocs()
			b.SetBytes(int64(tc.valueSize))

			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("arn:aws:s3::%s:%s/obj-%d.dat", accountID, bucketName, i)
				if err := db.Set([]byte(key), value); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkPrefixScan tests prefix scan performance with various scenarios
func BenchmarkPrefixScan(b *testing.B) {
	fixture := setupFixture(b)
	defer fixture.teardown()

	accountID := fixture.accounts[0]

	testCases := []struct {
		name         string
		prefix       string
		expectedSize string // approximate result set size
	}{
		// Account-level prefix (all objects in account)
		{"account-level", fmt.Sprintf("arn:aws:s3::%s:", accountID), "all"},

		// Bucket-level prefixes (all objects in bucket)
		{"bucket-images", fmt.Sprintf("arn:aws:s3::%s:images/", accountID), "~1k"},
		{"bucket-documents", fmt.Sprintf("arn:aws:s3::%s:documents/", accountID), "~5k"},
		{"bucket-archive", fmt.Sprintf("arn:aws:s3::%s:archive/", accountID), "~10k"},

		// Shallow prefix (moderate result set)
		{"prefix-image-", fmt.Sprintf("arn:aws:s3::%s:images/image-", accountID), "~100"},
		{"prefix-thumbnails", fmt.Sprintf("arn:aws:s3::%s:images/thumbnails/", accountID), "~400"},
		{"prefix-doc-2024", fmt.Sprintf("arn:aws:s3::%s:documents/doc-2024-", accountID), "~365"},

		// Deep prefix (small result set)
		{"deep-images-2024-01", fmt.Sprintf("arn:aws:s3::%s:images/images/2024/01/", accountID), "~50"},
		{"deep-archive-2024-01-01", fmt.Sprintf("arn:aws:s3::%s:archive/archive/2024/01/01/", accountID), "~30"},
		{"deep-documents-user-10", fmt.Sprintf("arn:aws:s3::%s:documents/documents/user-10/", accountID), "~100"},

		// Very specific prefix (tiny result set)
		{"specific-image-00000", fmt.Sprintf("arn:aws:s3::%s:images/image-00000", accountID), "1"},
		{"specific-doc-001", fmt.Sprintf("arn:aws:s3::%s:documents/doc-2024-001-", accountID), "1"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			prefix := []byte(tc.prefix)

			// Warmup call to verify prefix works
			keys, err := fixture.db.ListKeys(prefix)
			if err != nil {
				b.Fatalf("ListKeys failed: %v", err)
			}
			resultCount := len(keys)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				keys, err := fixture.db.ListKeys(prefix)
				if err != nil {
					b.Fatal(err)
				}
				if len(keys) != resultCount {
					b.Fatalf("Result count mismatch: got %d, expected %d", len(keys), resultCount)
				}
			}

			b.ReportMetric(float64(resultCount), "keys")
		})
	}
}

// BenchmarkPrefixScanByResultSize tests prefix scans categorized by result set size
func BenchmarkPrefixScanByResultSize(b *testing.B) {
	fixture := setupFixture(b)
	defer fixture.teardown()

	accountID := fixture.accounts[0]

	// Define test cases by approximate result size
	testCases := []struct {
		name   string
		prefix string
	}{
		// Tiny: 1-10 results
		{"tiny-1", fmt.Sprintf("arn:aws:s3::%s:images/image-00000", accountID)},

		// Small: 10-100 results
		{"small-100", fmt.Sprintf("arn:aws:s3::%s:images/image-", accountID)},

		// Medium: 100-1000 results
		{"medium-400", fmt.Sprintf("arn:aws:s3::%s:images/thumbnails/", accountID)},

		// Large: 1000-10000 results
		{"large-1k", fmt.Sprintf("arn:aws:s3::%s:images/", accountID)},
		{"large-5k", fmt.Sprintf("arn:aws:s3::%s:documents/", accountID)},

		// Very large: 10000+ results
		{"xlarge-10k", fmt.Sprintf("arn:aws:s3::%s:archive/", accountID)},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			prefix := []byte(tc.prefix)

			// Get expected count
			keys, err := fixture.db.ListKeys(prefix)
			if err != nil {
				b.Fatal(err)
			}
			resultCount := len(keys)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := fixture.db.ListKeys(prefix)
				if err != nil {
					b.Fatal(err)
				}
			}

			b.ReportMetric(float64(resultCount), "keys")
			b.ReportMetric(float64(resultCount*100)/float64(b.N), "keys/op")
		})
	}
}

// BenchmarkGet tests point lookup performance
func BenchmarkGet(b *testing.B) {
	fixture := setupFixture(b)
	defer fixture.teardown()

	accountID := fixture.accounts[0]

	// Prepare test keys that we know exist in the fixture
	testKeys := [][]byte{
		[]byte(fmt.Sprintf("arn:aws:s3::%s:images/image-00000.jpg", accountID)),
		[]byte(fmt.Sprintf("arn:aws:s3::%s:images/image-00050.jpg", accountID)),
		[]byte(fmt.Sprintf("arn:aws:s3::%s:documents/doc-2024-001-00000.pdf", accountID)),
		[]byte(fmt.Sprintf("arn:aws:s3::%s:archive/archive/2024/01/01/file-00000.dat", accountID)),
	}

	// Verify keys exist
	for _, key := range testKeys {
		if _, err := fixture.db.Get(key); err != nil {
			b.Fatalf("Test key %s does not exist: %v", string(key), err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := testKeys[i%len(testKeys)]
		_, err := fixture.db.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMixedWorkload tests a mixed read/write workload
func BenchmarkMixedWorkload(b *testing.B) {
	fixture := setupFixture(b)
	defer fixture.teardown()

	accountID := fixture.accounts[0]

	patterns := []struct {
		name       string
		readRatio  int // percentage of reads (0-100)
		prefixScan bool
	}{
		{"read-heavy-90", 90, false},
		{"balanced-50", 50, false},
		{"write-heavy-10", 10, false},
		{"scan-heavy-80", 80, true},
	}

	for _, pattern := range patterns {
		b.Run(pattern.name, func(b *testing.B) {
			value := []byte("test-data-mixed-workload")
			readKey := []byte(fmt.Sprintf("arn:aws:s3::%s:images/image-00050.jpg", accountID))
			scanPrefix := []byte(fmt.Sprintf("arn:aws:s3::%s:images/image-", accountID))

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Determine operation based on ratio
				if i%100 < pattern.readRatio {
					// Read operation
					if pattern.prefixScan && i%10 == 0 {
						// Occasional prefix scan
						_, _ = fixture.db.ListKeys(scanPrefix)
					} else {
						// Point read
						_, _ = fixture.db.Get(readKey)
					}
				} else {
					// Write operation
					key := []byte(fmt.Sprintf("arn:aws:s3::%s:test-mixed/obj-%d.dat", accountID, i))
					_ = fixture.db.Set(key, value)
				}
			}
		})
	}
}

// BenchmarkConcurrentPrefixScan tests concurrent prefix scanning
func BenchmarkConcurrentPrefixScan(b *testing.B) {
	fixture := setupFixture(b)
	defer fixture.teardown()

	accountID := fixture.accounts[0]
	prefix := []byte(fmt.Sprintf("arn:aws:s3::%s:images/", accountID))

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := fixture.db.ListKeys(prefix)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPrefixDepth tests how prefix depth affects performance
func BenchmarkPrefixDepth(b *testing.B) {
	fixture := setupFixture(b)
	defer fixture.teardown()

	accountID := fixture.accounts[0]

	depthCases := []struct {
		name   string
		prefix string
		depth  int
	}{
		{"depth-1-account", fmt.Sprintf("arn:aws:s3::%s:", accountID), 1},
		{"depth-2-bucket", fmt.Sprintf("arn:aws:s3::%s:archive/", accountID), 2},
		{"depth-3-year", fmt.Sprintf("arn:aws:s3::%s:archive/archive/2024/", accountID), 3},
		{"depth-4-month", fmt.Sprintf("arn:aws:s3::%s:archive/archive/2024/06/", accountID), 4},
		{"depth-5-day", fmt.Sprintf("arn:aws:s3::%s:archive/archive/2024/06/15/", accountID), 5},
	}

	for _, tc := range depthCases {
		b.Run(tc.name, func(b *testing.B) {
			prefix := []byte(tc.prefix)

			// Get result count
			keys, err := fixture.db.ListKeys(prefix)
			if err != nil {
				b.Fatal(err)
			}
			resultCount := len(keys)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := fixture.db.ListKeys(prefix)
				if err != nil {
					b.Fatal(err)
				}
			}

			b.ReportMetric(float64(tc.depth), "depth")
			b.ReportMetric(float64(resultCount), "keys")
		})
	}
}

// BenchmarkMultiAccountScan tests scanning across different accounts
func BenchmarkMultiAccountScan(b *testing.B) {
	fixture := setupFixture(b)
	defer fixture.teardown()

	if len(fixture.accounts) < 2 {
		b.Skip("Need at least 2 accounts for this benchmark")
	}

	for idx, accountID := range fixture.accounts {
		if idx >= 3 {
			break // Test first 3 accounts only
		}

		b.Run(fmt.Sprintf("account-%d", idx+1), func(b *testing.B) {
			prefix := []byte(fmt.Sprintf("arn:aws:s3::%s:", accountID))

			keys, err := fixture.db.ListKeys(prefix)
			if err != nil {
				b.Fatal(err)
			}
			resultCount := len(keys)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := fixture.db.ListKeys(prefix)
				if err != nil {
					b.Fatal(err)
				}
			}

			b.ReportMetric(float64(resultCount), "keys")
		})
	}
}

// BenchmarkConcurrentScans tests concurrent scanning with different goroutine counts
func BenchmarkConcurrentScans(b *testing.B) {
	fixture := setupFixture(b)
	defer fixture.teardown()

	accountID := fixture.accounts[0]

	// Define multiple prefixes to scan concurrently
	prefixes := []struct {
		name   string
		prefix []byte
	}{
		{"images", []byte(fmt.Sprintf("arn:aws:s3::%s:images/", accountID))},
		{"documents", []byte(fmt.Sprintf("arn:aws:s3::%s:documents/", accountID))},
		{"archive", []byte(fmt.Sprintf("arn:aws:s3::%s:archive/", accountID))},
		{"image-prefix", []byte(fmt.Sprintf("arn:aws:s3::%s:images/image-", accountID))},
		{"doc-prefix", []byte(fmt.Sprintf("arn:aws:s3::%s:documents/doc-2024-", accountID))},
		{"archive-month", []byte(fmt.Sprintf("arn:aws:s3::%s:archive/archive/2024/06/", accountID))},
	}

	// Verify all prefixes exist and get expected counts
	expectedCounts := make([]int, len(prefixes))
	for i, p := range prefixes {
		keys, err := fixture.db.ListKeys(p.prefix)
		if err != nil {
			b.Fatalf("Failed to validate prefix %s: %v", p.name, err)
		}
		expectedCounts[i] = len(keys)
	}

	goroutineCounts := []int{2, 4, 8}

	for _, numGoroutines := range goroutineCounts {
		b.Run(fmt.Sprintf("goroutines-%d", numGoroutines), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				prefixIdx := 0
				for pb.Next() {
					// Rotate through different prefixes for variety
					p := prefixes[prefixIdx%len(prefixes)]
					expectedCount := expectedCounts[prefixIdx%len(prefixes)]
					prefixIdx++

					keys, err := fixture.db.ListKeys(p.prefix)
					if err != nil {
						b.Fatalf("ListKeys failed for %s: %v", p.name, err)
					}

					if len(keys) != expectedCount {
						b.Fatalf("Expected %d keys for %s, got %d", expectedCount, p.name, len(keys))
					}
				}
			})

			b.ReportMetric(float64(numGoroutines), "goroutines")
		})
	}
}

// BenchmarkConcurrentMixedOperations tests concurrent mixed read/write operations
func BenchmarkConcurrentMixedOperations(b *testing.B) {
	fixture := setupFixture(b)
	defer fixture.teardown()

	accountID := fixture.accounts[0]

	// Prefixes for reading
	readPrefixes := [][]byte{
		[]byte(fmt.Sprintf("arn:aws:s3::%s:images/image-", accountID)),
		[]byte(fmt.Sprintf("arn:aws:s3::%s:documents/doc-2024-", accountID)),
		[]byte(fmt.Sprintf("arn:aws:s3::%s:archive/archive/2024/", accountID)),
	}

	// Test keys for point reads
	testKeys := [][]byte{
		[]byte(fmt.Sprintf("arn:aws:s3::%s:images/image-00000.jpg", accountID)),
		[]byte(fmt.Sprintf("arn:aws:s3::%s:documents/doc-2024-001-00000.pdf", accountID)),
		[]byte(fmt.Sprintf("arn:aws:s3::%s:archive/archive/2024/01/01/file-00000.dat", accountID)),
	}

	goroutineCounts := []int{2, 4, 8}

	for _, numGoroutines := range goroutineCounts {
		b.Run(fmt.Sprintf("goroutines-%d", numGoroutines), func(b *testing.B) {
			value := []byte("concurrent-test-data")

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				opCount := 0
				for pb.Next() {
					opCount++

					switch opCount % 4 {
					case 0:
						// Prefix scan
						prefix := readPrefixes[opCount%len(readPrefixes)]
						_, _ = fixture.db.ListKeys(prefix)

					case 1:
						// Point read
						key := testKeys[opCount%len(testKeys)]
						_, _ = fixture.db.Get(key)

					case 2:
						// Write operation
						key := []byte(fmt.Sprintf("arn:aws:s3::%s:concurrent-test/obj-%d.dat", accountID, opCount))
						_ = fixture.db.Set(key, value)

					case 3:
						// Another prefix scan with different prefix
						prefix := readPrefixes[(opCount+1)%len(readPrefixes)]
						_, _ = fixture.db.ListKeys(prefix)
					}
				}
			})

			b.ReportMetric(float64(numGoroutines), "goroutines")
		})
	}
}

// BenchmarkConcurrentScansWithContention tests concurrent scans on the same prefixes
func BenchmarkConcurrentScansWithContention(b *testing.B) {
	fixture := setupFixture(b)
	defer fixture.teardown()

	accountID := fixture.accounts[0]

	// Single prefix that all goroutines will access simultaneously (high contention)
	contentionPrefix := []byte(fmt.Sprintf("arn:aws:s3::%s:images/", accountID))

	// Verify prefix exists
	keys, err := fixture.db.ListKeys(contentionPrefix)
	if err != nil {
		b.Fatalf("Failed to validate prefix: %v", err)
	}
	expectedCount := len(keys)

	goroutineCounts := []int{2, 4, 8, 16}

	for _, numGoroutines := range goroutineCounts {
		b.Run(fmt.Sprintf("goroutines-%d", numGoroutines), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					keys, err := fixture.db.ListKeys(contentionPrefix)
					if err != nil {
						b.Fatalf("ListKeys failed: %v", err)
					}

					if len(keys) != expectedCount {
						b.Fatalf("Expected %d keys, got %d", expectedCount, len(keys))
					}
				}
			})

			b.ReportMetric(float64(numGoroutines), "goroutines")
			b.ReportMetric(float64(expectedCount), "keys")
		})
	}
}
