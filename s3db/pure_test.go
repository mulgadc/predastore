package s3db

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenObjectHash(t *testing.T) {
	t.Run("deterministic", func(t *testing.T) {
		h1 := GenObjectHash("bucket", "key")
		h2 := GenObjectHash("bucket", "key")
		assert.Equal(t, h1, h2)
	})

	t.Run("matches manual sha256", func(t *testing.T) {
		expected := sha256.Sum256([]byte("mybucket/mykey"))
		assert.Equal(t, expected, GenObjectHash("mybucket", "mykey"))
	})

	t.Run("different inputs produce different hashes", func(t *testing.T) {
		h1 := GenObjectHash("bucket-a", "key")
		h2 := GenObjectHash("bucket-b", "key")
		assert.NotEqual(t, h1, h2)
	})

	t.Run("format is bucket/object", func(t *testing.T) {
		expected := sha256.Sum256([]byte("b/k"))
		assert.Equal(t, expected, GenObjectHash("b", "k"))
	})
}
