package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// x-amz-copy-source is URL-encoded the way a request path is: + is a literal
// plus, and the query is split off before decoding so an encoded ? stays part
// of the key.
func TestParseCopySource(t *testing.T) {
	for _, tc := range []struct {
		raw, bucket, key, versionID string
	}{
		{"/src/plain.txt", "src", "plain.txt", ""},
		{"src/plain.txt", "src", "plain.txt", ""},
		{"/src/dir%20with%20space/%C3%BC-file%2Bplus.txt", "src", "dir with space/ü-file+plus.txt", ""},
		{"/src/a+b", "src", "a+b", ""},
		{"/src/100%25", "src", "100%", ""},
		{"/src/what%3F.txt", "src", "what?.txt", ""},
		{"/src/what%3F.txt?versionId=v%2B1", "src", "what?.txt", "v+1"},
		{"/src/a%20b?versionId=abc", "src", "a b", "abc"},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			bucket, key, versionID, err := parseCopySource(tc.raw)
			require.NoError(t, err)
			assert.Equal(t, tc.bucket, bucket)
			assert.Equal(t, tc.key, key)
			assert.Equal(t, tc.versionID, versionID)
		})
	}
}

func TestParseCopySourceRejects(t *testing.T) {
	for _, raw := range []string{"", "/src", "/src/", "/src/bad%zz", "/src/a?versionId=%zz"} {
		t.Run(raw, func(t *testing.T) {
			_, _, _, err := parseCopySource(raw)
			assert.Error(t, err)
		})
	}
}
