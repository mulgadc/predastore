package gate

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

// BenchmarkObjectValidate measures the key validation every object request
// runs in resolveObject, for a typical key and one at S3's 1,024-byte limit.
func BenchmarkObjectValidate(b *testing.B) {
	router := chi.NewRouter()
	router.With(resolveObject).Get("/{bucket}/*", func(http.ResponseWriter, *http.Request) {})

	keys := map[string]string{
		"20B":   "photos/2026/img1.jpg",
		"1024B": strings.Repeat("abcdefgh/", 113) + "abcdefg",
	}
	for name, key := range keys {
		b.Run(name, func(b *testing.B) {
			req := httptest.NewRequest(http.MethodGet, "/owner-bucket/"+key, nil)
			w := httptest.NewRecorder()
			b.ReportAllocs()
			for b.Loop() {
				router.ServeHTTP(w, req)
			}
			if w.Code != http.StatusOK {
				b.Fatalf("status %d", w.Code)
			}
		})
	}
}
