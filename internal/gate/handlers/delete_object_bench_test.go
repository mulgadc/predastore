package handlers

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/mulgadc/predastore/internal/gate/model"
)

// benchSeed stores one object the way deleteFixture.seed does, but reports
// failure through a *testing.B, which seed cannot take.
func benchSeed(b *testing.B, f deleteFixture, key string, body []byte) {
	b.Helper()
	ctx := context.Background()
	objectHash := model.ObjectHash(deleteTestBucket, key)

	place, _, err := f.write.write(ctx, objectHash, bytes.NewReader(body), int64(len(body)))
	if err != nil {
		b.Fatal(err)
	}
	record, err := EncodePlacement(place)
	if err != nil {
		b.Fatal(err)
	}
	if err := metaPut(ctx, f.write.mc, model.TableObjects, string(objectHash[:]), record); err != nil {
		b.Fatal(err)
	}
	if err := metaPut(ctx, f.write.mc, model.TableObjects, objectARN(deleteTestBucket, key), objectHash[:]); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkDeleteStoredObject times one inline S3 DELETE of a seeded object:
// a fan-out that succeeds, and one that fails and leaves a tombstone.
func BenchmarkDeleteStoredObject(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"1KiB", 1 << 10},
		{"1MiB", 1 << 20},
	}
	for _, mode := range []string{"fanout-ok", "fanout-failed"} {
		for _, sz := range sizes {
			b.Run(fmt.Sprintf("%s/%s", mode, sz.name), func(b *testing.B) {
				f := deleteFixture{write: newWriteFixture(4, 2)}
				var bc BlobClient = f.write.bc
				if mode == "fanout-failed" {
					bc = &failingBlob{fakeBlob: f.write.bc}
				}
				body := bytes.Repeat([]byte("x"), sz.size)
				ctx := context.Background()

				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					benchSeed(b, f, "a.txt", body)
					b.StartTimer()
					if err := deleteStoredObject(ctx, f.write.mc, bc, deleteTestBucket, "a.txt"); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
