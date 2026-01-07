package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/mulgadc/predastore/backend/distributed"
)

func main() {
	cfg := &distributed.Config{
		BadgerDir: "s3/tests/data/distributed/badger",
	}

	b, err := distributed.New(cfg)
	if err != nil {
		panic(err)
	}
	defer b.Close()

	// Cast to access distributed-specific methods
	d := b.(*distributed.Backend)

	mode := flag.String("mode", "upload", "Mode: upload, download")
	filename := flag.String("file", "", "File to upload or download")
	out := flag.String("out", "", "File to save the downloaded object")

	flag.Parse()

	if *filename == "" {
		panic("Please provide a file to upload/download using -file")
	}

	var outW io.Writer = os.Stdout

	if *out != "" {
		outFile, err := os.Create(*out)
		if err != nil {
			panic(err)
		}
		defer outFile.Close()
		outW = outFile
	}

	ctx := context.Background()

	if *mode == "upload" {
		err = d.PutObjectFromPath(ctx, "test-bucket", *filename)
		if err != nil {
			fmt.Println("Error uploading object:", err)
			panic(err)
		}
	}

	if *mode == "download" {
		var buf bytes.Buffer
		if err := d.GetFromPath(ctx, "test-bucket", *filename, &buf); err != nil {
			fmt.Println("Error downloading object:", err)
		} else {
			outW.Write(buf.Bytes())
		}
	}
}
