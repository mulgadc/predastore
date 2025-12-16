package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/mulgadc/predastore/s3/distributed"
)

func main() {

	d, err := distributed.New(distributed.Backend{BadgerDir: "s3/tests/data/distributed/badger"})

	if err != nil {
		panic(err)
	}

	// Close DB
	defer d.DB.Badger.Close()

	mode := flag.String("mode", "upload", "Mode: upload, download")
	filename := flag.String("file", "", "File to upload or download")
	out := flag.String("out", "", "File to save the downloaded object")

	flag.Parse()

	if *filename == "" {
		panic("Please provide a file to upload/download using -file")
	}

	if *mode == "upload" {

		err = d.PutObject("test-bucket", *filename, nil)

		if err != nil {
			fmt.Println("Error uploading object:", err)
			panic(err)
		}

	}

	if *mode == "download" {

		var outW io.Writer = os.Stdout

		if *out != "" { // optionally also treat no output as stdout
			outFile, err := os.Create(*out) // creates/truncates, write-only
			if err != nil {
				panic(err)
			}
			defer outFile.Close()
			outW = outFile
		}

		if err := d.Get("test-bucket", *filename, outW, nil); err != nil {
			panic(err)
		}

	}

}
