package main

import (
	"flag"
	"fmt"
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

	if *out == "" && *mode == "download" {
		panic("Please provide a file to save the downloaded object using -out")
	}

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

		f, err := os.OpenFile(*out, os.O_CREATE|os.O_RDWR, 0640)

		if err != nil {
			panic(err)
		}
		defer f.Close()

		err = d.Get("test-bucket", *filename, f, nil)

		if err != nil {
			panic(err)
		}

	}

}
