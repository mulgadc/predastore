package main

import (
	"flag"
	"fmt"

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
	flag.Parse()

	if *filename == "" {
		panic("Please provide a file to upload using -file")
	}

	if *mode == "upload" {

		err = d.PutObject("test-bucket", *filename, nil)

		if err != nil {
			fmt.Println("Error uploading object:", err)
			panic(err)
		}

	}

	if *mode == "download" {

		err = d.Get("test-bucket", *filename, nil)

		if err != nil {
			panic(err)
		}

	}

}
