package main

import (
	"flag"

	"github.com/mulgadc/predastore/s3/distributed"
)

func main() {

	d, err := distributed.New(nil)

	if err != nil {
		panic(err)
	}

	mode := flag.String("mode", "upload", "Mode: upload, download")
	filename := flag.String("file", "", "File to upload or download")
	flag.Parse()

	if *filename == "" {
		panic("Please provide a file to upload using -file")
	}

	if *mode == "upload" {

		err = d.PutObject("test-bucket", *filename, nil)

		if err != nil {
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
