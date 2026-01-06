package main

import (
	"flag"

	"github.com/mulgadc/predastore/quic/quicserver"
)

func main() {

	storageRoot := flag.String("storage-root", "./data", "Storage root directory")
	addr := flag.String("addr", "0.0.0.0:7443", "Address to listen on")
	flag.Parse()

	quicserver.New(*storageRoot, *addr)

}
