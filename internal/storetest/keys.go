package storetest

import "bytes"

// TestMasterKey is a deterministic 32-byte AES-256 key for use in tests that
// open a store or QUIC server. Production code MUST load the key via
// store/crypto.LoadMasterKey instead.
var TestMasterKey = bytes.Repeat([]byte{0x42}, 32)
