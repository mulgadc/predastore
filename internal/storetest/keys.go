package storetest

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
)

// TestMasterKey is a deterministic 32-byte AES-256 key for use in tests that
// open a quicserver (or any other layer that takes raw key bytes). Production
// code MUST load the key via internal/keyfile.Load instead.
var TestMasterKey = bytes.Repeat([]byte{0x42}, 32)

// TestAEAD returns the cipher.AEAD derived from TestMasterKey. Use this in
// tests that call store.WithAEAD directly — the store no longer accepts raw
// key bytes.
func TestAEAD() cipher.AEAD {
	block, err := aes.NewCipher(TestMasterKey)
	if err != nil {
		panic(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	return aead
}
