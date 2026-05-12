package storetest

import (
	"bytes"
	"crypto/cipher"

	"github.com/mulgadc/predastore/internal/masterkey"
)

// TestMasterKey is a deterministic 32-byte AES-256 key for use in tests that
// need raw key bytes (e.g. property tests that exercise the loader). Production
// code MUST load the key via internal/masterkey.Load instead.
var TestMasterKey = bytes.Repeat([]byte{0x42}, 32)

// TestAEAD returns the cipher.AEAD derived from TestMasterKey. Use this in
// tests that call store.WithAEAD directly — the store no longer accepts raw
// key bytes.
func TestAEAD() cipher.AEAD {
	aead, err := masterkey.NewAEAD(TestMasterKey)
	if err != nil {
		panic(err)
	}
	return aead
}

// TestKey returns a *masterkey.Key derived from TestMasterKey, suitable for
// quicserver.WithMasterKey in tests.
func TestKey() *masterkey.Key {
	return &masterkey.Key{
		AEAD:        TestAEAD(),
		Fingerprint: masterkey.Fingerprint(TestMasterKey),
	}
}
