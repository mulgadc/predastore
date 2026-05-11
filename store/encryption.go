package store

import (
	"crypto/cipher"
	"encoding/binary"
)

// AAD layout (52 bytes, big-endian):
//
//	[0:32]   objectHash (SHA-256, already part of the shard key)
//	[32:36]  shardIndex
//	[36:44]  shardNum   (from the on-disk fragment header at read time)
//	[44:52]  fragNum    (from the on-disk fragment header at read time)
//
// storeID is deliberately omitted: putting it in the nonce already defeats
// cross-data-dir substitution. Adding it here would be redundant.
const aadSize = 52

// makeNonce builds the 12-byte GCM nonce: BE(fragNum) || BE(storeID).
// fragNum is monotonic per data dir (crash-safe via state.json high-water);
// storeID is 4 random bytes generated on first Store.Open. Together they are
// unique for the lifetime of the master key.
func makeNonce(fragNum uint64, storeID uint32) [12]byte {
	var nonce [12]byte
	binary.BigEndian.PutUint64(nonce[0:8], fragNum)
	binary.BigEndian.PutUint32(nonce[8:12], storeID)
	return nonce
}

// makeAAD binds each fragment to its logical position. A tampered fragment
// header or a fragment spliced into a different shard / object / position
// produces a different AAD at read time, and GCM Open fails. Returned by
// value so it stays on the stack on the per-fragment hot path.
func makeAAD(objectHash [32]byte, shardIndex uint32, shardNum, fragNum uint64) [aadSize]byte {
	var aad [aadSize]byte
	copy(aad[0:32], objectHash[:])
	binary.BigEndian.PutUint32(aad[32:36], shardIndex)
	binary.BigEndian.PutUint64(aad[36:44], shardNum)
	binary.BigEndian.PutUint64(aad[44:52], fragNum)
	return aad
}

// sealFragment encrypts plaintext under aead and appends the 16-byte GCM tag.
// Callers pass plaintext as a slice whose capacity extends exactly to the tag
// slot in the writer buffer; the seal is therefore in-place and no allocation
// is required. The returned slice has length len(plaintext) + aead.Overhead().
func sealFragment(aead cipher.AEAD, plaintext, aad, nonce []byte) []byte {
	return aead.Seal(plaintext[:0], nonce, plaintext, aad)
}

// openFragment decrypts ciphertext (body + tag) under aead and verifies the
// GCM tag against aad. Callers pass ciphertext sized to body + tag; the open
// is in-place. On tag mismatch, the underlying cipher error is returned —
// the reader wraps it with ErrIntegrity for callers to inspect via errors.Is.
func openFragment(aead cipher.AEAD, ciphertext, aad, nonce []byte) ([]byte, error) {
	return aead.Open(ciphertext[:0], nonce, ciphertext, aad)
}
