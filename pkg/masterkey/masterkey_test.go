package masterkey_test

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/mulgadc/predastore/pkg/masterkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mustRandKey returns a fresh random 32-byte master key.
func mustRandKey(t *testing.T) []byte {
	t.Helper()
	raw := make([]byte, masterkey.MasterKeySize)
	_, err := rand.Read(raw)
	require.NoError(t, err)
	return raw
}

// writeKeyFile writes contents to a fresh path under t.TempDir() and explicitly
// chmods to mode so the test result is independent of the process umask.
func writeKeyFile(t *testing.T, name string, contents []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.WriteFile(path, contents, mode))
	require.NoError(t, os.Chmod(path, mode))
	return path
}

func TestLoad_HappyPath(t *testing.T) {
	raw := make([]byte, masterkey.MasterKeySize)
	_, err := rand.Read(raw)
	require.NoError(t, err)

	path := writeKeyFile(t, "key", raw, 0o600)

	k, err := masterkey.Load(path)
	require.NoError(t, err)
	require.NotNil(t, k)
	assert.NotNil(t, k.AEAD, "Load must construct a usable AEAD")
	assert.Equal(t, masterkey.Fingerprint(raw), k.Fingerprint,
		"Fingerprint on the returned Key must match Fingerprint(rawBytes)")
}

func TestLoad_TightPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission semantics not enforced on Windows")
	}

	raw := make([]byte, masterkey.MasterKeySize)

	for _, mode := range []os.FileMode{0o600, 0o400, 0o700} {
		t.Run(fmt.Sprintf("mode_%#o", mode), func(t *testing.T) {
			path := writeKeyFile(t, "key", raw, mode)
			_, err := masterkey.Load(path)
			assert.NoError(t, err, "mode %#o has no group/other bits set; should be accepted", mode)
		})
	}
}

func TestLoad_LoosePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission semantics not enforced on Windows")
	}

	raw := make([]byte, masterkey.MasterKeySize)

	// Each of these has at least one bit set in the 0o077 (group/other) mask
	// and MUST be rejected outright — the loader is fail-closed with no
	// override flag.
	for _, mode := range []os.FileMode{0o644, 0o640, 0o604, 0o601, 0o610, 0o660, 0o666, 0o777} {
		t.Run(fmt.Sprintf("mode_%#o", mode), func(t *testing.T) {
			path := writeKeyFile(t, "key", raw, mode)
			_, err := masterkey.Load(path)
			require.Error(t, err, "mode %#o exposes group/other access; loader must reject", mode)
			assert.Contains(t, err.Error(), "permissions",
				"error should explain the perm failure to the operator")
		})
	}
}

func TestLoadShared_AcceptsGroupRead(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission semantics not enforced on Windows")
	}

	raw := make([]byte, masterkey.MasterKeySize)
	_, err := rand.Read(raw)
	require.NoError(t, err)

	// LoadShared exists for cluster-shared keys (e.g. the IAM master key at
	// root:spinifex 0640). Owner-only modes must keep working too.
	for _, mode := range []os.FileMode{0o600, 0o640, 0o400, 0o440} {
		t.Run(fmt.Sprintf("mode_%#o", mode), func(t *testing.T) {
			path := writeKeyFile(t, "key", raw, mode)
			k, err := masterkey.LoadShared(path)
			require.NoError(t, err, "mode %#o has no other-access bits; should be accepted", mode)
			require.NotNil(t, k)
			assert.NotNil(t, k.AEAD)
			assert.Equal(t, masterkey.Fingerprint(raw), k.Fingerprint)
		})
	}
}

func TestLoadShared_RejectsOtherAccess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission semantics not enforced on Windows")
	}

	raw := make([]byte, masterkey.MasterKeySize)

	// LoadShared still rejects any "other" access bit — group sharing is
	// fine, world-readable is not.
	for _, mode := range []os.FileMode{0o644, 0o604, 0o601, 0o606, 0o646, 0o666, 0o777} {
		t.Run(fmt.Sprintf("mode_%#o", mode), func(t *testing.T) {
			path := writeKeyFile(t, "key", raw, mode)
			_, err := masterkey.LoadShared(path)
			require.Error(t, err, "mode %#o exposes other access; LoadShared must reject", mode)
			assert.Contains(t, err.Error(), "permissions",
				"error should explain the perm failure to the operator")
		})
	}
}

func TestLoad_WrongLength(t *testing.T) {
	for _, n := range []int{0, 1, 16, 24, 31, 33, 64, 128} {
		t.Run(fmt.Sprintf("len_%d", n), func(t *testing.T) {
			path := writeKeyFile(t, "key", make([]byte, n), 0o600)
			_, err := masterkey.Load(path)
			require.Error(t, err, "length %d must be rejected", n)
			assert.Contains(t, err.Error(), fmt.Sprintf("%d bytes", masterkey.MasterKeySize),
				"error should cite the required size")
		})
	}
}

func TestLoad_MissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist")
	_, err := masterkey.Load(path)
	require.Error(t, err)
	assert.True(t, os.IsNotExist(err) || strings.Contains(err.Error(), "no such file"),
		"error should signal the missing path: %v", err)
}

func TestFingerprint_Stable(t *testing.T) {
	raw := make([]byte, masterkey.MasterKeySize)
	_, err := rand.Read(raw)
	require.NoError(t, err)

	fp1 := masterkey.Fingerprint(raw)
	fp2 := masterkey.Fingerprint(raw)
	assert.Equal(t, fp1, fp2, "fingerprint must be deterministic for the same key")

	// 8 bytes of sha256 → 16 hex chars; anything else is a contract change.
	assert.Len(t, fp1, 16)
	_, err = hex.DecodeString(fp1)
	assert.NoError(t, err, "fingerprint must be valid hex")
}

func TestFingerprint_Distinct(t *testing.T) {
	seen := make(map[string][]byte)
	for range 16 {
		raw := make([]byte, masterkey.MasterKeySize)
		_, err := rand.Read(raw)
		require.NoError(t, err)

		fp := masterkey.Fingerprint(raw)
		if prev, ok := seen[fp]; ok {
			t.Fatalf("collision: fingerprint %s for distinct keys %x and %x", fp, prev, raw)
		}
		seen[fp] = raw
	}
}

func TestNew_FromBytes(t *testing.T) {
	raw := mustRandKey(t)
	k, err := masterkey.New(raw)
	require.NoError(t, err)
	require.NotNil(t, k)
	assert.NotNil(t, k.AEAD, "New must construct a usable AEAD")
	assert.Equal(t, masterkey.Fingerprint(raw), k.Fingerprint,
		"Fingerprint on the returned Key must match Fingerprint(rawBytes)")
}

func TestNew_WrongLength(t *testing.T) {
	for _, n := range []int{0, 1, 16, 24, 31, 33, 64} {
		t.Run(fmt.Sprintf("len_%d", n), func(t *testing.T) {
			_, err := masterkey.New(make([]byte, n))
			require.Error(t, err, "length %d must be rejected", n)
			assert.Contains(t, err.Error(), fmt.Sprintf("%d bytes", masterkey.MasterKeySize))
		})
	}
}

func TestEncryptDecrypt_RoundTrip(t *testing.T) {
	k, err := masterkey.New(mustRandKey(t))
	require.NoError(t, err)

	secret := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	ct, err := k.Encrypt(secret)
	require.NoError(t, err)
	assert.NotEqual(t, secret, ct, "ciphertext must differ from plaintext")

	pt, err := k.Decrypt(ct)
	require.NoError(t, err)
	assert.Equal(t, secret, pt)
}

func TestEncrypt_FreshNoncePerCall(t *testing.T) {
	k, err := masterkey.New(mustRandKey(t))
	require.NoError(t, err)

	ct1, err := k.Encrypt("same-plaintext")
	require.NoError(t, err)
	ct2, err := k.Encrypt("same-plaintext")
	require.NoError(t, err)
	assert.NotEqual(t, ct1, ct2,
		"encrypting the same plaintext twice must produce different ciphertexts (random nonce)")
}

func TestDecrypt_WrongKeyFails(t *testing.T) {
	k1, err := masterkey.New(mustRandKey(t))
	require.NoError(t, err)
	k2, err := masterkey.New(mustRandKey(t))
	require.NoError(t, err)

	ct, err := k1.Encrypt("secret")
	require.NoError(t, err)
	_, err = k2.Decrypt(ct)
	assert.Error(t, err, "decrypting with the wrong key must fail")
}

func TestDecrypt_TamperedFails(t *testing.T) {
	k, err := masterkey.New(mustRandKey(t))
	require.NoError(t, err)

	ct, err := k.Encrypt("secret")
	require.NoError(t, err)
	b := []byte(ct)
	if b[len(b)-2] == 'A' {
		b[len(b)-2] = 'B'
	} else {
		b[len(b)-2] = 'A'
	}
	_, err = k.Decrypt(string(b))
	assert.Error(t, err, "tampered ciphertext must fail the GCM auth tag check")
}

func TestDecrypt_InvalidBase64(t *testing.T) {
	k, err := masterkey.New(mustRandKey(t))
	require.NoError(t, err)
	_, err = k.Decrypt("not-valid-base64!!!")
	assert.Error(t, err)
}

func TestDecrypt_TooShort(t *testing.T) {
	k, err := masterkey.New(mustRandKey(t))
	require.NoError(t, err)
	_, err = k.Decrypt(base64.StdEncoding.EncodeToString([]byte{0x01, 0x02}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
}

// TestDecrypt_SpinifexWireFormat proves Key.Decrypt reads a ciphertext produced
// by the exact byte layout spinifex handlers_iam.EncryptSecret writes:
// base64(nonce + ciphertext + tag), 12-byte nonce, nil AAD. This is the
// cross-project wire-format parity guarantee the consolidation must preserve.
func TestDecrypt_SpinifexWireFormat(t *testing.T) {
	raw := mustRandKey(t)
	k, err := masterkey.New(raw)
	require.NoError(t, err)

	block, err := aes.NewCipher(raw)
	require.NoError(t, err)
	gcm, err := cipher.NewGCM(block)
	require.NoError(t, err)
	nonce := make([]byte, gcm.NonceSize())
	_, err = rand.Read(nonce)
	require.NoError(t, err)

	plaintext := "cross-project-secret-value"
	wire := base64.StdEncoding.EncodeToString(gcm.Seal(nonce, nonce, []byte(plaintext), nil))

	got, err := k.Decrypt(wire)
	require.NoError(t, err)
	assert.Equal(t, plaintext, got, "Key.Decrypt must read the spinifex wire format verbatim")
}
