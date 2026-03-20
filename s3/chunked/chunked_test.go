package chunked

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildChunkedBody creates a valid chunked-encoded body from payload chunks and optional trailers.
func buildChunkedBody(chunks []string, trailers map[string]string) string {
	var buf strings.Builder
	for _, chunk := range chunks {
		fmt.Fprintf(&buf, "%x\r\n%s\r\n", len(chunk), chunk)
	}
	buf.WriteString("0\r\n")
	for k, v := range trailers {
		fmt.Fprintf(&buf, "%s:%s\r\n", k, v)
	}
	buf.WriteString("\r\n")
	return buf.String()
}

func crc64Base64(crc uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], crc)
	return base64.StdEncoding.EncodeToString(b[:])
}

func TestParseHexInt64(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr string
	}{
		{"zero", "0", 0, ""},
		{"single digit", "5", 5, ""},
		{"lowercase hex", "ff", 255, ""},
		{"uppercase hex", "FF", 255, ""},
		{"mixed case", "aB", 0xAB, ""},
		{"multi-byte", "1A2B", 0x1A2B, ""},
		{"large value", "DEADBEEF", 0xDEADBEEF, ""},
		{"empty string", "", 0, "empty hex string"},
		{"invalid char", "1G", 0, "invalid hex digit"},
		{"space", "1 2", 0, "invalid hex digit"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseHexInt64(tt.input)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestVerifyCRC64NVME(t *testing.T) {
	t.Run("valid checksum matches", func(t *testing.T) {
		var crc uint64 = 0x123456789ABCDEF0
		encoded := crc64Base64(crc)
		assert.NoError(t, VerifyCRC64NVME(encoded, crc))
	})

	t.Run("checksum mismatch", func(t *testing.T) {
		var crc uint64 = 0x123456789ABCDEF0
		encoded := crc64Base64(crc)
		err := VerifyCRC64NVME(encoded, 0x0000000000000001)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "CRC64NVME mismatch")
	})

	t.Run("invalid base64", func(t *testing.T) {
		err := VerifyCRC64NVME("not-valid-base64!!!", 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid base64 CRC64NVME")
	})

	t.Run("wrong length", func(t *testing.T) {
		short := base64.StdEncoding.EncodeToString([]byte{1, 2, 3, 4})
		err := VerifyCRC64NVME(short, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid CRC64NVME length: got 4, want 8")
	})

	t.Run("zero checksum", func(t *testing.T) {
		encoded := crc64Base64(0)
		assert.NoError(t, VerifyCRC64NVME(encoded, 0))
	})

	t.Run("whitespace trimmed", func(t *testing.T) {
		var crc uint64 = 0xAABBCCDDEEFF0011
		encoded := "  " + crc64Base64(crc) + "  "
		assert.NoError(t, VerifyCRC64NVME(encoded, crc))
	})
}

func TestDecoder_SingleChunk(t *testing.T) {
	body := buildChunkedBody([]string{"hello"}, nil)
	dec := NewDecoder(strings.NewReader(body), 5)

	data, err := io.ReadAll(dec)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(data))
}

func TestDecoder_MultipleChunks(t *testing.T) {
	body := buildChunkedBody([]string{"abc", "defg", "hi"}, nil)
	dec := NewDecoder(strings.NewReader(body), 9)

	data, err := io.ReadAll(dec)
	require.NoError(t, err)
	assert.Equal(t, "abcdefghi", string(data))
}

func TestDecoder_EmptyPayload(t *testing.T) {
	body := "0\r\n\r\n"
	dec := NewDecoder(strings.NewReader(body), 0)

	data, err := io.ReadAll(dec)
	require.NoError(t, err)
	assert.Empty(t, data)
}

func TestDecoder_SmallReadBuffer(t *testing.T) {
	body := buildChunkedBody([]string{"hello world"}, nil)
	dec := NewDecoder(strings.NewReader(body), 11)

	var result []byte
	buf := make([]byte, 3)
	for {
		n, err := dec.Read(buf)
		result = append(result, buf[:n]...)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
	assert.Equal(t, "hello world", string(result))
}

func TestDecoder_ChunkExtensions(t *testing.T) {
	body := "5;chunk-signature=abc123\r\nhello\r\n0\r\n\r\n"
	dec := NewDecoder(strings.NewReader(body), 5)

	data, err := io.ReadAll(dec)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(data))
}

func TestDecoder_WithTrailers(t *testing.T) {
	trailers := map[string]string{
		"x-amz-checksum-crc64nvme": "dGVzdHZhbHVl",
	}
	body := buildChunkedBody([]string{"data"}, trailers)
	dec := NewDecoder(strings.NewReader(body), 4)

	_, err := io.ReadAll(dec)
	require.NoError(t, err)

	val, ok := dec.TrailerChecksum()
	assert.True(t, ok)
	assert.Equal(t, "dGVzdHZhbHVl", val)
}

func TestDecoder_TrailerChecksum_CaseInsensitive(t *testing.T) {
	// buildChunkedBody lowercases keys in the map iteration, so construct manually
	body := "0\r\nX-Amz-Checksum-Crc64nvme:somevalue\r\n\r\n"
	dec := NewDecoder(strings.NewReader(body), 0)

	_, err := io.ReadAll(dec)
	require.NoError(t, err)

	val, ok := dec.TrailerChecksum()
	assert.True(t, ok)
	assert.Equal(t, "somevalue", val)
}

func TestDecoder_TrailerChecksum_Missing(t *testing.T) {
	body := buildChunkedBody([]string{"data"}, nil)
	dec := NewDecoder(strings.NewReader(body), 4)

	_, err := io.ReadAll(dec)
	require.NoError(t, err)

	_, ok := dec.TrailerChecksum()
	assert.False(t, ok)
}

func TestDecoder_VerifyTrailerChecksum_Missing(t *testing.T) {
	body := buildChunkedBody([]string{"data"}, nil)
	dec := NewDecoder(strings.NewReader(body), 4)

	_, err := io.ReadAll(dec)
	require.NoError(t, err)

	err = dec.VerifyTrailerChecksum()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing x-amz-checksum-crc64nvme trailer")
}

func TestDecoder_VerifyTrailerChecksum_Valid(t *testing.T) {
	payload := "test payload data"

	// First pass: compute the CRC64
	body1 := buildChunkedBody([]string{payload}, nil)
	dec1 := NewDecoder(strings.NewReader(body1), int64(len(payload)))
	_, err := io.ReadAll(dec1)
	require.NoError(t, err)
	checksum := crc64Base64(dec1.CRC64())

	// Second pass: with the correct trailer
	trailers := map[string]string{
		"x-amz-checksum-crc64nvme": checksum,
	}
	body2 := buildChunkedBody([]string{payload}, trailers)
	dec2 := NewDecoder(strings.NewReader(body2), int64(len(payload)))
	_, err = io.ReadAll(dec2)
	require.NoError(t, err)

	assert.NoError(t, dec2.VerifyTrailerChecksum())
}

func TestDecoder_VerifyTrailerChecksum_Mismatch(t *testing.T) {
	trailers := map[string]string{
		"x-amz-checksum-crc64nvme": crc64Base64(0xDEADBEEF),
	}
	body := buildChunkedBody([]string{"some data"}, trailers)
	dec := NewDecoder(strings.NewReader(body), 9)

	_, err := io.ReadAll(dec)
	require.NoError(t, err)

	err = dec.VerifyTrailerChecksum()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CRC64NVME mismatch")
}

func TestDecoder_CRC64_Deterministic(t *testing.T) {
	payload := "deterministic test"
	body := buildChunkedBody([]string{payload}, nil)

	dec1 := NewDecoder(strings.NewReader(body), int64(len(payload)))
	_, _ = io.ReadAll(dec1)

	dec2 := NewDecoder(strings.NewReader(body), int64(len(payload)))
	_, _ = io.ReadAll(dec2)

	assert.Equal(t, dec1.CRC64(), dec2.CRC64())
	assert.NotZero(t, dec1.CRC64())
}

func TestDecoder_CRC64_MultiChunkMatchesSingle(t *testing.T) {
	payload := "abcdefghij"

	body1 := buildChunkedBody([]string{payload}, nil)
	dec1 := NewDecoder(strings.NewReader(body1), int64(len(payload)))
	_, _ = io.ReadAll(dec1)

	body2 := buildChunkedBody([]string{"abcde", "fghij"}, nil)
	dec2 := NewDecoder(strings.NewReader(body2), int64(len(payload)))
	_, _ = io.ReadAll(dec2)

	assert.Equal(t, dec1.CRC64(), dec2.CRC64())
}

func TestDecoder_LargePayload(t *testing.T) {
	chunk := strings.Repeat("X", 16384)
	body := buildChunkedBody([]string{chunk, chunk, chunk, chunk}, nil)
	dec := NewDecoder(strings.NewReader(body), 65536)

	data, err := io.ReadAll(dec)
	require.NoError(t, err)
	assert.Len(t, data, 65536)
}

func TestDecoder_ReadAfterDone(t *testing.T) {
	body := buildChunkedBody([]string{"hi"}, nil)
	dec := NewDecoder(strings.NewReader(body), 2)

	_, err := io.ReadAll(dec)
	require.NoError(t, err)

	buf := make([]byte, 10)
	n, err := dec.Read(buf)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
}

func TestDecoder_MultipleTrailers(t *testing.T) {
	trailers := map[string]string{
		"x-amz-checksum-crc64nvme": "checkval",
		"x-amz-request-id":         "reqid123",
	}
	body := buildChunkedBody([]string{"data"}, trailers)
	dec := NewDecoder(strings.NewReader(body), 4)

	_, err := io.ReadAll(dec)
	require.NoError(t, err)

	val, ok := dec.TrailerChecksum()
	assert.True(t, ok)
	assert.Equal(t, "checkval", val)
}

func TestDecoder_MalformedTrailerIgnored(t *testing.T) {
	body := "0\r\nmalformed-no-colon\r\nx-amz-checksum-crc64nvme:valid\r\n\r\n"
	dec := NewDecoder(strings.NewReader(body), 0)

	_, err := io.ReadAll(dec)
	require.NoError(t, err)

	val, ok := dec.TrailerChecksum()
	assert.True(t, ok)
	assert.Equal(t, "valid", val)
}

func TestDecoder_EmptyChunkHeader(t *testing.T) {
	body := "\r\nhello\r\n0\r\n\r\n"
	dec := NewDecoder(strings.NewReader(body), 0)

	_, err := io.ReadAll(dec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty chunk header")
}

func TestDecoder_InvalidChunkSize(t *testing.T) {
	body := "xyz\r\nhello\r\n0\r\n\r\n"
	dec := NewDecoder(strings.NewReader(body), 0)

	_, err := io.ReadAll(dec)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid chunk size")
}

func TestDecoder_HexChunkSizes(t *testing.T) {
	body := "a\r\n0123456789\r\n0\r\n\r\n"
	dec := NewDecoder(strings.NewReader(body), 10)

	data, err := io.ReadAll(dec)
	require.NoError(t, err)
	assert.Equal(t, "0123456789", string(data))
}

func TestDecoder_NoDecodedLenHint(t *testing.T) {
	body := buildChunkedBody([]string{"hello"}, nil)
	dec := NewDecoder(strings.NewReader(body), 0)

	data, err := io.ReadAll(dec)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(data))
}

func TestNewHTTPBodyReader(t *testing.T) {
	body := strings.NewReader("test body")
	req := httptest.NewRequest(http.MethodPut, "/bucket/key", body)

	reader := NewHTTPBodyReader(req)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "test body", string(data))
}

func TestDecoder_BinaryPayload(t *testing.T) {
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}

	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%x\r\n", len(payload))
	buf.Write(payload)
	buf.WriteString("\r\n0\r\n\r\n")

	dec := NewDecoder(&buf, int64(len(payload)))
	data, err := io.ReadAll(dec)
	require.NoError(t, err)
	assert.Equal(t, payload, data)
}
