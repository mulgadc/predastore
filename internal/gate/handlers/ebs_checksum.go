package handlers

import (
	"encoding/base64"
	"encoding/binary"
	"net/http"

	"github.com/mulgadc/predastore/internal/gate/model"
)

const (
	// ebsOptInHeader marks a PutObject from the EBS data path, which never reads
	// an ETag, so its body is checked with CRC64NVME and not hashed with MD5.
	ebsOptInHeader  = "X-Spinifex-Ebs"
	crc64nvmeHeader = "X-Amz-Checksum-Crc64nvme"
)

// ebsChecksum returns the CRC64NVME an opted-in EBS chunk PUT declares. Both
// headers are needed because AWS SDKs send CRC64NVME by default, and only a
// plain body qualifies; anything else keeps its MD5 ETag.
func ebsChecksum(r *http.Request, framed bool) (want uint64, ok bool, err error) {
	value := r.Header.Get(crc64nvmeHeader)
	if framed || value == "" || r.Header.Get(ebsOptInHeader) != "true" {
		return 0, false, nil
	}
	raw, err := base64.StdEncoding.DecodeString(value)
	if err != nil || len(raw) != 8 {
		return 0, false, model.NewS3Error(model.ErrInvalidDigest,
			"Value for x-amz-checksum-crc64nvme header is invalid.", http.StatusBadRequest)
	}
	return binary.BigEndian.Uint64(raw), true, nil
}

// ebsDigest fills the record's 16-byte digest for an EBS chunk, so its ETag is
// the CRC64NVME then the length, in hex: stable, and a function of the content.
func ebsDigest(crc uint64, size int64) []byte {
	d := make([]byte, 16)
	binary.BigEndian.PutUint64(d[:8], crc)
	binary.BigEndian.PutUint64(d[8:], uint64(size))
	return d
}
