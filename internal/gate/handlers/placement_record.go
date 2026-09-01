package handlers

import (
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/mulgadc/predastore/internal/config"
)

// The placement record's on-disk encoding.
//
//	off  size  field
//	0    1     magic, always zero
//	1    1     format version
//	2    1     k, the data shard count; m is len(nodes) - k
//	3    8     object size, uint64 big-endian
//	11   8     write epoch, uint64 big-endian
//	19   8     block size, uint64 big-endian
//	27   16    content MD5, all zero when the record carries none
//	43   var   digest marker, a uvarint: 0 means no digest, 1 means the 16
//	           bytes above are a plain content digest, and N > 1 means they
//	           are the composite of N-1 multipart parts
//	var  var   k+m node ids as uvarints, data shards first then parity
//
// gob spent 217 bytes on a 66-byte payload because it emits a type descriptor
// with every value. This spends 44 plus one byte per node id, in the common
// case of a record carrying no digest.
//
// The marker is a uvarint rather than a fixed field because a multipart part
// count runs to 10000 -- S3's own limit -- which does not fit one byte, and a
// fixed two-byte field would spend a byte on every record carrying no
// composite, which is nearly all of them.
//
// The version is 3 rather than 1 because versions 1 and 2 were written to real
// stores. Reusing their numbers would let a record from one of them decode as
// this format and read plausible nonsense; leaving the number alone makes it
// fail loudly instead.
const (
	placementMagic     = 0x00
	placementVersion   = 0x03
	placementFixedSize = 43
	maxDataShards      = 255
)

// errPlacementFormat rejects a record written before the cutover. A gob stream
// begins with a length byte and so can never begin with the magic, which is
// the whole reason the magic is zero: the old format fails loudly here instead
// of being handed to a decoder that would read plausible nonsense out of it.
var errPlacementFormat = errors.New(
	"placement record predates the fixed binary format; this store must be rebuilt")

// errPlacementLegacy rejects the two record versions that predate content
// ETags. They are not migrated: a store holding them is rebuilt, not upgraded.
var errPlacementLegacy = errors.New(
	"placement record predates content ETags; this store must be rebuilt")

// EncodePlacement renders a placement record. The object hash is not stored:
// the caller knows it — it is the key for whole objects, and derivable from
// the part name for multipart parts — so keeping a copy is pure duplication.
func EncodePlacement(p ObjectToShardNodes) ([]byte, error) {
	k := len(p.DataShardNodes)
	if k > maxDataShards {
		return nil, fmt.Errorf("data shard count %d exceeds %d", k, maxDataShards)
	}
	if p.Size < 0 {
		return nil, fmt.Errorf("negative object size %d", p.Size)
	}
	if p.BlockSize < 0 {
		return nil, fmt.Errorf("negative block size %d", p.BlockSize)
	}
	if len(p.Digest) != 0 && len(p.Digest) != md5.Size {
		return nil, fmt.Errorf("digest is %d bytes, want %d", len(p.Digest), md5.Size)
	}
	if p.PartCount < 0 {
		return nil, fmt.Errorf("negative part count %d", p.PartCount)
	}

	buf := make([]byte, placementFixedSize, placementFixedSize+binary.MaxVarintLen64+k+len(p.ParityShardNodes))
	buf[0] = placementMagic
	buf[1] = placementVersion
	buf[2] = byte(k)
	binary.BigEndian.PutUint64(buf[3:11], uint64(p.Size))
	binary.BigEndian.PutUint64(buf[11:19], p.WriteEpoch)
	binary.BigEndian.PutUint64(buf[19:27], uint64(p.BlockSize))

	// The marker is 0 for "no digest", which is what a multipart part's record
	// carries; 1 + PartCount otherwise, so 1 means a plain digest and anything
	// higher names how many parts composed it.
	var marker uint64
	if len(p.Digest) == md5.Size {
		copy(buf[27:43], p.Digest)
		marker = uint64(p.PartCount) + 1
	}
	buf = binary.AppendUvarint(buf, marker)

	for _, id := range p.DataShardNodes {
		buf = binary.AppendUvarint(buf, uint64(id))
	}
	for _, id := range p.ParityShardNodes {
		buf = binary.AppendUvarint(buf, uint64(id))
	}
	return buf, nil
}

// DecodePlacement parses a placement record, rejecting anything it cannot
// account for byte by byte rather than returning a partially populated record.
func DecodePlacement(b []byte) (ObjectToShardNodes, error) {
	if len(b) < placementFixedSize {
		return ObjectToShardNodes{}, fmt.Errorf("placement record is %d bytes, want at least %d",
			len(b), placementFixedSize)
	}
	if b[0] != placementMagic {
		return ObjectToShardNodes{}, errPlacementFormat
	}
	switch b[1] {
	case placementVersion:
	case 0x01, 0x02:
		return ObjectToShardNodes{}, errPlacementLegacy
	default:
		return ObjectToShardNodes{}, fmt.Errorf("unknown placement record version %d", b[1])
	}

	k := int(b[2])
	p := ObjectToShardNodes{
		Size:       int64(binary.BigEndian.Uint64(b[3:11])), //nolint:gosec // round-trips bit-for-bit from encode.
		WriteEpoch: binary.BigEndian.Uint64(b[11:19]),
		BlockSize:  int64(binary.BigEndian.Uint64(b[19:27])), //nolint:gosec // round-trips bit-for-bit from encode.
	}

	// The digest marker sits between the fixed header and the node ids, so the
	// node ids start after it rather than at the end of the fixed header.
	marker, n := binary.Uvarint(b[placementFixedSize:])
	if n <= 0 {
		return ObjectToShardNodes{}, errors.New("placement record has a malformed digest marker")
	}
	nodeIDStart := placementFixedSize + n
	if marker > 0 {
		p.Digest = append([]byte(nil), b[27:43]...)
		p.PartCount = int(marker - 1) //nolint:gosec // marker is 1+PartCount, bounded by S3's own part limit.
	}

	ids := make([]config.NodeID, 0, k)
	for rest := b[nodeIDStart:]; len(rest) > 0; {
		v, n := binary.Uvarint(rest)
		if n <= 0 {
			return ObjectToShardNodes{}, errors.New("placement record has a malformed node id")
		}
		ids = append(ids, config.NodeID(v))
		rest = rest[n:]
	}
	if len(ids) < k {
		return ObjectToShardNodes{}, fmt.Errorf(
			"placement record declares %d data shards but carries %d node ids", k, len(ids))
	}

	// Full slice expressions: appending to the data shards must not reach into
	// the parity shards sharing the array.
	p.DataShardNodes = ids[:k:k]
	p.ParityShardNodes = ids[k:len(ids):len(ids)]
	return p, nil
}

// ModifiedAt reports when the object was written. This is the object's S3
// LastModified on every surface that serves one.
func (p ObjectToShardNodes) ModifiedAt() time.Time {
	return EpochTime(p.WriteEpoch)
}

// ETag renders the S3 entity tag the record's content digest names, quoted,
// and reports whether it can. A multipart part's own record carries no digest
// -- the part's ETag lives in the parts table -- so it has none to give.
func (p ObjectToShardNodes) ETag() (string, bool) {
	if len(p.Digest) != md5.Size {
		return "", false
	}
	if p.PartCount > 0 {
		return fmt.Sprintf("\"%x-%d\"", p.Digest, p.PartCount), true
	}
	return fmt.Sprintf("\"%x\"", p.Digest), true
}
