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
//	11   8     write epoch, uint64 big-endian (version 2+ carries a timestamp)
//	19   8     block size, uint64 big-endian (version 2+ only)
//	27   16    content MD5 (version 3 only), all zero when the record carries none
//	43   var   digest marker, a uvarint (version 3 only): 0 means no digest, 1
//	           means the 16 bytes above are a plain content digest, and N > 1
//	           means they are the composite of N-1 multipart parts
//	var  var   k+m node ids as uvarints, data shards first then parity
//
// gob spent 217 bytes on a 66-byte payload because it emits a type descriptor
// with every value. This spends 44 plus one byte per node id, in the common
// case of a record carrying no digest.
//
// Version 1 has no block size and its objects are laid out with each shard
// contiguous, which is what the gate wrote before it could stream a write. Its
// write epoch is random rather than a timestamp, so objects written under it
// have no modification time to report.
//
// Version 3 adds the object's content MD5, so an ETag can be served without
// re-reading the body. The marker is a uvarint rather than a fixed field
// because a multipart part count runs to 10000 -- S3's own limit -- which does
// not fit one byte, and a fixed two-byte field would spend a byte on every
// object with no composite, which is nearly all of them, to cover a case only
// multipart completion ever hits.
const (
	placementMagic       = 0x00
	placementVersionV1   = 0x01
	placementVersionV2   = 0x02
	placementVersionV3   = 0x03
	placementVersion     = placementVersionV3
	placementFixedSizeV1 = 19
	placementFixedSizeV2 = 27
	placementFixedSizeV3 = 43
	maxDataShards        = 255
)

// errPlacementFormat rejects a record written before the cutover. A gob stream
// begins with a length byte and so can never begin with the magic, which is
// the whole reason the magic is zero: the old format fails loudly here instead
// of being handed to a decoder that would read plausible nonsense out of it.
var errPlacementFormat = errors.New(
	"placement record predates the fixed binary format; this store must be rebuilt")

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
	if p.DigestPresent && len(p.Digest) != md5.Size {
		return nil, fmt.Errorf("digest is %d bytes, want %d", len(p.Digest), md5.Size)
	}
	if p.PartCount < 0 {
		return nil, fmt.Errorf("negative part count %d", p.PartCount)
	}

	buf := make([]byte, placementFixedSizeV3, placementFixedSizeV3+binary.MaxVarintLen64+k+len(p.ParityShardNodes))
	buf[0] = placementMagic
	buf[1] = placementVersion
	buf[2] = byte(k)
	binary.BigEndian.PutUint64(buf[3:11], uint64(p.Size))
	binary.BigEndian.PutUint64(buf[11:19], p.WriteEpoch)
	binary.BigEndian.PutUint64(buf[19:27], uint64(p.BlockSize))

	// The marker is 0 for "no digest" so a v1/v2-style absent case round-trips
	// as zero bytes; 1 + PartCount otherwise, so 1 means a plain digest and
	// anything higher names how many parts composed it.
	var marker uint64
	if p.DigestPresent {
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
	if len(b) < placementFixedSizeV1 {
		return ObjectToShardNodes{}, fmt.Errorf("placement record is %d bytes, want at least %d",
			len(b), placementFixedSizeV1)
	}
	if b[0] != placementMagic {
		return ObjectToShardNodes{}, errPlacementFormat
	}

	// A version 1 record carries no block size, and zero is what the read path
	// reads as the contiguous layout those objects were written with.
	fixed := placementFixedSizeV2
	switch b[1] {
	case placementVersionV1:
		fixed = placementFixedSizeV1
	case placementVersionV2:
		if len(b) < placementFixedSizeV2 {
			return ObjectToShardNodes{}, fmt.Errorf("placement record is %d bytes, want at least %d",
				len(b), placementFixedSizeV2)
		}
	case placementVersionV3:
		if len(b) < placementFixedSizeV3 {
			return ObjectToShardNodes{}, fmt.Errorf("placement record is %d bytes, want at least %d",
				len(b), placementFixedSizeV3)
		}
		fixed = placementFixedSizeV3
	default:
		return ObjectToShardNodes{}, fmt.Errorf("unknown placement record version %d", b[1])
	}

	k := int(b[2])
	p := ObjectToShardNodes{
		Size:       int64(binary.BigEndian.Uint64(b[3:11])), //nolint:gosec // round-trips bit-for-bit from encode.
		WriteEpoch: binary.BigEndian.Uint64(b[11:19]),

		// A version 1 epoch is eight random bytes, so reading it as a time
		// gives a date hundreds of millions of years out. Version 2 and later
		// carry a real one.
		Timestamped: b[1] != placementVersionV1,
	}
	if fixed >= placementFixedSizeV2 {
		p.BlockSize = int64(binary.BigEndian.Uint64(b[19:27])) //nolint:gosec // round-trips bit-for-bit from encode.
	}

	// The digest marker sits between the fixed header and the node ids, so the
	// node ids start after it rather than at the end of the fixed header
	// itself for a version 3 record.
	nodeIDStart := fixed
	if b[1] == placementVersionV3 {
		marker, n := binary.Uvarint(b[placementFixedSizeV3:])
		if n <= 0 {
			return ObjectToShardNodes{}, errors.New("placement record has a malformed digest marker")
		}
		nodeIDStart = placementFixedSizeV3 + n
		if marker > 0 {
			p.Digest = append([]byte(nil), b[27:43]...)
			p.DigestPresent = true
			p.PartCount = int(marker - 1) //nolint:gosec // marker is 1+PartCount, bounded by S3's own part limit.
		}
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

// ModifiedAt reports when the object was written, and whether the record can
// say. This is the object's S3 LastModified on every surface that serves one.
func (p ObjectToShardNodes) ModifiedAt() (time.Time, bool) {
	if !p.Timestamped {
		return time.Time{}, false
	}

	return EpochTime(p.WriteEpoch), true
}

// ETag renders the S3 entity tag the record's content digest names, quoted,
// and reports whether it can. A record with no digest -- everything written
// before version 3 -- has none to give, the same way an untimestamped record
// has no ModifiedAt.
func (p ObjectToShardNodes) ETag() (string, bool) {
	if !p.DigestPresent {
		return "", false
	}
	if p.PartCount > 0 {
		return fmt.Sprintf("\"%x-%d\"", p.Digest, p.PartCount), true
	}
	return fmt.Sprintf("\"%x\"", p.Digest), true
}
