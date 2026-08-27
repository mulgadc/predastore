package handlers

import (
	"encoding/binary"
	"errors"
	"fmt"

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
//	19   var   k+m node ids as uvarints, data shards first then parity
//
// gob spent 217 bytes on a 66-byte payload because it emits a type descriptor
// with every value. This spends 19 plus one byte per node id.
const (
	placementMagic     = 0x00
	placementVersion   = 0x01
	placementFixedSize = 19
	maxDataShards      = 255
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

	buf := make([]byte, placementFixedSize, placementFixedSize+k+len(p.ParityShardNodes))
	buf[0] = placementMagic
	buf[1] = placementVersion
	buf[2] = byte(k)
	binary.BigEndian.PutUint64(buf[3:11], uint64(p.Size))
	binary.BigEndian.PutUint64(buf[11:19], p.WriteEpoch)

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
	if b[1] != placementVersion {
		return ObjectToShardNodes{}, fmt.Errorf("unknown placement record version %d", b[1])
	}

	k := int(b[2])
	p := ObjectToShardNodes{
		Size:       int64(binary.BigEndian.Uint64(b[3:11])), //nolint:gosec // round-trips bit-for-bit from encode.
		WriteEpoch: binary.BigEndian.Uint64(b[11:19]),
	}

	ids := make([]config.NodeID, 0, k)
	for rest := b[placementFixedSize:]; len(rest) > 0; {
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
