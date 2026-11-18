// Package endpointproxy is the contract between a node's VPC endpoint proxy and
// the gate's endpoint listener: a PROXY protocol v2 header naming the guest, the
// address it dialled and the endpoint it came through, sent inside the
// node-to-node mTLS session ahead of the guest's own TLS bytes.
package endpointproxy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"regexp"
)

// ContractVersion1 is the only version this build reads or writes: the guest's
// addresses, the endpoint ID, its VPC and its owning account.
const ContractVersion1 = 1

// TLV types. TypeAWS with SubtypeVpceID is AWS's own encoding of the endpoint
// ID; the rest sit in the range PROXY v2 reserves for applications.
const (
	TypeAWS         = 0xEA
	SubtypeVpceID   = 0x01
	TypeVersion     = 0xE0
	TypeVpcID       = 0xE1
	TypeAccountID   = 0xE2
	maxHeaderLength = 32 << 10
)

var signature = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}

const (
	cmdProxy  = 0x21 // version 2, PROXY
	famTCPv4  = 0x11
	addrLenV4 = 12
)

var (
	vpceIDPattern    = regexp.MustCompile(`^vpce-[0-9a-f]{8,17}$`)
	vpcIDPattern     = regexp.MustCompile(`^vpc-[0-9a-f]{8,17}$`)
	accountIDPattern = regexp.MustCompile(`^[0-9]{12}$`)
)

// ErrUnsupportedVersion is a well-formed header at a contract version this
// build does not serve. The listener closes the connection on it.
var ErrUnsupportedVersion = errors.New("endpointproxy: unsupported contract version")

// Header is one proxied connection's identity.
type Header struct {
	Version     uint8
	Source      netip.AddrPort // the guest's private address and port
	Destination netip.AddrPort // the prefix-list entry the guest dialled
	VpceID      string
	VpcID       string
	AccountID   string // the endpoint's owning account
}

// Validate reports whether h is a complete version 1 header.
func (h Header) Validate() error {
	if h.Version != ContractVersion1 {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, h.Version)
	}
	for _, a := range []netip.AddrPort{h.Source, h.Destination} {
		if !a.IsValid() || !a.Addr().Is4() || a.Port() == 0 {
			return fmt.Errorf("endpointproxy: %v is not an IPv4 address and port", a)
		}
	}
	src := h.Source.Addr()
	if src.IsLoopback() || src.IsUnspecified() || src.IsMulticast() {
		return fmt.Errorf("endpointproxy: %v cannot be a guest address", src)
	}
	switch {
	case !vpceIDPattern.MatchString(h.VpceID):
		return fmt.Errorf("endpointproxy: malformed endpoint ID %q", h.VpceID)
	case !vpcIDPattern.MatchString(h.VpcID):
		return fmt.Errorf("endpointproxy: malformed VPC ID %q", h.VpcID)
	case !accountIDPattern.MatchString(h.AccountID):
		return errors.New("endpointproxy: malformed account ID")
	}
	return nil
}

// Marshal encodes h, refusing a header that would not validate.
func (h Header) Marshal() ([]byte, error) {
	if err := h.Validate(); err != nil {
		return nil, err
	}
	var body bytes.Buffer
	src, dst := h.Source.Addr().As4(), h.Destination.Addr().As4()
	body.Write(src[:])
	body.Write(dst[:])
	_ = binary.Write(&body, binary.BigEndian, h.Source.Port())
	_ = binary.Write(&body, binary.BigEndian, h.Destination.Port())
	writeTLV(&body, TypeVersion, []byte{h.Version})
	writeTLV(&body, TypeAWS, append([]byte{SubtypeVpceID}, h.VpceID...))
	writeTLV(&body, TypeVpcID, []byte(h.VpcID))
	writeTLV(&body, TypeAccountID, []byte(h.AccountID))

	out := make([]byte, 0, 16+body.Len())
	out = append(out, signature...)
	out = append(out, cmdProxy, famTCPv4)
	out = binary.BigEndian.AppendUint16(out, uint16(body.Len())) //nolint:gosec // G115: bounded by the four short TLVs above.
	return append(out, body.Bytes()...), nil
}

func writeTLV(b *bytes.Buffer, typ byte, v []byte) {
	b.WriteByte(typ)
	_ = binary.Write(b, binary.BigEndian, uint16(len(v))) //nolint:gosec // G115: every value is an identifier or one byte.
	b.Write(v)
}

// Read consumes exactly one header from r, leaving the guest's bytes after it
// unread, and returns it only if it validates.
func Read(r io.Reader) (Header, error) {
	var fixed [16]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return Header{}, fmt.Errorf("endpointproxy: read header: %w", err)
	}
	if !bytes.Equal(fixed[:12], signature) {
		return Header{}, errors.New("endpointproxy: not a PROXY v2 header")
	}
	if fixed[12] != cmdProxy || fixed[13] != famTCPv4 {
		return Header{}, fmt.Errorf("endpointproxy: command %#x family %#x is not PROXY over TCP/IPv4", fixed[12], fixed[13])
	}
	n := int(binary.BigEndian.Uint16(fixed[14:16]))
	if n < addrLenV4 || n > maxHeaderLength {
		return Header{}, fmt.Errorf("endpointproxy: header length %d out of range", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return Header{}, fmt.Errorf("endpointproxy: read header body: %w", err)
	}

	h := Header{
		Source:      netip.AddrPortFrom(netip.AddrFrom4([4]byte(body[0:4])), binary.BigEndian.Uint16(body[8:10])),
		Destination: netip.AddrPortFrom(netip.AddrFrom4([4]byte(body[4:8])), binary.BigEndian.Uint16(body[10:12])),
	}
	seen := map[byte]bool{}
	for tlvs := body[addrLenV4:]; len(tlvs) > 0; {
		if len(tlvs) < 3 {
			return Header{}, errors.New("endpointproxy: truncated TLV")
		}
		typ, l := tlvs[0], int(binary.BigEndian.Uint16(tlvs[1:3]))
		if len(tlvs) < 3+l {
			return Header{}, errors.New("endpointproxy: truncated TLV value")
		}
		v := tlvs[3 : 3+l]
		tlvs = tlvs[3+l:]
		if seen[typ] {
			return Header{}, fmt.Errorf("endpointproxy: TLV %#x repeated", typ)
		}
		seen[typ] = true
		switch typ {
		case TypeVersion:
			if l != 1 {
				return Header{}, errors.New("endpointproxy: version TLV is not one byte")
			}
			h.Version = v[0]
		case TypeAWS:
			if l < 1 || v[0] != SubtypeVpceID {
				return Header{}, errors.New("endpointproxy: AWS TLV does not carry an endpoint ID")
			}
			h.VpceID = string(v[1:])
		case TypeVpcID:
			h.VpcID = string(v)
		case TypeAccountID:
			h.AccountID = string(v)
		}
	}
	if !seen[TypeVersion] {
		return Header{}, errors.New("endpointproxy: no contract version")
	}
	if err := h.Validate(); err != nil {
		return Header{}, err
	}
	return h, nil
}
