package endpointproxy_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"net/netip"
	"strings"
	"testing"

	"github.com/mulgadc/predastore/endpointproxy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func validHeader() endpointproxy.Header {
	return endpointproxy.Header{
		Version:     endpointproxy.ContractVersion1,
		Source:      netip.MustParseAddrPort("10.20.30.40:49152"),
		Destination: netip.MustParseAddrPort("192.0.2.10:8443"),
		VpceID:      "vpce-0a1b2c3d4e5f60718",
		VpcID:       "vpc-0a1b2c3d4e5f60718",
		AccountID:   "111122223333",
	}
}

func TestRead_RoundTripsAndLeavesTheGuestBytes(t *testing.T) {
	raw, err := validHeader().Marshal()
	require.NoError(t, err)
	stream := bytes.NewReader(append(raw, "\x16\x03\x01guest-tls"...))

	got, err := endpointproxy.Read(stream)
	require.NoError(t, err)
	assert.Equal(t, validHeader(), got)

	rest, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, "\x16\x03\x01guest-tls", string(rest), "Read must consume exactly the header")
}

func TestRead_CarriesTheEndpointIDInAWSsOwnTLV(t *testing.T) {
	raw, err := validHeader().Marshal()
	require.NoError(t, err)
	want := append([]byte{endpointproxy.TypeAWS, 0x00, 23, endpointproxy.SubtypeVpceID}, "vpce-0a1b2c3d4e5f60718"...)
	assert.True(t, bytes.Contains(raw, want), "the vpce ID is PP2_TYPE_AWS / PP2_SUBTYPE_AWS_VPCE_ID")
}

// reframe rebuilds a header around body so a test can corrupt what Marshal
// would refuse to write.
func reframe(cmd, fam byte, body []byte) []byte {
	out := []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A, cmd, fam}
	out = binary.BigEndian.AppendUint16(out, uint16(len(body)))
	return append(out, body...)
}

func bodyOf(t *testing.T, h endpointproxy.Header) []byte {
	t.Helper()
	raw, err := h.Marshal()
	require.NoError(t, err)
	return raw[16:]
}

func tlv(typ byte, v ...byte) []byte {
	return append([]byte{typ, byte(len(v) >> 8), byte(len(v))}, v...)
}

func join(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func TestRead_RefusesAnUnknownContractVersion(t *testing.T) {
	body := bodyOf(t, validHeader())
	at := bytes.Index(body, []byte{endpointproxy.TypeVersion, 0x00, 0x01, 0x01})
	require.GreaterOrEqual(t, at, 0)
	body[at+3] = 3

	_, err := endpointproxy.Read(bytes.NewReader(reframe(0x21, 0x11, body)))
	assert.ErrorIs(t, err, endpointproxy.ErrUnsupportedVersion)
}

func policyHeader() endpointproxy.Header {
	h := validHeader()
	h.Version = endpointproxy.ContractVersion2
	h.PolicyGeneration = 7
	h.Policy = `{"Statement":[{"Effect":"Allow","Principal":"*","Action":"*","Resource":"*"}]}`
	return h
}

// Version 2 carries the endpoint policy and its generation, so the gate needs
// no store of its own to evaluate it.
func TestRead_RoundTripsAVersion2Header(t *testing.T) {
	raw, err := policyHeader().Marshal()
	require.NoError(t, err)
	got, err := endpointproxy.Read(bytes.NewReader(raw))
	require.NoError(t, err)
	assert.Equal(t, policyHeader(), got)

	long := policyHeader()
	long.Policy = strings.Repeat("x", endpointproxy.MaxPolicyLength)
	raw, err = long.Marshal()
	require.NoError(t, err, "a policy at AWS's limit fits the header")
	got, err = endpointproxy.Read(bytes.NewReader(raw))
	require.NoError(t, err)
	assert.Len(t, got.Policy, endpointproxy.MaxPolicyLength)
}

// The policy TLVs belong to version 2 alone: a version 1 header carrying one,
// or a version 2 header missing one, is a contract the gate does not read.
func TestRead_RefusesAPolicyAtTheWrongVersion(t *testing.T) {
	v1 := bodyOf(t, validHeader())
	gen := tlv(endpointproxy.TypePolicyGen, 0, 0, 0, 0, 0, 0, 0, 7)
	v2 := bodyOf(t, policyHeader())
	at := bytes.Index(v2, []byte{endpointproxy.TypePolicy})
	require.Positive(t, at)
	short := []byte(nil)
	short = append(short, v2...)
	short = bytes.Replace(short, gen, tlv(endpointproxy.TypePolicyGen, 0, 7), 1)

	cases := []struct {
		name string
		body []byte
	}{
		{"version 1 with a policy", join(v1, gen, tlv(endpointproxy.TypePolicy, '{', '}'))},
		{"version 2 without its policy", v2[:at]},
		{"version 2 with a short generation", short},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := endpointproxy.Read(bytes.NewReader(reframe(0x21, 0x11, tc.body)))
			assert.Error(t, err)
		})
	}
}

func TestRead_IgnoresTLVsOutsideTheContract(t *testing.T) {
	body := join(bodyOf(t, validHeader()), tlv(0x04, 0, 0, 0)) // PP2_TYPE_NOOP
	got, err := endpointproxy.Read(bytes.NewReader(reframe(0x21, 0x11, body)))
	require.NoError(t, err)
	assert.Equal(t, validHeader(), got)
}

func TestRead_Refuses(t *testing.T) {
	good := bodyOf(t, validHeader())
	loopback := join(good)
	copy(loopback[0:4], []byte{127, 0, 0, 1})

	cases := []struct {
		name string
		raw  []byte
	}{
		{"not a PROXY header", []byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n")},
		{"LOCAL command", reframe(0x20, 0x11, good)},
		{"IPv6 family", reframe(0x21, 0x21, good)},
		{"truncated body", reframe(0x21, 0x11, good)[:20]},
		{"truncated TLV", reframe(0x21, 0x11, join(good, []byte{endpointproxy.TypeVpcID, 0x00}))},
		{"repeated TLV", reframe(0x21, 0x11, join(good, tlv(endpointproxy.TypeVpcID, []byte("vpc-0a1b2c3d4e5f60718")...)))},
		{"no contract version", reframe(0x21, 0x11, good[:12])},
		{"loopback guest source", reframe(0x21, 0x11, loopback)},
		{"AWS TLV of another subtype", reframe(0x21, 0x11, join(good[:12], tlv(endpointproxy.TypeVersion, 1), tlv(endpointproxy.TypeAWS, 0x02, 'x')))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := endpointproxy.Read(bytes.NewReader(tc.raw))
			assert.Error(t, err)
		})
	}
}

func TestMarshal_RefusesWhatReadWouldRefuse(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*endpointproxy.Header)
	}{
		{"endpoint service ID", func(h *endpointproxy.Header) { h.VpceID = "vpce-svc-0a1b2c3d" }},
		{"VPC ID", func(h *endpointproxy.Header) { h.VpcID = "subnet-0a1b2c3d" }},
		{"account ID", func(h *endpointproxy.Header) { h.AccountID = "1111" }},
		{"destination", func(h *endpointproxy.Header) { h.Destination = netip.AddrPort{} }},
		{"loopback source", func(h *endpointproxy.Header) { h.Source = netip.MustParseAddrPort("127.0.0.1:40000") }},
		{"version", func(h *endpointproxy.Header) { h.Version = 3 }},
		{"version 1 with a policy", func(h *endpointproxy.Header) { h.Policy = "{}" }},
		{"version 2 without a policy", func(h *endpointproxy.Header) { h.Version = 2; h.PolicyGeneration = 1 }},
		{"version 2 without a generation", func(h *endpointproxy.Header) { h.Version = 2; h.Policy = "{}" }},
		{"version 2 policy too long", func(h *endpointproxy.Header) {
			h.Version, h.PolicyGeneration, h.Policy = 2, 1, strings.Repeat("x", endpointproxy.MaxPolicyLength+1)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := validHeader()
			tc.mutate(&h)
			_, err := h.Marshal()
			assert.Error(t, err)
		})
	}
}
