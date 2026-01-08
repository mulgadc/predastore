package quicproto

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

const (
	Version1 uint8 = 1

	MethodGET     uint8 = 1
	MethodPUT     uint8 = 2
	MethodSTATUS  uint8 = 3
	MethodREBUILD uint8 = 4
	MethodDELETE  uint8 = 5

	StatusOK           uint16 = 200
	StatusBadRequest   uint16 = 400
	StatusNotFound     uint16 = 404
	StatusServerError  uint16 = 500
	StatusUnavailable  uint16 = 503
	StatusUnauthorized uint16 = 401
)

var (
	ErrBadVersion = errors.New("bad protocol version")
)

type Header struct {
	Version uint8
	Method  uint8
	Status  uint16 // request: 0, response: HTTP-like status
	Flags   uint16
	_       uint16 // padding for alignment / future use

	ReqID uint64

	KeyLen  uint32
	MetaLen uint32 // JSON bytes, optional
	BodyLen uint64 // optional, 0 means "stream until EOF" or "no body"
}

// Fixed header size: 1+1+2+2+2 +8+4+4+8 = 32 bytes
const headerSize = 32

func WriteHeader(w io.Writer, h Header) error {
	var b [headerSize]byte
	b[0] = h.Version
	b[1] = h.Method
	binary.BigEndian.PutUint16(b[2:4], h.Status)
	binary.BigEndian.PutUint16(b[4:6], h.Flags)
	binary.BigEndian.PutUint16(b[6:8], 0)

	binary.BigEndian.PutUint64(b[8:16], h.ReqID)
	binary.BigEndian.PutUint32(b[16:20], h.KeyLen)
	binary.BigEndian.PutUint32(b[20:24], h.MetaLen)
	binary.BigEndian.PutUint64(b[24:32], h.BodyLen)
	_, err := w.Write(b[:])
	return err
}

func ReadHeader(r io.Reader) (Header, error) {
	var b [headerSize]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return Header{}, err
	}
	h := Header{
		Version: b[0],
		Method:  b[1],
		Status:  binary.BigEndian.Uint16(b[2:4]),
		Flags:   binary.BigEndian.Uint16(b[4:6]),
		ReqID:   binary.BigEndian.Uint64(b[8:16]),
		KeyLen:  binary.BigEndian.Uint32(b[16:20]),
		MetaLen: binary.BigEndian.Uint32(b[20:24]),
		BodyLen: binary.BigEndian.Uint64(b[24:32]),
	}
	if h.Version != Version1 {
		return Header{}, ErrBadVersion
	}
	return h, nil
}

// Helpers that avoid big allocations on hot paths.
func ReadExactBytes(r *bufio.Reader, n uint32, limit uint32) ([]byte, error) {
	if n == 0 {
		return nil, nil
	}
	if n > limit {
		return nil, errors.New("field too large")
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}
