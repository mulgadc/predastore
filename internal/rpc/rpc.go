package rpc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/mulgadc/predastore/internal/transport"
)

const maxHeaderSize = 1024 * 1024

var ErrHeaderTooLarge = errors.New("header too large")

type Opcode uint32

// Header is the payload of one length-prefixed frame.
//
// The slice passed to Unmarshal aliases the frame buffer and is valid only for
// that call; an implementation retaining part of it must copy first.
type Header interface {
	Append(buf []byte) ([]byte, error)
	Unmarshal([]byte) error
}

// Status is the outcome a response carries. Zero is success; anything else
// means the peer answered with a failure message in place of a header. Codes
// are allocated per service in non-overlapping ranges; rpc owns the low ones.
type Status uint16

const (
	StatusOK Status = 0
	// StatusInternal carries its detail in the message. A failure with protocol
	// meaning gets its own code instead.
	StatusInternal Status = 1
)

// Stream error codes abort a stream that broke before a response could be
// written. A reset discards buffered data, so an operation that ran and failed
// answers with a Status instead.
const (
	ErrCodeUnknownOpcode transport.StreamErrorCode = 1
	ErrCodeBadHeader     transport.StreamErrorCode = 2
	ErrCodeHandlerFailed transport.StreamErrorCode = 3
)

// ResponseError is a failure the peer reported in-band.
type ResponseError struct {
	Status  Status
	Message string
}

func (e *ResponseError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("peer reported status %d", e.Status)
	}
	return fmt.Sprintf("peer reported status %d: %s", e.Status, e.Message)
}

// rawHeader frames a payload that is already encoded.
type rawHeader []byte

func (h rawHeader) Append(buf []byte) ([]byte, error) { return append(buf, h...), nil }
func (h rawHeader) Unmarshal([]byte) error            { return errors.New("rawHeader does not decode") }

// appendFrame appends a request header frame to buf.
func appendFrame[H Header](buf []byte, h H) ([]byte, error) {
	// The length is only known once the header is encoded, so reserve its slot
	// and fill it in afterwards.
	off := len(buf)
	buf = append(buf, 0, 0, 0, 0)

	buf, err := h.Append(buf)
	if err != nil {
		return nil, fmt.Errorf("encode header: %w", err)
	}

	n := len(buf) - off - 4
	if n > maxHeaderSize {
		return nil, fmt.Errorf("%w: %v", ErrHeaderTooLarge, n)
	}
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(n)) //nolint:gosec // G115: bounded by maxHeaderSize above.
	return buf, nil
}

// appendResponseFrame appends a response header frame to buf, carrying status.
func appendResponseFrame[H Header](buf []byte, status Status, h H) ([]byte, error) {
	// Reserve the length and status slots ahead of the payload.
	off := len(buf)
	buf = append(buf, 0, 0, 0, 0, 0, 0)

	buf, err := h.Append(buf)
	if err != nil {
		return nil, fmt.Errorf("encode response header: %w", err)
	}

	// The length covers the status, so one read takes the whole frame.
	n := len(buf) - off - 4
	if n > maxHeaderSize {
		return nil, fmt.Errorf("%w: %v", ErrHeaderTooLarge, n)
	}
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(n)) //nolint:gosec // G115: bounded by maxHeaderSize above.
	binary.BigEndian.PutUint16(buf[off+4:off+6], uint16(status))
	return buf, nil
}

// readFrame reads one frame's payload, leaving r on the first byte after it.
// Reads are exact: a buffered reader here would overrun into the body.
func readFrame(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("read frame length: %w", err)
	}

	// Bound the allocation below by what a peer is allowed to declare.
	n := binary.BigEndian.Uint32(lenBuf[:])
	if n > maxHeaderSize {
		return nil, fmt.Errorf("%w: %v", ErrHeaderTooLarge, n)
	}

	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("read frame payload: %w", err)
	}
	return payload, nil
}

// WriteResponse answers with a success header. Any body follows it on the
// stream and ends when that write side closes.
func WriteResponse[H Header](w io.Writer, h H) error {
	buf, err := appendResponseFrame(nil, StatusOK, h)
	if err != nil {
		return err
	}
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("write response frame: %w", err)
	}
	return nil
}

// WriteError answers with a failure the operation reported. No body follows.
func WriteError(w io.Writer, status Status, msg string) error {
	if status == StatusOK {
		return errors.New("WriteError needs a non-zero status")
	}
	// The message takes the payload slot a header would have occupied.
	buf, err := appendResponseFrame(nil, status, rawHeader(msg))
	if err != nil {
		return err
	}
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("write error frame: %w", err)
	}
	return nil
}

// ReadResponse reads the peer's answer, returning a *ResponseError for any
// failure status so a header comes back only on success. Any body follows it on
// the stream and ends when the peer closes its write side.
func ReadResponse[T any, PT interface {
	*T
	Header
}](r io.Reader) (T, error) {
	var decoded T

	payload, err := readFrame(r)
	if err != nil {
		return decoded, err
	}
	if len(payload) < 2 {
		return decoded, fmt.Errorf("response frame is %d bytes, want at least 2", len(payload))
	}

	// On failure the rest of the payload is the message, not a header.
	if status := Status(binary.BigEndian.Uint16(payload[:2])); status != StatusOK {
		return decoded, &ResponseError{Status: status, Message: string(payload[2:])}
	}

	if err := PT(&decoded).Unmarshal(payload[2:]); err != nil {
		return decoded, fmt.Errorf("decode response header: %w", err)
	}
	return decoded, nil
}
