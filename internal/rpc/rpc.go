package rpc

import "errors"

const maxHeaderSize = 1024 * 1024

var ErrHeaderTooLarge = errors.New("header too large")

type Opcode uint32

type Header interface {
	Append(buf []byte) ([]byte, error)
	Unmarshal([]byte) error
}
