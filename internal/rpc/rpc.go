// Package rpc carries predastore's intra-cluster requests: a framed header per
// stream, a client and server keyed by opcode, and a pool holding one connection
// per peer node.
package rpc

import "errors"

const maxHeaderSize = 1024 * 1024

var ErrHeaderTooLarge = errors.New("header too large")

type Opcode uint32

type Header interface {
	Append(buf []byte) ([]byte, error)
	Unmarshal([]byte) error
}
