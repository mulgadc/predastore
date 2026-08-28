package rpc

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"time"
)

const DefaultReadOpcodeTimeout = 2 * time.Second

type Transport interface {
	net.Listener
	Dial(ctx context.Context, addr net.Addr) (net.Conn, error)
}

type Opcode uint16

func readOpcode(conn net.Conn) (Opcode, error) {
	buf := make([]byte, 2)

	if err := conn.SetReadDeadline(time.Now().Add(DefaultReadOpcodeTimeout)); err != nil {
		return Opcode(0), err
	}

	if _, err := io.ReadFull(conn, buf); err != nil {
		return Opcode(0), err
	}

	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return Opcode(0), err
	}

	return Opcode(binary.BigEndian.Uint16(buf)), nil
}
