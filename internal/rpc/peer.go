package rpc

import (
	"context"
	"log/slog"
	"net"
	"sync"

	"github.com/quic-go/quic-go"
)

type StateFunc func(ctx context.Context) (StateFunc, error)

type Peer struct {
	tr   *quic.Transport
	conn *quic.Conn
}

func (p *Peer) dial(ctx context.Context) (StateFunc, error) {
	conn, err := p.tr.Dial(context.TODO(), addr, &tls.Config{}, &quic.Config{})

	return p.serve, nil
}

func (p *Peer) serve(ctx context.Context) (StateFunc, error) {
	return p.drain, nil
}

func (p *Peer) drain(ctx context.Context) (StateFunc, error) {
	return p.close, nil
}

func (p *Peer) close(ctx context.Context) (StateFunc, error) {
	return nil, nil
}
