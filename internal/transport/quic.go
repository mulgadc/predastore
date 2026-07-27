package transport

const NetworkQUIC Network = "quic"

type QUICAddr struct {
	host string
}

func newQUICAddr(host string) *QUICAddr {
	return &QUICAddr{host: host}
}

func (qa *QUICAddr) Network() string {
	return string(NetworkQUIC)
}

func (qa *QUICAddr) String() string {
	return qa.host
}

var _ Transport = (*QUICTransport)(nil)

type QUICTransport struct {
}
