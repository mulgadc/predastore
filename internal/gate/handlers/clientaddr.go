package handlers

import (
	"net"
	"net/http"
	"strings"
)

// realIPHeader carries the connecting client's address, set by the nginx edge
// in front of the gate on every node.
const realIPHeader = "X-Real-IP"

// ClientAddr is the client address to log for a request. X-Real-IP is trusted
// only when the peer is loopback, where it can only be the node's own nginx;
// from anywhere else the header is the client's to forge.
func ClientAddr(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return r.RemoteAddr
	}
	if forwarded := net.ParseIP(strings.TrimSpace(r.Header.Get(realIPHeader))); forwarded != nil {
		return forwarded.String()
	}

	return r.RemoteAddr
}
