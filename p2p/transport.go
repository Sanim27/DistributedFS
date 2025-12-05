package p2p

import (
	"net"
)
// Peer is interface that represents the remote node
type Peer interface {
	net.Conn
	Close() error
	Send([]byte) error
	CloseStream() 
}

// Transport is anything that handles communication between
// nodes in the network. This can be of the form TCP, UDP, etc.
type Transport interface {
	Dial(string) error
	ListenAndAccept() error
	Consume() <-chan RPC
	Close() error
	Addr() string
	// ListenAddr() string
}
