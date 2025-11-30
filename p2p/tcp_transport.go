package p2p

import (
	"fmt"
	"log"
	"net"
	// "net/rpc"
	// "sync"
)

// TCPPeer represents the remote node over a TCP established connection.
type TCPPeer struct {
	conn net.Conn

	// if we dial and retrieve a conn => outbound == true
	// if we accept and retrieve a conn => outbound == false
	outbound bool
}
// a constructor that returns a remote node after connection is established
func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer {
	return &TCPPeer{
		conn: conn,
		outbound: outbound,
	}
}


type TCPTranportOpts struct {
	ListenAddr 		string
	HandshakeFunc 	HandshakeFunc
	Decoder	 		Decoder
	OnPeer func(Peer) error
}

// tcp manager that handles connection
type TCPTransport struct {
	TCPTransportOpts TCPTranportOpts
	listener net.Listener
	rpcch chan RPC
	// mu sync.RWMutex
	// peers map[net.Addr]Peer

}
 
func (p *TCPPeer) Close() error {
	return p.conn.Close()
}

func NewTCPTransport(opts TCPTranportOpts) *TCPTransport {
	return &TCPTransport{
		TCPTransportOpts: opts,
		rpcch: make(chan RPC),
	}
}
// Consume implements the Transport interface, which will return read-only channel
// for reading the incoming messages received from another peer in the network.
func (t *TCPTransport) Consume() <-chan RPC {
	return t.rpcch
}


func (t *TCPTransport) ListenAndAccept() error {
	var err error 
	t.listener, err = net.Listen("tcp", t.TCPTransportOpts.ListenAddr)
	if err != nil {
		return  err
	}
	go t.startAcceptLoop()

	log.Printf("tcp transport listening on port: %s\n",t.TCPTransportOpts.ListenAddr)
	return  nil
}

func (t *TCPTransport) startAcceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			fmt.Printf("TCP accept error: %s\n", err)
		}
		fmt.Printf("new incoming connection %+v\n", conn)

		go t.handleConn(conn)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn) {
	var err error

	defer func() {
		fmt.Printf("Dropping connection because of this error : %s", err)
	}()

	peer := NewTCPPeer(conn, true)

	if err = t.TCPTransportOpts.HandshakeFunc(peer); err !=nil {
		return
	}

	if t.TCPTransportOpts.OnPeer != nil {
		if err = t.TCPTransportOpts.OnPeer(peer); err != nil {
			return
		}
	}

	//Read loop
	rpc := RPC{}
	for {
		if err = t.TCPTransportOpts.Decoder.Decode(conn,&rpc); err != nil {
			fmt.Printf("tcp error: %s\n", err)
			continue
		}

		rpc.From = conn.RemoteAddr()
		t.rpcch <- rpc
	}

}

