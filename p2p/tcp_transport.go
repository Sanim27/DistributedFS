package p2p

import (
	"fmt"
	"net"
)

// TCPPeer represents the remote node over a TCP established connection
type TCPPeer struct {
	// conn is the underlying connection of the peer
	conn net.Conn

	// if we dial and retrieve a conn -> outbound == true
	// if we accept and retrieve a conn -> outbound == false
	outbound bool
}

type TCPtransportOpts struct {
	ListenAddr    string
	HandshakeFunc HandshakeFunc
	Decoder       Decoder
	OnPeer        func(Peer) error
}

func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer {
	return &TCPPeer{
		conn:     conn,
		outbound: outbound,
	}
}

// close implements the Peer interface
func (p *TCPPeer) Close() error {
	return p.conn.Close()
}

type TCPtransport struct {
	TCPtransportOpts
	listener net.Listener
	rpcch    chan RPC

	// mu    sync.RWMutex
	// peers map[net.Addr]Peer
}

func NewTCPTransport(opts TCPtransportOpts) *TCPtransport {
	return &TCPtransport{
		TCPtransportOpts: opts,
		rpcch:            make(chan RPC),
	}
}

// Consume implements the transport interface, which will return a read only channel
// for reading the incoming messages received from
// another peer in the network
func (t *TCPtransport) Consume() <-chan RPC {
	return t.rpcch
}

func (t *TCPtransport) ListenAndAccept() error {
	var err error
	t.listener, err = net.Listen("tcp", t.ListenAddr)
	if err != nil {
		return err
	}

	go t.startAcceptLoop()
	return nil
}

func (t *TCPtransport) startAcceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			fmt.Printf("TCP accept error: %s\n", err)
		}

		fmt.Printf("new incoming connection %+v\n", conn)
		go t.handleConn(conn)
	}
}

func (t *TCPtransport) handleConn(conn net.Conn) {
	var err error
	defer func() {
		fmt.Printf("Dropping peer connection: %s", err)
		conn.Close()
	}()

	peer := NewTCPPeer(conn, false)

	if err = t.HandshakeFunc(peer); err != nil {
		// conn.Close()
		// fmt.Printf("TCP handshake error: %s\n", err)
		return
	}

	if t.OnPeer != nil {
		if err = t.OnPeer(peer); err != nil {
			return
		}
	}

	//Read loop
	rpc := RPC{}
	for {
		err = t.Decoder.Decode(conn, &rpc)
		// fmt.Println(reflect.TypeOf(err))
		// panic(err)
		// if err == &net.OpError {
		// 	return
		// }
		if err != nil {
			// fmt.Printf("TCP read error: %s\n", err)
			// continue
			return
		}

		rpc.From = conn.RemoteAddr()
		t.rpcch <- rpc
		//fmt.Printf("message: %+v\n", rpc)
	}
}
