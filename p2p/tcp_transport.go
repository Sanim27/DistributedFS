package p2p

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
)

// TCPPeer represents the remote node over a TCP established connection
type TCPPeer struct {
	// The underlying connection of the peer, which in this case
	// is tcp connection
	net.Conn

	// if we dial and retrieve a conn -> outbound == true
	// if we accept and retrieve a conn -> outbound == false
	outbound bool

	Wg *sync.WaitGroup
}

type TCPtransportOpts struct {
	ListenAddr    string
	HandshakeFunc HandshakeFunc
	Decoder       Decoder
	OnPeer        func(Peer) error
}

func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer {
	return &TCPPeer{
		Conn:     conn,
		outbound: outbound,
		Wg:       &sync.WaitGroup{},
	}
}

func (p *TCPPeer) Send(b []byte) error {
	_, err := p.Conn.Write(b)
	return err
}

// RemoteAddr implements the peer interface
// will return the address of its underlying connection
// func (p *TCPPeer) RemoteAddr() net.Addr {
// 	return p.conn.RemoteAddr()
// }

// // Close implements the Peer interface
// func (p *TCPPeer) Close() error {
// 	return p.conn.Close()
// }

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

// Close implements the Transport Interface.
func (t *TCPtransport) Close() error {
	return t.listener.Close()
}

// Dial implements the Transport Interface.
func (t *TCPtransport) Dial(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	go t.handleConn(conn, true)
	return nil
}

func (t *TCPtransport) ListenAndAccept() error {
	var err error
	t.listener, err = net.Listen("tcp", t.ListenAddr)
	if err != nil {
		return err
	}

	go t.startAcceptLoop()

	log.Printf("TCP transport listening on port %s\n", t.ListenAddr)
	return nil
}

func (t *TCPtransport) startAcceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			fmt.Printf("TCP accept error: %s\n", err)
		}

		go t.handleConn(conn, false)
	}
}

func (t *TCPtransport) handleConn(conn net.Conn, outbound bool) {
	var err error
	defer func() {
		fmt.Printf("Dropping peer connection: %s", err)
		conn.Close()
	}()

	peer := NewTCPPeer(conn, outbound)

	if err = t.HandshakeFunc(peer); err != nil {
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
		if err != nil {
			return
		}

		rpc.From = conn.RemoteAddr().String()
		peer.Wg.Add(1)
		fmt.Println("waiting till stream is done")
		t.rpcch <- rpc
		peer.Wg.Wait()
		fmt.Println("stream done continuing normal read loop")
		//fmt.Printf("message: %+v\n", rpc)
	}
}
