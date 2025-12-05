package p2p

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	// "net/rpc"
	// "sync"
)

// TCPPeer represents the remote node over a TCP established connection.
// It implements Peer interface because Peer requires net.Conn methods + Close(),
// and TCPPeer has them all.

// When TCPPeer embeds net.Conn, it inherits all of net.Conn's methods
type TCPPeer struct {
	// The underlying connection of the peer, which in this case is 
	// is a tcp connection.
	net.Conn

	// if we dial and retrieve a conn => outbound == true
	// if we accept and retrieve a conn => outbound == false
	outbound bool

	wg *sync.WaitGroup
}
// a constructor that returns a remote node after connection is established
func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer {
	return &TCPPeer{
		Conn: conn,
		outbound: outbound,
		wg: &sync.WaitGroup{},
	}
}

func (p *TCPPeer) CloseStream() {
	p.wg.Done()
}

func (p *TCPPeer) Send(b []byte) error {
	_, err := p.Conn.Write(b)
	return err
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


func NewTCPTransport(opts TCPTranportOpts) *TCPTransport {
	return &TCPTransport{
		TCPTransportOpts: opts,
		rpcch: make(chan RPC),
	}
}
// Addr implements the transport interface return the address
// the transport is accepting connections 
func (t *TCPTransport) Addr() string {
	return t.TCPTransportOpts.ListenAddr
}

// Consume implements the Transport interface, which will return read-only channel
// for reading the incoming messages received from another peer in the network.
func (t *TCPTransport) Consume() <-chan RPC {
	return t.rpcch
}

func (t *TCPTransport) Close() error {
	return t.listener.Close()
}

// dial implements the transport interface
func (t *TCPTransport) Dial(addr string) error {
	conn, err := net.Dial("tcp", addr) // if connection is established, t.listener.Accept() unblocks
	if err !=nil {
		return err
	}
	go t.handleConn(conn, true)

	return nil

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
		conn, err := t.listener.Accept() // this blocks until listener is closed or some hardware failure occurs or input connection is received.
		if errors.Is(err, net.ErrClosed) { 
			return   
		}
		if err != nil {
			fmt.Printf("TCP accept error: %s\n", err)
		}
		// fmt.Printf("new incoming connection %+v\n", conn)

		go t.handleConn(conn, false)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn, outbound bool) {
	var err error

	defer func() {
		fmt.Printf("Dropping connection because of this error : %s", err)
	}()
	// s1 becomes a peer if s2.handleConn() is called or vice versa.
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
	for {
		rpc := RPC{}
		err = t.TCPTransportOpts.Decoder.Decode(conn,&rpc) // Decode is blocking function
		if err != nil {
			return
		}
		rpc.From = conn.RemoteAddr().String()  

		if rpc.Stream {
			peer.wg.Add(1)
			fmt.Printf("[%s] incoming stream, waiting...\n",conn.RemoteAddr())
			peer.wg.Wait()
			fmt.Printf("[%s] stream closed, resuming read loop\n", conn.RemoteAddr())
			continue
		}

		t.rpcch <- rpc
	}

}

