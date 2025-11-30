package main

import (
	// "fmt"
	"log"
	"github.com/Sanim27/DistributedFS/p2p"
)

// func OnPeer(peer p2p.Peer) error {
// 	peer.Close()
// 	return nil
// }
func main() {
	tcptransportOpts := p2p.TCPTranportOpts{
		ListenAddr: ":3000",
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder: p2p.DefaultDecoder{},
		// TODO: onPeer func
	}
	tcpTransport := p2p.NewTCPTransport(tcptransportOpts)


	fileServerOpts := FileServerOpts{
		StorageRoot: "3000_network",
		PathTransformFunc: CASPathTranformFunc,
		Transport: tcpTransport,
	}
	s := NewFileServer(fileServerOpts)

	if err := s.Start(); err != nil {
		log.Fatal(err)
	}
	select{}
}