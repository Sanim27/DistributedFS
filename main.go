package main

import (
	"log"

	"github.com/Sanim27/dfs/p2p"
)

func main() {

	tcpOpts := p2p.TCPtransportOpts{
		ListenAddr:    ":3000",
		HandshakeFunc: p2p.NOPHandshakefunc,
		Decoder:       p2p.DefaultDecoder{},
	}

	tr := p2p.NewTCPTransport(tcpOpts)
	if err := tr.ListenAndAccept(); err != nil {
		log.Fatal(err)
	}

	select {}
}
