package main

import (
	"bytes"
	"log"
	"time"

	"github.com/Sanim27/dfs/p2p"
)

func makeServer(listenAddr string, nodes ...string) *FileServer {
	tcptransportOpts := p2p.TCPtransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: p2p.NOPHandshakefunc,
		Decoder:       p2p.DefaultDecoder{},
		// TODO : OnPeer func
	}
	tcptransport := p2p.NewTCPTransport(tcptransportOpts)
	fileServerOpts := FileServerOpts{
		StorageRoot:       listenAddr + "_network",
		PathTransformFunc: CASPathTransformFunc,
		Transport:         tcptransport,
		BootstrapNodes:    nodes,
	}
	s := NewFileServer(fileServerOpts)
	tcptransport.OnPeer = s.OnPeer
	return s
}

func main() {
	s1 := makeServer(":3000", "")
	s2 := makeServer(":4000", ":3000")

	go func() {
		log.Fatal(s1.Start())
	}()

	time.Sleep(1 * time.Second)
	go s2.Start()
	time.Sleep(1 * time.Second)

	data := bytes.NewReader([]byte("my big data file here!"))

	s2.StoreData("myprivatedata", data)

	select {}
}
