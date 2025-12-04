package main

import (
	"bytes"
	"log"
	"time"

	"github.com/Sanim27/DistributedFS/p2p"
)

func makeServer(listenAddr string, nodes ...string) *FileServer {
	tcptransportOpts := p2p.TCPTranportOpts{
		ListenAddr: listenAddr,
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder: p2p.DefaultDecoder{},
	}
	tcpTransport := p2p.NewTCPTransport(tcptransportOpts)


	fileServerOpts := FileServerOpts{
		StorageRoot: listenAddr+ "_network",
		PathTransformFunc: CASPathTranformFunc,
		Transport: tcpTransport,
		BootstrapNodes: nodes,
		
	}
	s := NewFileServer(fileServerOpts)
	tcpTransport.TCPTransportOpts.OnPeer = s.OnPeer
	return s


}
func main() {
	s1 := makeServer(":3000", "") // s1 fileserver -> one node 
	s2 := makeServer(":4000", ":3000") //s2 fileserver -> another node with port 4000
	go func ()  {
		log.Fatal(s1.Start())
	}()
	time.Sleep(4 * time.Second) // haven't understood

	go s2.Start()
	time.Sleep(4 * time.Second) // haven't understood

	for i := 0; i < 1; i++ {
		data := bytes.NewReader([]byte("my big data file here!"))
		s2.Store("myprivatedata", data)
		time.Sleep(5 * time.Millisecond)
	}
	select {}
}