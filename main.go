package main

import (
	"bytes"
	"fmt"
	"io"

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
		EncKey: newEncryptionKey(),
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

	// for i := 0; i < 20; i++ {
		// key := fmt.Sprintf("picture_%d.png",i)
		key := "picture.png"
		data := bytes.NewReader([]byte("my big data file here!"))
		s2.Store(key, data)
		
		time.Sleep(5 * time.Second)
		if err := s2.store.Delete(key); err != nil {
			log.Fatal(err)
		}
		time.Sleep(2 * time.Second)

		r, err := s2.Get(key)
		if err != nil {
			log.Fatal(err)
		}
		b, err := io.ReadAll(r)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(b))	
	// }
}