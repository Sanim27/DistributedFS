// package main

// import (
// 	"bytes"
// 	"fmt"
// 	"io"
// 	"log"
// 	"time"

// 	"github.com/Sanim27/dfs/p2p"
// )

// func makeServer(listenAddr string, nodes ...string) *FileServer {
// 	tcptransportOpts := p2p.TCPtransportOpts{
// 		ListenAddr:    listenAddr,
// 		HandshakeFunc: p2p.NOPHandshakefunc,
// 		Decoder:       p2p.DefaultDecoder{},
// 		// TODO : OnPeer func
// 	}
// 	tcptransport := p2p.NewTCPTransport(tcptransportOpts)
// 	fileServerOpts := FileServerOpts{
// 		EncKey:            newEncryptionKey(),
// 		StorageRoot:       listenAddr + "_network",
// 		PathTransformFunc: CASPathTransformFunc,
// 		Transport:         tcptransport,
// 		BootstrapNodes:    nodes,
// 	}
// 	s := NewFileServer(fileServerOpts)
// 	tcptransport.OnPeer = s.OnPeer
// 	return s
// }

// func main() {
// 	s1 := makeServer(":3000", "")
// 	s2 := makeServer(":4000", ":3000")

// 	go func() {
// 		log.Fatal(s1.Start())
// 	}()

// 	time.Sleep(1 * time.Second)

// 	go s2.Start()
// 	time.Sleep(1 * time.Second)

// 	// for i := 0; i < 1; i++ {
// 	// 	data := bytes.NewReader([]byte("my big data file here!"))
// 	// 	s2.Store(fmt.Sprintf("myprivatedata_%d", i), data)
// 	// 	time.Sleep(5 * time.Millisecond)
// 	// }

// 	// time.Sleep(5 * time.Millisecond)

// 	for i := 0; i < 20; i++ {
// 		key := fmt.Sprintf("picture_%d", i)
// 		data := bytes.NewReader([]byte("my big data file here!"))
// 		s2.Store(key, data)
// 		time.Sleep(5 * time.Millisecond)

// 		if err := s2.store.Delete(key); err != nil {
// 			log.Fatal(err)
// 		}

// 		r, err := s2.Get(key)
// 		if err != nil {
// 			log.Fatal(err)
// 		}

// 		b, err := io.ReadAll(r)
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 		fmt.Println(string(b))

// 	}
// }

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
	tcptransportOpts := p2p.TCPtransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: p2p.NOPHandshakefunc,
		Decoder:       p2p.DefaultDecoder{},
		// TODO : OnPeer func
	}
	tcptransport := p2p.NewTCPTransport(tcptransportOpts)
	fileServerOpts := FileServerOpts{
		EncKey:            newEncryptionKey(),
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
	s3 := makeServer(":6000", ":3000", ":4000")

	// go func() {
	// 	log.Fatal(s1.Start())
	// 	time.Sleep(500 * time.Millisecond)
	// 	log.Fatal(s2.Start())
	// }()

	go func() { log.Fatal(s1.Start()) }()
	go func() { log.Fatal(s2.Start()) }()

	// go func() {
	// 	if err := s1.Start(); err != nil {
	// 		log.Fatal(err)
	// 	}
	// }()

	// time.Sleep(time.Second) // allow peer discovery

	// go func() {
	// 	if err := s2.Start(); err != nil {
	// 		log.Fatal(err)
	// 	}
	// }()

	time.Sleep(500 * time.Millisecond)

	go s3.Start()
	time.Sleep(500 * time.Millisecond)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("picture_%d", i)
		data := bytes.NewReader([]byte("my big data file here!"))
		s3.Store(key, data)
		time.Sleep(5 * time.Millisecond)

		if err := s3.store.Delete(s3.ID, key); err != nil {
			log.Fatal(err)
		}

		r, err := s3.Get(key)
		if err != nil {
			log.Fatal(err)
		}

		b, err := io.ReadAll(r)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(b))

	}
}
