package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/Sanim27/DistributedFS/p2p"
)

func makeServer(listenAddr string, nodes ...string) *FileServer {
	tcptransportOpts := p2p.TCPTranportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder:       p2p.DefaultDecoder{},
	}
	tcpTransport := p2p.NewTCPTransport(tcptransportOpts)

	fileServerOpts := FileServerOpts{
		EncKey:            newEncryptionKey(),
		StorageRoot:       listenAddr + "_network",
		PathTransformFunc: CASPathTransformFunc,
		Transport:         tcpTransport,
		BootstrapNodes:    nodes,
	}
	s := NewFileServer(fileServerOpts)
	tcpTransport.TCPTransportOpts.OnPeer = s.OnPeer
	return s
}

func main() {
	flag.Parse()
	args := flag.Args()

	// Check if a filename is provided for upload
	if len(args) > 0 {
		uploadMode(args[0])
		return
	}

	// Default demo mode
	demoMode()
}

func uploadMode(filename string) {
	// Check if file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		log.Fatalf("File not found: %s\n", filename)
	}

	// Open file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open file: %v\n", err)
	}
	defer file.Close()

	// Start servers
	fmt.Println("Starting servers...")
	s1 := makeServer(":3000", "")
	s2 := makeServer(":4000", "")
	s3 := makeServer(":5000", ":3000", ":4000")

	go func() { log.Fatal(s1.Start()) }()
	time.Sleep(500 * time.Millisecond)
	go func() { log.Fatal(s2.Start()) }()
	time.Sleep(2 * time.Second)
	go s3.Start()
	time.Sleep(2 * time.Second)

	// Upload file
	fmt.Printf("Uploading %s to distributed network...\n", filename)
	if err := s3.Store(filename, file); err != nil {
		log.Fatalf("Failed to upload file: %v\n", err)
	}

	fmt.Printf("✓ File '%s' successfully uploaded to all nodes!\n", filename)
	fmt.Printf("  - Stored in :3000_network/%s\n", s1.ID)
	fmt.Printf("  - Stored in :4000_network/%s\n", s2.ID)
	fmt.Printf("  - Stored in :5000_network/%s\n", s3.ID)

	time.Sleep(1 * time.Second)
}

func demoMode() {
	s1 := makeServer(":3000", "") // s1 fileserver -> one node
	s2 := makeServer(":4000", "") //s2 fileserver -> another node with port 4000
	s3 := makeServer(":5000", ":3000", ":4000")
	go func() { log.Fatal(s1.Start()) }()
	time.Sleep(500 * time.Millisecond)
	go func() { log.Fatal(s2.Start()) }()

	time.Sleep(2 * time.Second)

	go s3.Start()
	time.Sleep(2 * time.Second)

	for i := 0; i < 1; i++ {
		key := fmt.Sprintf("picture_%d.png", i)
		// key := "picture.png"
		data := bytes.NewReader([]byte("my big data file here!"))
		s3.Store(key, data)

		// time.Sleep(5 * time.Second)
		if err := s3.store.Delete(s3.ID, key); err != nil {
			log.Fatal(err)
		}
		// time.Sleep(2 * time.Second)

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
