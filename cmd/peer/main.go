package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/Sanim27/dfs/internal/peer"
	"github.com/Sanim27/dfs/p2p"
	"github.com/Sanim27/dfs/store"
)

func main() {
	listenAddr := flag.String("port", ":3000", "peer listen address")
	masterAddr := flag.String("master", ":9000", "master node address")
	upload := flag.String("upload", "", "file to upload to the DFS")
	download := flag.String("download", "", "file to download from the DFS")

	flag.Parse()

	// --- transport setup ---
	tcpOpts := p2p.TCPtransportOpts{
		ListenAddr:    "localhost" + *listenAddr,
		HandshakeFunc: p2p.NOPHandshakefunc,
		Decoder:       p2p.DefaultDecoder{},
	}
	transport := p2p.NewTCPTransport(tcpOpts)

	// --- peer(server+client) setup ---
	opts := peer.FileServerOpts{
		EncKey:            p2p.NewEncryptionKey(),
		StorageRoot:       (*listenAddr) + "_storage",
		PathTransformFunc: store.CASPathTransformFunc,
		Transport:         transport,
		MasterAddr:        *masterAddr,
	}

	s := peer.NewFileServer(opts)
	transport.OnPeer = s.OnPeer

	log.Printf("[PEER] starting on %s (master: %s)\n", *listenAddr, *masterAddr)

	go func() {
		nodes, err := s.PollMaster(2 * time.Second)
		if err != nil {
			log.Printf("poll master stopped: %v", err)
			return
		}
		log.Printf("[PEER] master returned %d nodes: %v", len(nodes), nodes)
		s.BootstrapNodes = nodes
	}()

	go func() {
		if err := s.Start(); err != nil {
			log.Fatalf("server error: %v", err)
		}
	}()
	waitTimeout := 15 * time.Second
	start := time.Now()
	for s.NumPeers() == 0 && time.Since(start) < waitTimeout {
		//log.Println("waiting for peers to connect...")
		time.Sleep(300 * time.Millisecond)
	}
	if *upload != "" {
		if s.NumPeers() == 0 {
			log.Println("no peers connected within timeout — will store locally and continue")
		} else {
			log.Printf("peers connected: %d — proceeding with upload\n", s.NumPeers())
		}

		f, err := os.Open(*upload)
		if err != nil {
			log.Fatalf("Failed to open upload file: %v", err)
		}
		defer f.Close()
		if err := s.Store(*upload, f); err != nil {
			log.Fatalf("upload failed: %v", err)
		}
		log.Println("Upload complete.")
	}

	if *download != "" {
		if s.NumPeers() == 0 {
			log.Println("no peers connected within timeout — will provide file if it is present in local")
		} else {
			log.Printf("peers connected: %d — if not in local may fetch from them. \n", s.NumPeers())
		}

		r, err := s.Get(*download)
		if err != nil {
			log.Fatal(err)
		}
		b, err := io.ReadAll(r)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(b))

		log.Println("Download complete.")
	}

	select {} // keep running
}
