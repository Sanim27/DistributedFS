package peer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Sanim27/dfs/p2p"
	"github.com/Sanim27/dfs/store"
)

type FileServerOpts struct {
	ID                string
	EncKey            []byte
	StorageRoot       string
	PathTransformFunc store.PathTransformFunc
	Transport         p2p.Transport
	MasterAddr        string
	BootstrapNodes    []string
}

type FileServer struct {
	FileServerOpts

	peerLock sync.Mutex
	peers    map[string]p2p.Peer

	store  *store.Store
	quitch chan struct{}
}

func NewFileServer(opts FileServerOpts) *FileServer {
	storeOpts := store.StoreOpts{
		Root:              opts.StorageRoot,
		PathTransformFunc: opts.PathTransformFunc,
	}

	if len(opts.ID) == 0 {
		opts.ID = p2p.GenerateID(storeOpts.Root)
	}

	return &FileServer{
		FileServerOpts: opts,
		store:          store.NewStore(storeOpts),
		quitch:         make(chan struct{}),
		peers:          make(map[string]p2p.Peer),
	}
}

func (s *FileServer) broadcast(msg *Message) error {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}
	for _, peer := range s.peers {
		peer.Send([]byte{p2p.IncomingMessage})
		if err := peer.Send(buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

type Message struct {
	Payload any
}

type MessageStoreFile struct {
	ID   string
	Key  string
	Size int64
}

type MessageGetFile struct {
	ID  string
	Key string
}

// type DataMessage struct {
// 	Key  string
// 	Data []byte
// }

type MessageHeartBeat struct {
}

// NumPeers returns how many peers are currently connected.
func (s *FileServer) NumPeers() int {
	s.peerLock.Lock()
	defer s.peerLock.Unlock()
	return len(s.peers)
}

func (s *FileServer) Get(key string) (io.Reader, error) {
	if s.store.Has(s.ID, key) {
		fmt.Printf("[%s] serving file (%s) from local disk...\n", s.Transport.Addr(), key)
		_, r, err := s.store.Read(s.ID, key)
		return r, err
	}

	fmt.Printf("[%s] dont have file (%s) locally, fetching from network...\n", s.Transport.Addr(), key)

	msg := Message{
		Payload: MessageGetFile{
			ID:  s.ID,
			Key: p2p.HashKey(key),
		},
	}

	if err := s.broadcast(&msg); err != nil {
		return nil, err
	}

	time.Sleep(time.Millisecond * 500)

	for _, peer := range s.peers {
		// First we read the file size to limit the amount of bytes
		// we read from the connection, so it will not keep blocking.
		var fileSize int64
		binary.Read(peer, binary.LittleEndian, &fileSize)

		n, err := s.store.WriteDecrypt(s.EncKey, s.ID, key, io.LimitReader(peer, fileSize))
		if err != nil {
			return nil, err
		}
		fmt.Printf("[%s] received [%d] bytes over the network from (%s) ", s.Transport.Addr(), n, peer.RemoteAddr())

		peer.CloseStream()
	}

	_, r, err := s.store.Read(s.ID, key)
	return r, err
}

func (s *FileServer) Store(key string, r io.Reader) error {

	fileBuffer := new(bytes.Buffer)
	tee := io.TeeReader(r, fileBuffer)

	size, err := s.store.Write(s.ID, key, tee)
	if err != nil {
		return err
	}

	msg := Message{
		Payload: MessageStoreFile{
			ID:   s.ID,
			Key:  p2p.HashKey(key),
			Size: size + 16,
		},
	}

	time.Sleep(time.Millisecond * 5)

	if err := s.broadcast(&msg); err != nil {
		return err
	}

	time.Sleep(time.Millisecond * 5)

	peers := []io.Writer{}
	for _, peer := range s.peers {
		peers = append(peers, peer)
	}
	mw := io.MultiWriter(peers...)
	mw.Write([]byte{p2p.IncomingStream})
	n, err := p2p.CopyEncrypt(s.EncKey, fileBuffer, mw)
	if err != nil {
		return err
	}
	fmt.Printf("[%s] received and written (%d) bytes to disk \n", s.Transport.Addr(), n)
	return nil
}

func (s *FileServer) Stop() {
	close(s.quitch)
}

func (s *FileServer) OnPeer(p p2p.Peer) error {
	s.peerLock.Lock()
	defer s.peerLock.Unlock()
	s.peers[p.RemoteAddr().String()] = p

	log.Printf("connected with remote %s", p.RemoteAddr())
	return nil
}

func (s *FileServer) loop() {
	defer func() {
		fmt.Println("File server closed due to user action")
		s.Transport.Close()
	}()

	for {
		select {
		case rpc := <-s.Transport.Consume():
			var msg Message
			if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&msg); err != nil {
				log.Println("decoding error: ", err)
				// return
			}
			if err := s.handleMessage(rpc.From, &msg); err != nil {
				log.Println("handle message error: ", err)
				// return
			}
		case <-s.quitch:
			return
		}
	}
}

func (s *FileServer) handleMessage(from string, msg *Message) error {
	switch v := msg.Payload.(type) {
	case MessageStoreFile:
		return s.handleMessageStoreFile(from, v)
	case MessageGetFile:
		return s.handleMessageGetFile(from, v)
	}

	return nil
}

func (s *FileServer) handleMessageGetFile(from string, msg MessageGetFile) error {
	// need to get a file from disk and send it over the wire

	if !s.store.Has(msg.ID, msg.Key) {
		return fmt.Errorf("[%s] need to serve file (%s) ; but it doesnt exist on disk", s.Transport.Addr(), msg.Key)
	}

	fmt.Printf("[%s] serving file (%s) over the network\n", s.Transport.Addr(), msg.Key)

	fileSize, r, err := s.store.Read(msg.ID, msg.Key)
	if err != nil {
		return err
	}

	// assert
	if rc, ok := r.(io.ReadCloser); ok {
		fmt.Println("closing readCloser")
		defer rc.Close()
	}

	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer %s not in map", from)
	}

	peer.Send([]byte{p2p.IncomingStream})
	binary.Write(peer, binary.LittleEndian, fileSize)
	n, err := io.Copy(peer, r)
	if err != nil {
		return err
	}

	fmt.Printf("[%s] written %d bytes over the network to %s\n", s.Transport.Addr(), n, from)

	return nil
}

func (s *FileServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {

	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer (%s) could not be found in the peer list", from)
	}

	n, err := s.store.Write(msg.ID, msg.Key, io.LimitReader(peer, msg.Size))
	if err != nil {
		return err
	}

	fmt.Printf("[%s] written %d bytes to disk \n", s.Transport.Addr(), n)
	peer.CloseStream()

	return nil
}

func (s *FileServer) bootstrapNetwork() error {
	for _, addr := range s.BootstrapNodes {
		if len(addr) == 0 {
			continue
		}
		go func(addr string) {
			fmt.Printf("[%s] attempting to connect with remote %s \n", s.Transport.Addr(), addr)
			if err := s.Transport.Dial(addr); err != nil {
				log.Println("dial error: ", err)
			}
		}(addr)
	}
	return nil
}

func (s *FileServer) SendHeartBeatLoop(addr string) {
	for {
		select {
		case <-s.quitch:
			return
		default:
		}

		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.Printf("[%s] heartbeat to %s failed: %v (will retry)\n", s.Transport.Addr(), addr, err)
		} else {
			hb := fmt.Sprintf("HEARTBEAT %s\n", s.Transport.Addr())
			n, werr := conn.Write([]byte(hb))
			if werr != nil {
				log.Printf("[%s] heartbeat write error: %v\n", s.Transport.Addr(), werr)
			} else {
				log.Printf("[%s] heartbeat sent %d bytes to %s\n", s.Transport.Addr(), n, addr)
			}
			conn.Close()
		}
		time.Sleep(5 * time.Second)
	}
}

// PollMaster queries the master repeatedly until it obtains a non-empty peer list,
// then returns the list. It respects s.quitch so it will stop if the server is stopped.
func (s *FileServer) PollMaster(interval time.Duration) ([]string, error) {
	for {
		// allow shutdown
		select {
		case <-s.quitch:
			return nil, fmt.Errorf("server stopped")
		default:
		}

		masterAddr := strings.TrimSpace(s.MasterAddr)
		if masterAddr == "" {
			time.Sleep(interval)
			continue
		}

		conn, err := net.Dial("tcp", masterAddr)
		if err != nil {
			log.Printf("[%s] pollMaster: dial master %s failed: %v", s.Transport.Addr(), masterAddr, err)
			time.Sleep(interval)
			continue
		}

		// ask for peers
		_, _ = conn.Write([]byte("GETPEERS\n"))
		reader := bufio.NewReader(conn)
		line, err := reader.ReadString('\n')
		conn.Close()
		if err != nil {
			log.Printf("[%s] pollMaster: read reply failed: %v", s.Transport.Addr(), err)
			time.Sleep(interval)
			continue
		}

		line = strings.TrimSpace(line)
		if line == "" {
			// no peers reported yet
			time.Sleep(interval)
			continue
		}

		raw := strings.Split(line, ",")
		peers := make([]string, 0, len(raw))
		for _, p := range raw {
			p = strings.TrimSpace(p)
			if p == "" || p == s.Transport.Addr() {
				continue
			}
			peers = append(peers, p)
		}

		if len(peers) == 0 {
			time.Sleep(interval)
			continue
		}

		// success: return the list (caller can set s.BootstrapNodes and call bootstrapNetwork)
		return peers, nil
	}
}

func (s *FileServer) Start() error {
	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}

	go s.SendHeartBeatLoop(s.MasterAddr)

	s.bootstrapNetwork()

	s.loop()

	return nil
}

func init() {
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
}
