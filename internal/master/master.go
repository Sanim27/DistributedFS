// package master

// import (
// 	"errors"
// 	"fmt"
// 	"log"
// 	"net"
// 	"sync"
// 	"time"
// )

// type Master struct {
// 	ListenAddr string
// 	Listener   net.Listener

// 	HBLock     sync.Mutex
// 	Heartbeats map[string]time.Time
// }

// func NewMaster(listenaddr string) *Master {
// 	return &Master{
// 		ListenAddr: listenaddr,
// 		Heartbeats: make(map[string]time.Time),
// 	}
// }

// func (m *Master) listenToClients() error {
// 	var err error
// 	m.Listener, err = net.Listen("tcp", m.ListenAddr)
// 	if err != nil {
// 		return err
// 	}

// 	go m.startAcceptLoop()
// 	log.Printf("Master listening on port %s\n", m.ListenAddr)
// 	return nil
// }

// func (m *Master) startAcceptLoop() {
// 	for {
// 		conn, err := m.Listener.Accept()
// 		if errors.Is(err, net.ErrClosed) {
// 			return
// 		}
// 		if err != nil {
// 			fmt.Printf("TCP accept error: %s \n", err)
// 		}

// 		go m.handleConn(conn)
// 	}
// }

// func (m *Master) handleConn(conn net.Conn) {
// 	defer conn.Close()
// 	buf := make([]byte, 256)
// 	n, err := conn.Read(buf)
// 	if err != nil {
// 		log.Println("master read error:", err)
// 		return
// 	}
// 	msg := string(buf[:n])
// 	fmt.Printf("[MASTER] received %s \n", msg)

// 	var typ, peerID string
// 	fmt.Sscanf(msg, "%s %s", &typ, &peerID)

// 	if typ == "HEARTBEAT" {
// 		m.HBLock.Lock()
// 		m.Heartbeats[peerID] = time.Now()
// 		m.HBLock.Unlock()
// 	}
// }

package master

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

type Master struct {
	ListenAddr string
	Listener   net.Listener

	HBLock     sync.Mutex
	Heartbeats map[string]time.Time
	TTL        time.Duration // expiration duration for peers
}

func NewMaster(listenaddr string) *Master {
	return &Master{
		ListenAddr: listenaddr,
		Heartbeats: make(map[string]time.Time),
		TTL:        15 * time.Second,
	}
}

func (m *Master) listenToClients() error {
	var err error
	m.Listener, err = net.Listen("tcp", m.ListenAddr)
	if err != nil {
		return err
	}

	go m.startAcceptLoop()
	log.Printf("Master listening on %s\n", m.ListenAddr)
	return nil
}

func (m *Master) startAcceptLoop() {
	for {
		conn, err := m.Listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			fmt.Printf("TCP accept error: %s \n", err)
			continue
		}

		go m.handleConn(conn)
	}
}

func (m *Master) handleConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	// read a single line request
	line, err := r.ReadString('\n')
	if err != nil {
		// connection closed or error reading
		log.Println("master read error:", err)
		return
	}
	msg := strings.TrimSpace(line)
	fmt.Printf("[MASTER] received: %s\n", msg)

	// HEARTBEAT <addr>
	if strings.HasPrefix(msg, "HEARTBEAT ") {
		addr := strings.TrimSpace(strings.TrimPrefix(msg, "HEARTBEAT "))
		if addr != "" {
			m.HBLock.Lock()
			m.Heartbeats[addr] = time.Now()
			m.HBLock.Unlock()
			// optionally ack
			conn.Write([]byte("OK\n"))
		}
		return
	}

	// GETPEERS
	if msg == "GETPEERS" {
		// produce active list (non-expired)
		now := time.Now()
		var active []string
		m.HBLock.Lock()
		for addr, ts := range m.Heartbeats {
			if now.Sub(ts) <= m.TTL {
				active = append(active, addr)
			}
		}
		m.HBLock.Unlock()

		// reply comma-separated
		reply := strings.Join(active, ",") + "\n"
		_, _ = conn.Write([]byte(reply))
		return
	}

	// unknown request -> ignore
}

func (m *Master) heartbeatMonitor() {
	for {
		time.Sleep(10 * time.Second)
		now := time.Now()
		m.HBLock.Lock()
		for peer, ts := range m.Heartbeats {
			if now.Sub(ts) > 15*time.Second {
				fmt.Printf("[MASTER] peer %s DEAD \n", peer)
			}
		}
		m.HBLock.Unlock()
	}
}

func (m *Master) Start() error {
	// Start listening *synchronously* so we return an error if bind fails.
	if err := m.listenToClients(); err != nil {
		return err
	}

	// Now start the monitor in background.
	go m.heartbeatMonitor()
	// fmt.Println("Master server started and listening on", m.ListenAddr)
	return nil
}
