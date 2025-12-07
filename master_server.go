package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"dfs-project/dfspb"
	"google.golang.org/grpc"
)

type ServerInfo struct {
	LastHeartbeat time.Time
	Alive         bool
}

type MasterServer struct {
	dfspb.UnimplementedMasterServerServer
	mu           sync.Mutex
	fileChunks   map[string][]string
	fileSizes    map[string]int64
	chunkServers []string

	servers   map[string]*ServerInfo
	serversMu sync.RWMutex
}

func (m *MasterServer) CreateFile(ctx context.Context, req *dfspb.CreateFileRequest) (*dfspb.CreateFileResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.fileChunks[req.Filename] = []string{}
	m.fileSizes[req.Filename] = req.TotalSize

	log.Printf("Created %s (%d bytes)", req.Filename, req.TotalSize)
	return &dfspb.CreateFileResponse{Success: true}, nil
}

func (m *MasterServer) AllocateChunk(ctx context.Context, req *dfspb.AllocateChunkRequest) (*dfspb.AllocateChunkResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	filename := req.Filename
	chunks := m.fileChunks[filename]

	chunkID := fmt.Sprintf("%s_chunk_%03d", filename, len(chunks)+1)

	m.serversMu.RLock()
	var healthy []string
	for _, addr := range m.chunkServers {
		if info, ok := m.servers[addr]; ok && info.Alive {
			healthy = append(healthy, addr)
		}
	}
	m.serversMu.RUnlock()

	if len(healthy) == 0 {
		return nil, fmt.Errorf("no healthy chunkservers")
	}

	locations := healthy
	if len(healthy) > 3 {
		locations = healthy[:3]
	}

	m.fileChunks[filename] = append(chunks, chunkID)

	log.Printf("Allocated %s to %v", chunkID, locations)
	return &dfspb.AllocateChunkResponse{
		ChunkId:   chunkID,
		Locations: locations,
	}, nil
}

func (m *MasterServer) GetFileMetadata(ctx context.Context, req *dfspb.GetFileMetadataRequest) (*dfspb.GetFileMetadataResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	chunks := m.fileChunks[req.Filename]
	size := m.fileSizes[req.Filename]

	if len(chunks) == 0 {
		return &dfspb.GetFileMetadataResponse{}, nil
	}

	m.serversMu.RLock()
	var healthyLocations []string
	for _, addr := range m.chunkServers {
		if info, ok := m.servers[addr]; ok && info.Alive {
			healthyLocations = append(healthyLocations, addr)
		}
	}
	m.serversMu.RUnlock()

	return &dfspb.GetFileMetadataResponse{
		ChunkIds:  chunks,
		Locations: healthyLocations,
		FileSize:  size,
	}, nil
}

func (m *MasterServer) SendHeartbeat(ctx context.Context, req *dfspb.HeartbeatRequest) (*dfspb.HeartbeatResponse, error) {
	m.serversMu.Lock()
	if _, exists := m.servers[req.Address]; !exists {
		m.servers[req.Address] = &ServerInfo{}
		log.Printf("New chunkserver: %s", req.Address)
	}
	m.servers[req.Address].LastHeartbeat = time.Now()
	m.servers[req.Address].Alive = true
	m.serversMu.Unlock()

	log.Printf("Heartbeat from %s", req.Address)
	return &dfspb.HeartbeatResponse{Success: true}, nil
}

func main() {
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	server := &MasterServer{
		fileChunks:   make(map[string][]string),
		fileSizes:    make(map[string]int64),
		chunkServers: []string{"127.0.0.1:9001", "127.0.0.1:9002", "10.100.31.12:9003"},
		servers:      make(map[string]*ServerInfo),
	}
	dfspb.RegisterMasterServerServer(s, server)

	// Dead detector goroutine
	go func() {
		for {
			time.Sleep(10 * time.Second)
			server.serversMu.Lock()
			for addr, info := range server.servers {
				if time.Since(info.LastHeartbeat) > 20*time.Second {
					if info.Alive {
						info.Alive = false
						log.Printf("DEAD SERVER: %s", addr)
					}
				}
			}
			server.serversMu.Unlock()
		}
	}()

	log.Println("Master running on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}