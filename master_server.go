package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"dfs-project/dfspb"
	"google.golang.org/grpc"
)

type MasterServer struct {
	dfspb.UnimplementedMasterServerServer
	mu          sync.Mutex
	fileChunks  map[string][]string // filename -> chunk_ids
	fileSizes   map[string]int64    // filename -> size
	chunkServers []string
}

func (m *MasterServer) CreateFile(ctx context.Context, req *dfspb.CreateFileRequest) (*dfspb.CreateFileResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.fileChunks == nil {
		m.fileChunks = make(map[string][]string)
		m.fileSizes = make(map[string]int64)
	}

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
	locations := m.chunkServers[0:3] // 3 replicas

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
	size := m.fileSizes[req.Filename] // Ab use ho raha hai

	if len(chunks) == 0 {
		return &dfspb.GetFileMetadataResponse{}, nil
	}

	return &dfspb.GetFileMetadataResponse{
		ChunkIds:  chunks, // Ab repeated hai
		Locations: m.chunkServers[0:3],
		FileSize:  size, // Use kar liya
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	dfspb.RegisterMasterServerServer(s, &MasterServer{
		chunkServers: []string{"127.0.0.1:9001", "127.0.0.1:9002", "192.168.1.39:9003"},
	})

	log.Println("Master running on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}