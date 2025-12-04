package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"path/filepath"

	"dfs-project/dfspb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ChunkServer struct {
	dfspb.UnimplementedChunkServerServer
	storagePath string
}

func (c *ChunkServer) WriteChunk(ctx context.Context, req *dfspb.WriteChunkRequest) (*dfspb.WriteChunkResponse, error) {
	path := filepath.Join(c.storagePath, req.ChunkId)
	err := os.WriteFile(path, req.Data, 0644)
	if err != nil {
		return &dfspb.WriteChunkResponse{Success: false}, err
	}
	log.Printf("Stored %s (%d bytes)", req.ChunkId, len(req.Data))
	return &dfspb.WriteChunkResponse{Success: true}, nil
}

func (c *ChunkServer) ReadChunk(ctx context.Context, req *dfspb.ReadChunkRequest) (*dfspb.ReadChunkResponse, error) {
	path := filepath.Join(c.storagePath, req.ChunkId)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	log.Printf("Sent %s (%d bytes)", req.ChunkId, len(data))
	return &dfspb.ReadChunkResponse{Data: data}, nil
}

func (c *ChunkServer) ForwardChunk(ctx context.Context, req *dfspb.ForwardChunkRequest) (*dfspb.WriteChunkResponse, error) {
	// Local save kar
	path := filepath.Join(c.storagePath, req.ChunkId)
	err := os.WriteFile(path, req.Data, 0644)
	if err != nil {
		return &dfspb.WriteChunkResponse{Success: false}, err
	}
	log.Printf("Replicated locally %s (%d bytes)", req.ChunkId, len(req.Data))

	// Next replica pe pipeline forward kar
	if len(req.NextLocations) > 0 {
		nextAddr := req.NextLocations[0]
		remaining := req.NextLocations[1:]

		conn, err := grpc.Dial(nextAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return &dfspb.WriteChunkResponse{Success: false}, err
		}
		defer conn.Close()

		client := dfspb.NewChunkServerClient(conn)
		_, err = client.ForwardChunk(ctx, &dfspb.ForwardChunkRequest{
			ChunkId:       req.ChunkId,
			Data:          req.Data,
			NextLocations: remaining,
		})
		if err != nil {
			return &dfspb.WriteChunkResponse{Success: false}, err
		}
		log.Printf("Forwarded %s to %s", req.ChunkId, nextAddr)
	}

	return &dfspb.WriteChunkResponse{Success: true}, nil
}

func main() {
	port := flag.String("port", "9001", "listen port")
	storage := flag.String("storage", "chunks", "storage dir")
	flag.Parse()

	os.MkdirAll(*storage, 0755)

	lis, err := net.Listen("tcp", "0.0.0.0:"+*port) // Bind to 0.0.0.0 for real networking
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	dfspb.RegisterChunkServerServer(s, &ChunkServer{storagePath: *storage})

	log.Printf("ChunkServer started on 0.0.0.0:%s (storage: %s)", *port, *storage)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}