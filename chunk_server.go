package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

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
	path := filepath.Join(c.storagePath, req.ChunkId)
	err := os.WriteFile(path, req.Data, 0644)
	if err != nil {
		return &dfspb.WriteChunkResponse{Success: false}, err
	}
	log.Printf("Replicated %s (%d bytes)", req.ChunkId, len(req.Data))

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
	}

	return &dfspb.WriteChunkResponse{Success: true}, nil
}

func main() {
	port := flag.String("port", "9001", "server port")
	storage := flag.String("storage", "chunks", "storage directory")
	flag.Parse()

	os.MkdirAll(*storage, 0755)

	lis, err := net.Listen("tcp", "0.0.0.0:"+*port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	dfspb.RegisterChunkServerServer(s, &ChunkServer{storagePath: *storage})

	// Heartbeat goroutine
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		conn, err := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Failed to connect to master for heartbeat: %v", err)
		}
		defer conn.Close()
		masterClient := dfspb.NewMasterServerClient(conn)

		myAddr := fmt.Sprintf("127.0.0.1:%s", *port)  // Change to real IP if needed

		for range ticker.C {
			_, err := masterClient.SendHeartbeat(context.Background(), &dfspb.HeartbeatRequest{
				Address: myAddr,
			})
			if err != nil {
				log.Printf("Heartbeat failed: %v", err)
			} else {
				log.Printf("Heartbeat sent to master from %s", myAddr)
			}
		}
	}()

	log.Printf("ChunkServer running on 0.0.0.0:%s (storage: %s)", *port, *storage)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}