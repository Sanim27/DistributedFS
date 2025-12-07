package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"dfs-project/dfspb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const CHUNK_SIZE = 1 * 1024 * 1024

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Usage: go run client.go <upload|download> <filename>")
	}

	cmd := os.Args[1]
	file := os.Args[2]

	if cmd == "upload" {
		upload(file)
	} else if cmd == "download" {
		download(file)
	}
}

func upload(localPath string) {
	data, err := os.ReadFile(localPath)
	if err != nil {
		log.Fatal("Cannot read file:", err)
	}
	if len(data) == 0 {
		log.Fatal("File is empty bhai!")
	}

	conn, err := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	master := dfspb.NewMasterServerClient(conn)

	_, err = master.CreateFile(context.Background(), &dfspb.CreateFileRequest{
		Filename:  localPath,
		TotalSize: int64(len(data)),
	})
	if err != nil {
		log.Fatal("CreateFile failed:", err)
	}

	totalChunks := (len(data) + CHUNK_SIZE - 1) / CHUNK_SIZE
	log.Printf("Uploading %s → %d chunks (%d MB)", localPath, totalChunks, len(data)/(1024*1024))

	for i := 0; i < len(data); i += CHUNK_SIZE {
		end := i + CHUNK_SIZE
		if end > len(data) {
			end = len(data)
		}
		chunk := data[i:end]

		allocResp, err := master.AllocateChunk(context.Background(), &dfspb.AllocateChunkRequest{
			Filename: localPath,
		})
		if err != nil {
			log.Fatal("AllocateChunk failed:", err)
		}

		// Find first alive primary
		var primary string
		for _, loc := range allocResp.Locations {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			conn, err := grpc.DialContext(ctx, loc, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				primary = loc
				conn.Close()
				break
			}
		}
		if primary == "" {
			log.Fatal("No alive chunkservers!")
		}

		// Remaining as replicas
		var replicas []string
		for _, loc := range allocResp.Locations {
			if loc != primary {
				replicas = append(replicas, loc)
			}
		}

		chunkConn, _ := grpc.Dial(primary, grpc.WithTransportCredentials(insecure.NewCredentials()))
		client := dfspb.NewChunkServerClient(chunkConn)

		_, err = client.ForwardChunk(context.Background(), &dfspb.ForwardChunkRequest{
			ChunkId:       allocResp.ChunkId,
			Data:          chunk,
			NextLocations: replicas,
		})
		if err != nil {
			log.Fatal("ForwardChunk failed:", err)
		}

		fmt.Printf("Uploaded chunk %d/%d → %s (primary: %s, replicated to %d others, %d KB)\n",
			(i/CHUNK_SIZE)+1, totalChunks, allocResp.ChunkId, primary, len(replicas), len(chunk)/1024)
		chunkConn.Close()
	}
	
}

func download(filename string) {
	conn, err := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	master := dfspb.NewMasterServerClient(conn)

	meta, err := master.GetFileMetadata(context.Background(), &dfspb.GetFileMetadataRequest{Filename: filename})
	if err != nil || len(meta.ChunkIds) == 0 {
		log.Fatal("GetFileMetadata failed or file not found:", err)
	}

	var result []byte
	log.Printf("Downloading %d chunks...", len(meta.ChunkIds))

	for i, chunkID := range meta.ChunkIds {
		var data []byte
		for _, location := range meta.Locations {
			chunkConn, connErr := grpc.Dial(location, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if connErr != nil {
				log.Printf("Connect failed to %s – trying next", location)
				continue
			}
			client := dfspb.NewChunkServerClient(chunkConn)

			resp, readErr := client.ReadChunk(context.Background(), &dfspb.ReadChunkRequest{ChunkId: chunkID})
			chunkConn.Close()
			if readErr != nil {
				log.Printf("Read failed from %s – trying next", location)
				continue
			}
			data = resp.Data
			log.Printf("Successfully read from %s", location)
			break
		}
		if len(data) == 0 {
			log.Fatal("All replicas failed for chunk:", chunkID)
		}
		result = append(result, data...)
		fmt.Printf("Downloaded chunk %d/%d (%d KB)\n", i+1, len(meta.ChunkIds), len(data)/1024)
	}

	outputFile := "downloaded_" + filename
	os.WriteFile(outputFile, result, 0644)
	log.Printf("Successfully downloaded to %s", outputFile)
	
}