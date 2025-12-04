package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"dfs-project/dfspb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const CHUNK_SIZE = 1 * 1024 * 1024 // 1MB chunks

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
	} else {
		log.Fatal("Use: upload or download")
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
	master := dfspb.NewMasterServerClient(conn) // FIXED: NewMasterServerClient

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

		primary := allocResp.Locations[0]
		replicas := allocResp.Locations[1:]

		chunkConn, err := grpc.Dial(primary, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal(err)
		}
		client := dfspb.NewChunkServerClient(chunkConn)

		_, err = client.ForwardChunk(context.Background(), &dfspb.ForwardChunkRequest{
			ChunkId:       allocResp.ChunkId,
			Data:          chunk,
			NextLocations: replicas,
		})
		if err != nil {
			log.Fatal("ForwardChunk failed:", err)
		}

		fmt.Printf("Uploaded chunk %d/%d → %s (replicated, %d KB)\n", (i/CHUNK_SIZE)+1, totalChunks, allocResp.ChunkId, len(chunk)/1024)
		chunkConn.Close()
	}
	log.Println("FULL FILE UPLOADED WITH MULTI-CHUNKS")
}

func download(filename string) {
	conn, err := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	master := dfspb.NewMasterServerClient(conn) // FIXED: NewMasterServerClient

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
			log.Printf("Read success from %s", location)
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