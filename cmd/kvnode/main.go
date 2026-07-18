package main

import (
	"kv_store/gen/kvpb"
	"kv_store/internal/controller"
	"kv_store/internal/storage"
	"kv_store/internal/wal"
	"log"
	"net"

	"google.golang.org/grpc"
)

func main() {
	w, err := wal.NewWal("node.wal")
	if err != nil {
		log.Fatalf("Failed to initialize WAL: %v", err)
	}

	store := storage.NewStore()

	entries, err := w.Replay()
	if err != nil {
		log.Fatalf("Failed to replay WAL: %v", err)
	}

	for _, entry := range entries {
		switch entry.Command {
		case "SET":
			store.Set(entry.Key, entry.Value)
		case "DELETE":
			store.Delete(entry.Key)
		}
	}
	log.Printf("Successfully replayed %d entries from WAL", len(entries))

	server := controller.NewServer(store, w)

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	kvpb.RegisterKVServer(grpcServer, server)
	log.Println("kvnode listening on :50051")

	err = grpcServer.Serve(listener)
	if err != nil {
		log.Fatal(err)
	}
}
