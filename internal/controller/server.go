package controller

import (
	"context"
	"errors"
	"kv_store/gen/kvpb"
	"kv_store/internal/storage"
	"kv_store/internal/wal"
)

type Server struct {
	kvpb.UnimplementedKVServer
	store *storage.Store
	wal   *wal.WAL
}

func NewServer(store *storage.Store, w *wal.WAL) *Server {
	return &Server{
		store: store,
		wal:   w,
	}
}

func (s *Server) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	value, ok := s.store.Get(req.Key)

	if !ok {
		return nil, errors.New("Key Not Found")
	}

	return &kvpb.GetResponse{
		Value: value,
		Found: true,
	}, nil
}

func (s *Server) Set(ctx context.Context, req *kvpb.SetRequest) (*kvpb.SetResponse, error) {
	entry := wal.LogEntry{
		Command: "SET",
		Key:     req.Key,
		Value:   req.Value,
	}
	if err := s.wal.Write(entry); err != nil {
		return nil, err
	}

	s.store.Set(req.Key, req.Value)
	return &kvpb.SetResponse{Ok: true}, nil
}

func (s *Server) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	entry := wal.LogEntry{
		Command: "DELETE",
		Key:     req.Key,
		Value:   "",
	}
	if err := s.wal.Write(entry); err != nil {
		return nil, err
	}

	ok := s.store.Delete(req.Key)

	if !ok {
		return nil, errors.New("Server error")
	}

	return &kvpb.DeleteResponse{Deleted: true}, nil
}
