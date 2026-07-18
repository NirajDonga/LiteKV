package wal

import (
	"encoding/json"
	"os"
	"sync"
)

type LogEntry struct {
	Command string
	Key     string
	Value   string
}

type WAL struct {
	file *os.File
	mu   sync.Mutex
}

func NewWal(filename string) (*WAL, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file: f}, nil
}

func (w *WAL) Write(entry LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := json.NewEncoder(w.file).Encode(entry); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Replay() ([]LogEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, err := w.file.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	var entries []LogEntry
	decoder := json.NewDecoder(w.file)

	for decoder.More() {
		var entry LogEntry
		if err := decoder.Decode(&entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	_, err = w.file.Seek(0, 2)
	if err != nil {
		return nil, err
	}

	return entries, nil

}
