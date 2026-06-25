package persistence

import (
	"fmt";
	"os";
	"sync";
	"encoding/json";
	"time";
)

type Operation string

const (
	SET    Operation = "SET"
	DELETE Operation = "DELETE"
)

type WALEntry struct {
	Op        Operation `json:"op"`
	Key       string    `json:"key"`
	Value     string    `json:"value,omitempty"`
	TTL       int64     `json:"ttl,omitempty"`       // seconds
	Timestamp int64     `json:"ts"`                  // unix timestamp
}

type WAL struct {
	file  *os.File
	mu    sync.Mutex 
}

func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return nil, err
	}

	return &WAL{file: f,}, nil
}

func (w *WAL) SetWAL(key, value string, ttl int64) error {
	entry := WALEntry{
		Op:        SET,
		Key:       key,
		Value:     value,
		TTL:       ttl,
		Timestamp: time.Now().Unix(),
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	_, err = w.file.Write(append(data, '\n'))
	if err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) DeleteWAL(key string) error {
	entry := WALEntry{
		Op:        DELETE,
		Key:       key,
		Timestamp: time.Now().Unix(),
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	_, err = w.file.Write(append(data, '\n'))

	if err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Close() error {
    w.mu.Lock()
    defer w.mu.Unlock()

    return w.file.Close()
}