package persistence

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"tcp_test/storage"
)

type Snapshot struct {
	CreatedAt time.Time               `json:"created_at"`
	Entries   []storage.SnapshotEntry `json:"entries"`
}

type SnapshotConfig struct {
	Directory string
	Filename  string
}

func NewSnapshotConfig(directory, filename string) SnapshotConfig {
	return SnapshotConfig{
		Directory: directory,
		Filename:  filename,
	}
}

type SnapshotWriter struct {
	config SnapshotConfig
}

func NewSnapshotWriter(config SnapshotConfig) *SnapshotWriter {
	return &SnapshotWriter{
		config: config,
	}
}

func (s *SnapshotWriter) CreateSnapshot(cache *storage.Cache) error {
	snapshot := Snapshot{
		CreatedAt: time.Now(),
		Entries:   cache.SnapshotEntries(),
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(s.config.Directory, 0755); err != nil {
		return err
	}

	path := filepath.Join(s.config.Directory, s.config.Filename)
	tmpPath := path + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, path)
}