package persistence

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"tcp_test/storage"
)

// Snapshot represents a point-in-time snapshot of the cache state.
type Snapshot struct {
	CreatedAt time.Time                `json:"created_at"`
	Entries   map[string]SnapshotEntry `json:"entries"`
}

// SnapshotEntry represents a single cache entry in the snapshot.
type SnapshotEntry struct {
	Value     string    `json:"value"`
	ExpiresAt time.Time `json:"expires_at"`
}

// SnapshotConfig holds configuration for snapshot operations.
type SnapshotConfig struct {
	Directory string
	Filename  string
}

// DefaultSnapshotConfig returns default snapshot configuration.
func DefaultSnapshotConfig() SnapshotConfig {
	return SnapshotConfig{
		Directory: ".",
		Filename:  "snapshot.json",
	}
}

// SnapshotWriter handles snapshot creation and persistence.
type SnapshotWriter struct {
	mu     sync.RWMutex
	config SnapshotConfig
}

// NewSnapshotWriter creates a new snapshot writer with the given configuration.
func NewSnapshotWriter(config SnapshotConfig) *SnapshotWriter {
	return &SnapshotWriter{
		config: config,
	}
}

func (sw *SnapshotWriter) CreateSnapshot(cache *storage.Cache) error {

	snapshot := sw.copyFromCache(cache)

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize snapshot: %w", err)
	}
	snapshotPath := filepath.Join(sw.config.Directory, sw.config.Filename)
	tempPath := snapshotPath + ".tmp"

	if err := ioutil.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary snapshot file: %w", err)
	}

	if err := os.Rename(tempPath, snapshotPath); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to rename snapshot file: %w", err)
	}

	return nil
}

func (sw *SnapshotWriter) copyFromCache(cache *storage.Cache) *Snapshot {
	rawEntries := cache.GetSnapshotEntries()

	entries := make(map[string]SnapshotEntry)
	for key, rawEntry := range rawEntries {
		if entryMap, ok := rawEntry.(map[string]interface{}); ok {
			value := ""
			if v, ok := entryMap["value"].(string); ok {
				value = v
			}
			expiresAt := time.Time{}
			if et, ok := entryMap["expires_at"].(time.Time); ok {
				expiresAt = et
			}
			entries[key] = SnapshotEntry{
				Value:     value,
				ExpiresAt: expiresAt,
			}
		}
	}

	return &Snapshot{
		CreatedAt: time.Now(),
		Entries:   entries,
	}
}

func (sw *SnapshotWriter) GetSnapshotPath() string {
	return filepath.Join(sw.config.Directory, sw.config.Filename)
}

type SnapshotReader struct {
	mu     sync.RWMutex
	config SnapshotConfig
}

func NewSnapshotReader(config SnapshotConfig) *SnapshotReader {
	return &SnapshotReader{
		config: config,
	}
}

func (sr *SnapshotReader) LoadSnapshot() (*Snapshot, error) {
	snapshotPath := filepath.Join(sr.config.Directory, sr.config.Filename)

	if _, err := os.Stat(snapshotPath); err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No snapshot exists yet
		}
		return nil, fmt.Errorf("failed to stat snapshot file: %w", err)
	}

	data, err := ioutil.ReadFile(snapshotPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot file: %w", err)
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("failed to deserialize snapshot: %w", err)
	}

	return &snapshot, nil
}

func (sr *SnapshotReader) RestoreSnapshot(snapshot *Snapshot, cache *storage.Cache) (int, error) {
	if snapshot == nil {
		return 0, nil
	}

	now := time.Now()
	restoredCount := 0

	for key, entry := range snapshot.Entries {
		if !entry.ExpiresAt.IsZero() && now.After(entry.ExpiresAt) {
			continue
		}

		var ttl int
		if !entry.ExpiresAt.IsZero() {
			ttl = int(entry.ExpiresAt.Sub(now).Seconds())
			if ttl < 0 {
				ttl = 0
			}
		}

		cache.RestoreEntry(key, entry.Value, ttl, entry.ExpiresAt)
		restoredCount++
	}

	return restoredCount, nil
}
