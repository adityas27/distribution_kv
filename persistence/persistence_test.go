package persistence

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"tcp_test/storage"
	"tcp_test/wal"
)

// TestSnapshotCreation tests basic snapshot creation functionality.
func TestSnapshotCreation(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	cache := storage.NewCache()
	defer func() {
		if cache != nil {
			// Clean up cache if needed
		}
	}()

	// Add some test data
	cache.Set("key1", "value1", 3600)
	cache.Set("key2", "value2", 3600)
	cache.Set("key3", "value3", 0) // No TTL

	// Create snapshot
	config := SnapshotConfig{
		Directory: tmpDir,
		Filename:  "test_snapshot.json",
	}
	writer := NewSnapshotWriter(config)

	err := writer.CreateSnapshot(cache)
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	// Verify snapshot file exists
	snapshotPath := filepath.Join(tmpDir, "test_snapshot.json")
	if _, err := os.Stat(snapshotPath); err != nil {
		t.Fatalf("snapshot file not created: %v", err)
	}

	// Verify snapshot content
	data, err := ioutil.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("failed to read snapshot: %v", err)
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatalf("failed to unmarshal snapshot: %v", err)
	}

	// Check that all keys are present
	if len(snapshot.Entries) < 3 {
		t.Errorf("expected at least 3 entries in snapshot, got %d", len(snapshot.Entries))
	}

	if _, ok := snapshot.Entries["key1"]; !ok {
		t.Errorf("key1 not found in snapshot")
	}
	if _, ok := snapshot.Entries["key2"]; !ok {
		t.Errorf("key2 not found in snapshot")
	}
	if _, ok := snapshot.Entries["key3"]; !ok {
		t.Errorf("key3 not found in snapshot")
	}

	// Verify entry values
	if snapshot.Entries["key1"].Value != "value1" {
		t.Errorf("key1 value mismatch: got %q", snapshot.Entries["key1"].Value)
	}
}

// TestSnapshotAtomicWrite tests that snapshot writes are atomic.
func TestSnapshotAtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()
	cache := storage.NewCache()
	defer func() {
		if cache != nil {
			// Clean up cache if needed
		}
	}()

	cache.Set("key1", "value1", 3600)

	config := SnapshotConfig{
		Directory: tmpDir,
		Filename:  "atomic_snapshot.json",
	}
	writer := NewSnapshotWriter(config)

	err := writer.CreateSnapshot(cache)
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	// Verify no temporary file is left
	files, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to list directory: %v", err)
	}

	for _, f := range files {
		if filepath.Ext(f.Name()) == ".tmp" {
			t.Errorf("found temporary file: %s", f.Name())
		}
	}
}

// TestSnapshotLoading tests loading a snapshot from disk.
func TestSnapshotLoading(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a snapshot file manually
	snapshot := Snapshot{
		CreatedAt: time.Now(),
		Entries: map[string]SnapshotEntry{
			"key1": {
				Value:     "value1",
				ExpiresAt: time.Now().Add(1 * time.Hour),
			},
			"key2": {
				Value:     "value2",
				ExpiresAt: time.Time{}, // No expiration
			},
		},
	}

	data, _ := json.Marshal(snapshot)
	snapshotPath := filepath.Join(tmpDir, "test_snapshot.json")
	ioutil.WriteFile(snapshotPath, data, 0644)

	// Load snapshot
	config := SnapshotConfig{
		Directory: tmpDir,
		Filename:  "test_snapshot.json",
	}
	reader := NewSnapshotReader(config)

	loaded, err := reader.LoadSnapshot()
	if err != nil {
		t.Fatalf("failed to load snapshot: %v", err)
	}

	if loaded == nil {
		t.Fatal("snapshot is nil")
	}

	if len(loaded.Entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(loaded.Entries))
	}

	if loaded.Entries["key1"].Value != "value1" {
		t.Errorf("key1 value mismatch")
	}
}

// TestSnapshotLoadingNonExistent tests loading when snapshot doesn't exist.
func TestSnapshotLoadingNonExistent(t *testing.T) {
	tmpDir := t.TempDir()

	config := SnapshotConfig{
		Directory: tmpDir,
		Filename:  "nonexistent.json",
	}
	reader := NewSnapshotReader(config)

	snapshot, err := reader.LoadSnapshot()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if snapshot != nil {
		t.Errorf("expected nil snapshot, got %+v", snapshot)
	}
}

// TestRestoreSnapshot tests restoring cache from snapshot.
func TestRestoreSnapshot(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test snapshot
	now := time.Now()
	snapshot := &Snapshot{
		CreatedAt: now,
		Entries: map[string]SnapshotEntry{
			"key1": {
				Value:     "value1",
				ExpiresAt: now.Add(1 * time.Hour),
			},
			"key2": {
				Value:     "value2",
				ExpiresAt: time.Time{}, // No expiration
			},
		},
	}

	// Restore to cache
	cache := storage.NewCache()
	defer func() {
		if cache != nil {
			// Clean up
		}
	}()

	config := SnapshotConfig{
		Directory: tmpDir,
		Filename:  "test_snapshot.json",
	}
	reader := NewSnapshotReader(config)

	restored, err := reader.RestoreSnapshot(snapshot, cache)
	if err != nil {
		t.Fatalf("failed to restore snapshot: %v", err)
	}

	if restored != 2 {
		t.Errorf("expected 2 entries restored, got %d", restored)
	}

	// Verify entries in cache
	val, ok := cache.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("key1 not properly restored")
	}

	val, ok = cache.Get("key2")
	if !ok || val != "value2" {
		t.Errorf("key2 not properly restored")
	}
}

// TestRestoreSnapshotSkipsExpired tests that expired entries are skipped during restore.
func TestRestoreSnapshotSkipsExpired(t *testing.T) {
	tmpDir := t.TempDir()

	// Create snapshot with mixed expiration times
	now := time.Now()
	snapshot := &Snapshot{
		CreatedAt: now,
		Entries: map[string]SnapshotEntry{
			"valid_key": {
				Value:     "value1",
				ExpiresAt: now.Add(1 * time.Hour), // Future
			},
			"expired_key": {
				Value:     "value2",
				ExpiresAt: now.Add(-1 * time.Hour), // Past (expired)
			},
			"no_ttl_key": {
				Value:     "value3",
				ExpiresAt: time.Time{}, // No TTL
			},
		},
	}

	cache := storage.NewCache()
	defer func() {
		if cache != nil {
			// Clean up
		}
	}()

	config := SnapshotConfig{
		Directory: tmpDir,
		Filename:  "test_snapshot.json",
	}
	reader := NewSnapshotReader(config)

	restored, err := reader.RestoreSnapshot(snapshot, cache)
	if err != nil {
		t.Fatalf("failed to restore snapshot: %v", err)
	}

	// Only 2 entries should be restored (valid_key and no_ttl_key)
	if restored != 2 {
		t.Errorf("expected 2 entries restored, got %d", restored)
	}

	// Verify valid_key exists
	val, ok := cache.Get("valid_key")
	if !ok || val != "value1" {
		t.Errorf("valid_key not properly restored")
	}

	// Verify expired_key does not exist
	val, ok = cache.Get("expired_key")
	if ok {
		t.Errorf("expired_key should not be in cache")
	}

	// Verify no_ttl_key exists
	val, ok = cache.Get("no_ttl_key")
	if !ok || val != "value3" {
		t.Errorf("no_ttl_key not properly restored")
	}
}

// TestRecoveryFromSnapshot tests recovery with snapshot only.
func TestRecoveryFromSnapshot(t *testing.T) {
	tmpDir := t.TempDir()

	// Create snapshot
	now := time.Now()
	snapshot := &Snapshot{
		CreatedAt: now,
		Entries: map[string]SnapshotEntry{
			"key1": {
				Value:     "value1",
				ExpiresAt: now.Add(1 * time.Hour),
			},
			"key2": {
				Value:     "value2",
				ExpiresAt: time.Time{},
			},
		},
	}

	// Save snapshot to disk
	data, _ := json.Marshal(snapshot)
	snapshotPath := filepath.Join(tmpDir, "snapshot.json")
	ioutil.WriteFile(snapshotPath, data, 0644)

	// Create cache and recover
	cache := storage.NewCache()
	defer func() {
		if cache != nil {
			// Clean up
		}
	}()

	config := SnapshotConfig{
		Directory: tmpDir,
		Filename:  "snapshot.json",
	}
	walPath := filepath.Join(tmpDir, "wal.log")
	recovery := NewRecoveryManager(config, walPath)

	stats, err := recovery.Recover(cache)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if stats.SnapshotEntriesRestored != 2 {
		t.Errorf("expected 2 snapshot entries restored, got %d", stats.SnapshotEntriesRestored)
	}

	// Verify entries
	val, ok := cache.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("key1 not properly recovered")
	}
}

// TestRecoveryWithWAL tests recovery with snapshot and WAL replay.
func TestRecoveryWithWAL(t *testing.T) {
	tmpDir := t.TempDir()

	// Create initial snapshot
	now := time.Now()
	snapshot := &Snapshot{
		CreatedAt: now,
		Entries: map[string]SnapshotEntry{
			"key1": {
				Value:     "value1",
				ExpiresAt: now.Add(1 * time.Hour),
			},
		},
	}

	data, _ := json.Marshal(snapshot)
	snapshotPath := filepath.Join(tmpDir, "snapshot.json")
	ioutil.WriteFile(snapshotPath, data, 0644)

	// Create WAL with new entries after snapshot
	walPath := filepath.Join(tmpDir, "wal.log")
	walFile, _ := os.Create(walPath)

	// Write WAL entries (after snapshot creation time)
	walEntries := []wal.WALEntry{
		{
			Op:        wal.SET,
			Key:       "key2",
			Value:     "value2",
			TTL:       3600,
			Timestamp: now.Add(1 * time.Second).Unix(),
		},
		{
			Op:        wal.SET,
			Key:       "key3",
			Value:     "value3",
			TTL:       0,
			Timestamp: now.Add(2 * time.Second).Unix(),
		},
	}

	for _, entry := range walEntries {
		entryData, _ := json.Marshal(entry)
		walFile.Write(append(entryData, '\n'))
	}
	walFile.Close()

	// Recover
	cache := storage.NewCache()
	defer func() {
		if cache != nil {
			// Clean up
		}
	}()

	config := SnapshotConfig{
		Directory: tmpDir,
		Filename:  "snapshot.json",
	}
	recovery := NewRecoveryManager(config, walPath)

	stats, err := recovery.Recover(cache)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if stats.SnapshotEntriesRestored != 1 {
		t.Errorf("expected 1 snapshot entry restored, got %d", stats.SnapshotEntriesRestored)
	}

	if stats.WALEntriesReplayed != 2 {
		t.Errorf("expected 2 WAL entries replayed, got %d", stats.WALEntriesReplayed)
	}

	// Verify all entries exist
	val, ok := cache.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("key1 not in cache after recovery")
	}

	val, ok = cache.Get("key2")
	if !ok || val != "value2" {
		t.Errorf("key2 not in cache after recovery")
	}

	val, ok = cache.Get("key3")
	if !ok || val != "value3" {
		t.Errorf("key3 not in cache after recovery")
	}
}

// TestSnapshotManager tests background snapshot creation.
func TestSnapshotManager(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")

	// Create cache
	cache := storage.NewCache()
	defer func() {
		if cache != nil {
			// Clean up
		}
	}()

	// Create WAL
	walFile, _ := wal.NewWAL(walPath)
	defer walFile.Close()

	// Create manager with very short interval for testing
	config := SnapshotManagerConfig{
		SnapshotInterval: 100 * time.Millisecond,
		SnapshotDir:      tmpDir,
		SnapshotFilename: "snapshot.json",
		WALPath:          walPath,
	}
	manager := NewSnapshotManager(cache, walFile, config)

	// Start manager
	if err := manager.Start(); err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}

	// Add data to cache
	cache.Set("key1", "value1", 3600)

	// Wait for snapshot to be created
	time.Sleep(300 * time.Millisecond)

	// Stop manager
	if err := manager.Stop(); err != nil {
		t.Fatalf("failed to stop manager: %v", err)
	}

	// Verify snapshot was created
	snapshotPath := filepath.Join(tmpDir, "snapshot.json")
	if _, err := os.Stat(snapshotPath); err != nil {
		t.Fatalf("snapshot file not created: %v", err)
	}

	// Verify snapshot count
	count := manager.GetSnapshotCount()
	if count < 1 {
		t.Errorf("expected at least 1 snapshot, got %d", count)
	}
}

// TestSnapshotManagerInterval tests changing snapshot interval.
func TestSnapshotManagerInterval(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")

	cache := storage.NewCache()
	defer func() {
		if cache != nil {
			// Clean up
		}
	}()

	walFile, _ := wal.NewWAL(walPath)
	defer walFile.Close()

	config := SnapshotManagerConfig{
		SnapshotInterval: 1 * time.Second,
		SnapshotDir:      tmpDir,
		SnapshotFilename: "snapshot.json",
		WALPath:          walPath,
	}
	manager := NewSnapshotManager(cache, walFile, config)

	// Test SetInterval
	newInterval := 500 * time.Millisecond
	if err := manager.SetInterval(newInterval); err != nil {
		t.Fatalf("failed to set interval: %v", err)
	}

	// Test invalid interval
	err := manager.SetInterval(0)
	if err == nil {
		t.Errorf("expected error for invalid interval, got nil")
	}
}

// TestSnapshotManagerCreateNow tests immediate snapshot creation.
func TestSnapshotManagerCreateNow(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")

	cache := storage.NewCache()
	defer func() {
		if cache != nil {
			// Clean up
		}
	}()

	cache.Set("key1", "value1", 3600)

	walFile, _ := wal.NewWAL(walPath)
	defer walFile.Close()

	config := SnapshotManagerConfig{
		SnapshotInterval: 10 * time.Second,
		SnapshotDir:      tmpDir,
		SnapshotFilename: "snapshot.json",
		WALPath:          walPath,
	}
	manager := NewSnapshotManager(cache, walFile, config)

	// Start manager
	if err := manager.Start(); err != nil {
		t.Fatalf("failed to start manager: %v", err)
	}
	defer manager.Stop()

	// Create snapshot immediately
	if err := manager.CreateSnapshotNow(); err != nil {
		t.Fatalf("failed to create snapshot now: %v", err)
	}

	// Verify snapshot was created
	snapshotPath := filepath.Join(tmpDir, "snapshot.json")
	if _, err := os.Stat(snapshotPath); err != nil {
		t.Fatalf("snapshot file not created: %v", err)
	}
}

// TestSnapshotManagerNotRunning tests CreateSnapshotNow when manager is not running.
func TestSnapshotManagerNotRunning(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")

	cache := storage.NewCache()
	defer func() {
		if cache != nil {
			// Clean up
		}
	}()

	walFile, _ := wal.NewWAL(walPath)
	defer walFile.Close()

	config := SnapshotManagerConfig{
		SnapshotInterval: 10 * time.Second,
		SnapshotDir:      tmpDir,
		SnapshotFilename: "snapshot.json",
		WALPath:          walPath,
	}
	manager := NewSnapshotManager(cache, walFile, config)

	// Try to create snapshot without starting manager
	err := manager.CreateSnapshotNow()
	if err == nil {
		t.Errorf("expected error when manager not running")
	}
}
