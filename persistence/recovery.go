package persistence

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"tcp_test/storage"
	"tcp_test/wal"
)
type RecoveryManager struct {
	mu             sync.RWMutex
	snapshotReader *SnapshotReader
	walPath        string
}

func NewRecoveryManager(config SnapshotConfig, walPath string) *RecoveryManager {
	return &RecoveryManager{
		snapshotReader: NewSnapshotReader(config),
		walPath:        walPath,
	}
}

// RecoveryStats contains statistics about the recovery process.
type RecoveryStats struct {
	SnapshotEntriesRestored int
	WALEntriesReplayed      int
	ExpiredEntriesSkipped   int
	RecoveryTime            time.Duration
	StartTime               time.Time
	EndTime                 time.Time
}

// 1. Load snapshot first
// 2. Replay WAL entries after snapshot timestamp
// 3. Final cache state matches pre-crash state
func (rm *RecoveryManager) Recover(cache *storage.Cache) (*RecoveryStats, error) {
	startTime := time.Now()
	stats := &RecoveryStats{
		StartTime: startTime,
	}

	// Step 1: Load snapshot
	snapshot, err := rm.snapshotReader.LoadSnapshot()
	if err != nil {
		return stats, fmt.Errorf("failed to load snapshot: %w", err)
	}

	// Step 2: Restore from snapshot (if snapshot exists)
	snapshotTime := time.Time{}
	if snapshot != nil {
		restored, err := rm.snapshotReader.RestoreSnapshot(snapshot, cache)
		if err != nil {
			return stats, fmt.Errorf("failed to restore snapshot: %w", err)
		}
		stats.SnapshotEntriesRestored = restored
		snapshotTime = snapshot.CreatedAt
	}

	// Step 3: Replay WAL entries after snapshot time
	walStats, err := rm.replayWAL(cache, snapshotTime)
	if err != nil {
		return stats, fmt.Errorf("failed to replay WAL: %w", err)
	}

	stats.WALEntriesReplayed = walStats.EntriesReplayed
	stats.ExpiredEntriesSkipped = walStats.ExpiredEntriesSkipped

	stats.EndTime = time.Now()
	stats.RecoveryTime = stats.EndTime.Sub(startTime)

	return stats, nil
}

type WALReplayStats struct {
	EntriesReplayed       int
	ExpiredEntriesSkipped int
}

func (rm *RecoveryManager) replayWAL(cache *storage.Cache, snapshotTime time.Time) (*WALReplayStats, error) {
	stats := &WALReplayStats{}

	file, err := os.Open(rm.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No WAL file exists yet, which is fine
			return stats, nil
		}
		return stats, fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	now := time.Now()

	// Read and replay each WAL entr
	for scanner.Scan() {
		var entry wal.WALEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			fmt.Printf("warning: failed to parse WAL entry: %v\n", err)
			continue
		}

		// Skip entries from before the snapshot
		entryTime := time.Unix(entry.Timestamp, 0)
		if !snapshotTime.IsZero() && entryTime.Before(snapshotTime) {
			continue
		}

		// Replay the operation
		switch entry.Op {
		case wal.SET:
			// Check if the entry has already expired
			expiresAt := time.Time{}
			if entry.TTL > 0 {
				expiresAt = time.Unix(entry.Timestamp, 0).Add(time.Duration(entry.TTL) * time.Second)
				if now.After(expiresAt) {
					stats.ExpiredEntriesSkipped++
					continue
				}
			}

			var remainingTTL int
			if !expiresAt.IsZero() {
				remainingTTL = int(expiresAt.Sub(now).Seconds())
				if remainingTTL < 0 {
					remainingTTL = 0
				}
			}

			cache.RestoreEntry(entry.Key, entry.Value, remainingTTL, expiresAt)
			stats.EntriesReplayed++

		case wal.DELETE:
			cache.RestoreDelete(entry.Key)
			stats.EntriesReplayed++
		}
	}

	if err := scanner.Err(); err != nil {
		return stats, fmt.Errorf("error reading WAL file: %w", err)
	}

	return stats, nil
}

// RotateWAL rotates the WAL file by closing the current one and creating a new one.
// This is typically called after a successful snapshot to truncate the WAL.
func (rm *RecoveryManager) RotateWAL(walInstance *wal.WAL) error {
	if err := walInstance.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	// Archive the old WAL file with timestamp
	timestamp := time.Now().Format("20060102_150405")
	oldPath := rm.walPath
	archivePath := rm.walPath + ".archive." + timestamp

	if err := os.Rename(oldPath, archivePath); err != nil {
		return fmt.Errorf("failed to archive WAL: %w", err)
	}

	return nil
}

func (rm *RecoveryManager) CreateNewWALFile() (*wal.WAL, error) {
	newWAL, err := wal.NewWAL(rm.walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create new WAL: %w", err)
	}
	return newWAL, nil
}
