package persistence

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"tcp_test/storage"
	"tcp_test/wal"
)

type RecoveryManager struct {
	config  SnapshotConfig
	walPath string
}

func NewRecoveryManager(config SnapshotConfig, walPath string) *RecoveryManager {
	return &RecoveryManager{
		config:  config,
		walPath: walPath,
	}
}

type RecoveryStats struct {
	SnapshotEntriesRestored int
	WALEntriesReplayed      int
	ExpiredEntriesSkipped   int
	RecoveryTime            time.Duration
}

func (rm *RecoveryManager) Recover(cache *storage.Cache) (*RecoveryStats, error) {
	start := time.Now()

	stats := &RecoveryStats{}

	path := filepath.Join(rm.config.Directory, rm.config.Filename)

	var snapshot Snapshot

	data, err := os.ReadFile(path)
	if err == nil {
		if err := json.Unmarshal(data, &snapshot); err != nil {
			return nil, err
		}

		now := time.Now()

		for _, entry := range snapshot.Entries {
			if !entry.ExpiresAt.IsZero() && now.After(entry.ExpiresAt) {
				stats.ExpiredEntriesSkipped++
				continue
			}

			ttl := 0
			if !entry.ExpiresAt.IsZero() {
				ttl = int(entry.ExpiresAt.Sub(now).Seconds())
			}

			cache.RestoreEntry(
				entry.Key,
				entry.Value,
				ttl,
				entry.ExpiresAt,
			)

			stats.SnapshotEntriesRestored++
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	walStats, err := rm.replayWAL(cache, snapshot.CreatedAt)
	if err != nil {
		return nil, err
	}

	stats.WALEntriesReplayed = walStats.EntriesReplayed
	stats.ExpiredEntriesSkipped += walStats.ExpiredEntriesSkipped
	stats.RecoveryTime = time.Since(start)

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
			return stats, nil
		}
		return stats, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	now := time.Now()

	for scanner.Scan() {
		var entry wal.WALEntry

		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}

		entryTime := time.Unix(entry.Timestamp, 0)

		if !snapshotTime.IsZero() && entryTime.Before(snapshotTime) {
			continue
		}

		switch entry.Op {

		case wal.SET:
			expiresAt := time.Time{}

			if entry.TTL > 0 {
				expiresAt = entryTime.Add(time.Duration(entry.TTL) * time.Second)

				if now.After(expiresAt) {
					stats.ExpiredEntriesSkipped++
					continue
				}
			}

			ttl := 0

			if !expiresAt.IsZero() {
				ttl = int(expiresAt.Sub(now).Seconds())
			}

			cache.RestoreEntry(
				entry.Key,
				entry.Value,
				ttl,
				expiresAt,
			)

			stats.EntriesReplayed++

		case wal.DELETE:
			cache.RestoreDelete(entry.Key)
			stats.EntriesReplayed++
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return stats, nil
}

func (rm *RecoveryManager) RotateWAL(w *wal.WAL) error {
	if err := w.Close(); err != nil {
		return err
	}

	return os.Remove(rm.walPath)
}

func (rm *RecoveryManager) CreateNewWALFile() (*wal.WAL, error) {
	return wal.NewWAL(rm.walPath)
}