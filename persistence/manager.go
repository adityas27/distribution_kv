package persistence

import (
	"fmt"
	"sync/atomic"
	"time"

	"tcp_test/storage"
	"tcp_test/wal"
)

type SnapshotManager struct {
	cache          *storage.Cache
	walInstance    *wal.WAL
	snapshotWriter *SnapshotWriter
	interval       time.Duration
	ticker         *time.Ticker
	done           chan struct{}
	isRunning      int32
}

type SnapshotManagerConfig struct {
	SnapshotInterval time.Duration
	SnapshotDir      string
	SnapshotFilename string
}

func DefaultSnapshotManagerConfig() SnapshotManagerConfig {
	return SnapshotManagerConfig{
		SnapshotInterval: 5 * time.Minute,
		SnapshotDir:      ".",
		SnapshotFilename: "snapshot.json",
	}
}

func NewSnapshotManager(
	cache *storage.Cache,
	walInstance *wal.WAL,
	config SnapshotManagerConfig,
) *SnapshotManager {

	snapshotConfig := NewSnapshotConfig(
		config.SnapshotDir,
		config.SnapshotFilename,
	)

	return &SnapshotManager{
		cache:          cache,
		walInstance:    walInstance,
		snapshotWriter: NewSnapshotWriter(snapshotConfig),
		interval:       config.SnapshotInterval,
		done:           make(chan struct{}),
	}
}

func (sm *SnapshotManager) Start() error {
	if !atomic.CompareAndSwapInt32(&sm.isRunning, 0, 1) {
		return fmt.Errorf("snapshot manager already running")
	}

	sm.ticker = time.NewTicker(sm.interval)

	go func() {
		for {
			select {
			case <-sm.done:
				return

			case <-sm.ticker.C:
				_ = sm.snapshotWriter.CreateSnapshot(sm.cache)
			}
		}
	}()

	return nil
}

func (sm *SnapshotManager) Stop() error {
	if !atomic.CompareAndSwapInt32(&sm.isRunning, 1, 0) {
		return fmt.Errorf("snapshot manager not running")
	}

	sm.ticker.Stop()
	close(sm.done)

	return nil
}

func (sm *SnapshotManager) CreateSnapshotNow() error {
	return sm.snapshotWriter.CreateSnapshot(sm.cache)
}

func (sm *SnapshotManager) IsRunning() bool {
	return atomic.LoadInt32(&sm.isRunning) == 1
}