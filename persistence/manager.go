package persistence

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"tcp_test/storage"
	"tcp_test/wal"
)

// SnapshotManager manages periodic snapshot creation and WAL rotation.
type SnapshotManager struct {
	mu                     sync.RWMutex
	cache                  *storage.Cache
	walInstance            *wal.WAL
	snapshotWriter         *SnapshotWriter
	recoveryManager        *RecoveryManager
	interval               time.Duration
	done                   chan struct{}
	ticker                 *time.Ticker
	isRunning              int32
	lastSnapshotTime       time.Time
	lastSnapshotError      error
	snapshotCount          int64
	lastSuccessfulSnapshot time.Time
}

// SnapshotManagerConfig holds configuration for the snapshot manager.
type SnapshotManagerConfig struct {
	SnapshotInterval time.Duration
	SnapshotDir      string
	SnapshotFilename string
	WALPath          string
}

// DefaultSnapshotManagerConfig returns default configuration.
func DefaultSnapshotManagerConfig() SnapshotManagerConfig {
	return SnapshotManagerConfig{
		SnapshotInterval: 5 * time.Minute,
		SnapshotDir:      ".",
		SnapshotFilename: "snapshot.json",
		WALPath:          "wal.log",
	}
}

// NewSnapshotManager creates a new snapshot manager with the given configuration.
func NewSnapshotManager(
	cache *storage.Cache,
	walInstance *wal.WAL,
	config SnapshotManagerConfig,
) *SnapshotManager {
	snapshotConfig := SnapshotConfig{
		Directory: config.SnapshotDir,
		Filename:  config.SnapshotFilename,
	}

	return &SnapshotManager{
		cache:           cache,
		walInstance:     walInstance,
		snapshotWriter:  NewSnapshotWriter(snapshotConfig),
		recoveryManager: NewRecoveryManager(snapshotConfig, config.WALPath),
		interval:        config.SnapshotInterval,
		done:            make(chan struct{}),
		snapshotCount:   0,
	}
}

func (sm *SnapshotManager) Start() error {
	if !atomic.CompareAndSwapInt32(&sm.isRunning, 0, 1) {
		return fmt.Errorf("snapshot manager is already running")
	}

	sm.ticker = time.NewTicker(sm.interval)

	go sm.backgroundWorker()

	return nil
}

func (sm *SnapshotManager) Stop() error {
	if !atomic.CompareAndSwapInt32(&sm.isRunning, 1, 0) {
		return fmt.Errorf("snapshot manager is not running")
	}

	sm.ticker.Stop()
	close(sm.done)

	return nil
}

// backgroundWorker runs the periodic snapshot creation loop
func (sm *SnapshotManager) backgroundWorker() {
	for {
		select {
		case <-sm.done:
			return
		case <-sm.ticker.C:
			sm.createSnapshot()
		}
	}
}


func (sm *SnapshotManager) createSnapshot() {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	start := time.Now()

	// Create snapshot
	if err := sm.snapshotWriter.CreateSnapshot(sm.cache); err != nil {
		sm.lastSnapshotError = err
		fmt.Printf("failed to create snapshot: %v\n", err)
		return
	}

	// Snapshot created successfully, now rotate WAL
	if err := sm.recoveryManager.RotateWAL(sm.walInstance); err != nil {
		sm.lastSnapshotError = err
		fmt.Printf("failed to rotate WAL: %v\n", err)
		// Don't return - we may have created the snapshot but failed to rotate
	}

	// Create fresh WAL file
	newWAL, err := sm.recoveryManager.CreateNewWALFile()
	if err != nil {
		sm.lastSnapshotError = err
		fmt.Printf("failed to create new WAL: %v\n", err)
		return
	}

	// Update the WAL instance in the cache
	sm.walInstance = newWAL
	sm.cache.SetWAL(newWAL)

	// Update tracking information
	sm.lastSnapshotTime = start
	sm.lastSuccessfulSnapshot = time.Now()
	sm.lastSnapshotError = nil
	atomic.AddInt64(&sm.snapshotCount, 1)

	duration := time.Since(start)
	fmt.Printf("snapshot created successfully in %v (total: %d)\n", duration, atomic.LoadInt64(&sm.snapshotCount))
}

// GetLastSnapshotTime returns the time of the last snapshot attempt.
func (sm *SnapshotManager) GetLastSnapshotTime() time.Time {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastSnapshotTime
}

// GetLastSnapshotError returns the last error encountered during snapshotting.
func (sm *SnapshotManager) GetLastSnapshotError() error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastSnapshotError
}

// GetSnapshotCount returns the total number of successful snapshots.
func (sm *SnapshotManager) GetSnapshotCount() int64 {
	return atomic.LoadInt64(&sm.snapshotCount)
}

// GetLastSuccessfulSnapshot returns the time of the last successful snapshot.
func (sm *SnapshotManager) GetLastSuccessfulSnapshot() time.Time {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastSuccessfulSnapshot
}

// IsRunning returns true if the snapshot manager is currently running.
func (sm *SnapshotManager) IsRunning() bool {
	return atomic.LoadInt32(&sm.isRunning) == 1
}

// SetInterval updates the snapshot interval.
// Takes effect after the next snapshot cycle.
func (sm *SnapshotManager) SetInterval(interval time.Duration) error {
	if interval < 1*time.Second {
		return fmt.Errorf("interval must be at least 1 second")
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.interval = interval
	if sm.ticker != nil {
		sm.ticker.Reset(interval)
	}

	return nil
}

// CreateSnapshotNow triggers an immediate snapshot.
// Useful for testing or manual persistence points.
func (sm *SnapshotManager) CreateSnapshotNow() error {
	if !sm.IsRunning() {
		return fmt.Errorf("snapshot manager is not running")
	}

	sm.createSnapshot()
	return sm.lastSnapshotError
}
