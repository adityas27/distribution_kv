package storage

import (
	"container/list"
	"sync"
	"time"

	"tcp_test/wal"
)

const MemoryLimit = 1024 * 1024 * 512
const KeysLimit = 100

type Entry struct {
	Value     string
	ExpiresAt time.Time
	Size      int
	node      *list.Element
}

type Cache struct {
	mu            sync.RWMutex
	items         map[string]*Entry
	lru           *list.List
	currentMemory int

	wal *wal.WAL
}
type SnapshotEntry struct {
	Key       string
	Value     string
	ExpiresAt time.Time
}

func (c *Cache) SnapshotEntries() []SnapshotEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entries := make([]SnapshotEntry, 0, len(c.items))

	for key, entry := range c.items {
		entries = append(entries, SnapshotEntry{
			Key:       key,
			Value:     entry.Value,
			ExpiresAt: entry.ExpiresAt,
		})
	}

	return entries
}

func NewCache() *Cache {
	w, err := wal.NewWAL("wal.log")
	if err != nil {
		panic(err)
	}

	c := &Cache{
		items: make(map[string]*Entry),
		lru:   list.New(),
		wal:   w,
	}

	go c.cleanupWorker()
	go c.evictionWorker()

	return c
}

func (c *Cache) Set(key, value string, ttl int) {
	if err := c.wal.SetWAL(key, value, int64(ttl)); err != nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.items[key]; ok {
		c.currentMemory -= existing.Size
		c.lru.Remove(existing.node)
		delete(c.items, key)
	}

	entry := &Entry{
		Value: value,
		Size:  len(key) + len(value),
	}

	if ttl > 0 {
		entry.ExpiresAt = time.Now().Add(time.Duration(ttl) * time.Second)
	}

	// adds new item to top of the list : most recently used
	entry.node = c.lru.PushFront(key)
	c.items[key] = entry
	c.currentMemory += entry.Size

	c.evictIfNeeded() // evict as needed
}

func (c *Cache) Get(key string) (string, bool) {
	c.mu.RLock()
	entry, ok := c.items[key]
	c.mu.RUnlock()

	if !ok {
		return "", false
	}

	if !entry.ExpiresAt.IsZero() &&
		time.Now().After(entry.ExpiresAt) {

		c.mu.Lock()
		c.currentMemory -= entry.Size
		c.lru.Remove(entry.node)
		delete(c.items, key)
		c.mu.Unlock()
		return "", false
	}

	//update LRU order on access
	c.mu.Lock()
	c.lru.MoveToFront(entry.node)
	c.mu.Unlock()

	return entry.Value, true
}

func (c *Cache) Delete(key string) {
	if err := c.wal.DeleteWAL(key); err != nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.items[key]; ok {
		c.currentMemory -= entry.Size
		c.lru.Remove(entry.node)
		delete(c.items, key)
	}
}

// removes least recently used items when limits are exceeded
func (c *Cache) evictIfNeeded() {
	for {
		if len(c.items) > KeysLimit || c.currentMemory > MemoryLimit {
			if elem := c.lru.Back(); elem != nil {
				key := elem.Value.(string)
				entry := c.items[key]
				c.currentMemory -= entry.Size
				c.lru.Remove(elem)
				delete(c.items, key)
			} else {
				break
			}
		} else {
			break
		}
	}
}

// evictionWorker periodically checks and evicts if needed
func (c *Cache) evictionWorker() {
	ticker := time.NewTicker(10 * time.Second)

	for range ticker.C {
		c.mu.Lock()
		c.evictIfNeeded()
		c.mu.Unlock()
	}
}

func (c *Cache) cleanupWorker() {
	ticker := time.NewTicker(30 * time.Second)

	for range ticker.C {
		now := time.Now()
		c.mu.Lock()

		var keysToDelete []string
		for key, entry := range c.items {
			if !entry.ExpiresAt.IsZero() &&
				now.After(entry.ExpiresAt) {
				keysToDelete = append(keysToDelete, key)
			}
		}

		for _, key := range keysToDelete {
			entry := c.items[key]
			c.currentMemory -= entry.Size
			c.lru.Remove(entry.node)
			delete(c.items, key)
		}

		c.mu.Unlock()
	}
}

// statistic feature
func (c *Cache) Stats() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return map[string]interface{}{
		"items":        len(c.items),
		"memory_used":  c.currentMemory,
		"memory_limit": MemoryLimit,
		"keys_limit":   KeysLimit,
	}
}

// GetSnapshotEntries returns a copy of all cache entries for snapshot creation.
// Holds a read lock only while copying the entries to minimize contention.
func (c *Cache) GetSnapshotEntries() map[string]interface{} {
	entries := make(map[string]interface{})

	c.mu.RLock()
	defer c.mu.RUnlock()

	for key, entry := range c.items {
		entries[key] = map[string]interface{}{
			"value":      entry.Value,
			"expires_at": entry.ExpiresAt,
		}
	}

	return entries
}

// RestoreEntry restores an entry to the cache without logging to WAL.
// Used during recovery from snapshots or WAL replay.
// Does not update LRU order to avoid interference with recovery.
func (c *Cache) RestoreEntry(key, value string, ttl int, expiresAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove old entry if it exists
	if existing, ok := c.items[key]; ok {
		c.currentMemory -= existing.Size
		c.lru.Remove(existing.node)
		delete(c.items, key)
	}

	// Create new entry
	entry := &Entry{
		Value:     value,
		ExpiresAt: expiresAt,
		Size:      len(key) + len(value),
	}

	// Add to LRU list
	entry.node = c.lru.PushFront(key)
	c.items[key] = entry
	c.currentMemory += entry.Size

	// Evict if needed
	c.evictIfNeeded()
}

// RestoreDelete deletes an entry from the cache without logging to WAL.
// Used during WAL replay for DELETE operations.
func (c *Cache) RestoreDelete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.items[key]; ok {
		c.currentMemory -= entry.Size
		c.lru.Remove(entry.node)
		delete(c.items, key)
	}
}

// SetWAL updates the WAL instance, used when rotating WAL files.
func (c *Cache) SetWAL(newWAL *wal.WAL) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wal = newWAL
}
