package storage

import (
	"sync"
	"time"
)

type Entry struct {
	Value     string
	ExpiresAt time.Time
}

type Cache struct {
	mu    sync.RWMutex
	items map[string]Entry
}

func NewCache() *Cache {
	c := &Cache{
		items: make(map[string]Entry),
	}

	go c.cleanupWorker()

	return c
}

func (c *Cache) Set(key, value string, ttl int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry := Entry{
		Value: value,
	}

	if ttl > 0 {
		entry.ExpiresAt = time.Now().Add(time.Duration(ttl) * time.Second)
	}

	c.items[key] = entry
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
		delete(c.items, key)
		c.mu.Unlock()
		return "", false
	}

	return entry.Value, true
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

func (c *Cache) cleanupWorker() {
	ticker := time.NewTicker(30 * time.Second)

	for range ticker.C {
		now := time.Now()
		c.mu.Lock()

		for key, entry := range c.items {
			if !entry.ExpiresAt.IsZero() &&
				now.After(entry.ExpiresAt) {
				delete(c.items, key)
			}
		}

		c.mu.Unlock()
	}
}