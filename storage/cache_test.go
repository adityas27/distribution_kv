package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	cache := NewCache()

	if err := cache.Set("name", "alice", 0); err != nil {
		t.Fatal(err)
	}

	val, ok := cache.Get("name")

	if !ok {
		t.Fatal("expected key to exist")
	}

	if val != "alice" {
		t.Fatalf("expected alice got %s", val)
	}
}

func TestOverwriteKey(t *testing.T) {
	cache := NewCache()

	_ = cache.Set("k", "one", 0)
	_ = cache.Set("k", "two", 0)

	val, _ := cache.Get("k")

	if val != "two" {
		t.Fatal("overwrite failed")
	}
}

func TestGetMissingKey(t *testing.T) {
	cache := NewCache()

	_, ok := cache.Get("missing")

	if ok {
		t.Fatal("expected missing key")
	}
}

func TestDelete(t *testing.T) {
	cache := NewCache()

	_ = cache.Set("k", "v", 0)

	if err := cache.Delete("k"); err != nil {
		t.Fatal(err)
	}

	_, ok := cache.Get("k")

	if ok {
		t.Fatal("delete failed")
	}
}

func TestDeleteMissingKey(t *testing.T) {
	cache := NewCache()

	if err := cache.Delete("missing"); err != nil {
		t.Fatal(err)
	}
}

func TestTTLExpiration(t *testing.T) {
	cache := NewCache()

	_ = cache.Set("k", "v", 1)

	time.Sleep(2 * time.Second)

	_, ok := cache.Get("k")

	if ok {
		t.Fatal("expected expired key")
	}
}

func TestTTLNoExpiration(t *testing.T) {
	cache := NewCache()

	_ = cache.Set("k", "v", 5)

	time.Sleep(time.Second)

	_, ok := cache.Get("k")

	if !ok {
		t.Fatal("key expired too early")
	}
}

func TestLRUEviction(t *testing.T) {
	cache := NewCache()

	for i := 0; i < KeysLimit+1; i++ {
		key := fmt.Sprintf("k%d", i)
		_ = cache.Set(key, "v", 0)
	}

	_, ok := cache.Get("k0")

	if ok {
		t.Fatal("expected oldest key to be evicted")
	}
}

func TestStats(t *testing.T) {
	cache := NewCache()

	_ = cache.Set("a", "1", 0)
	_ = cache.Set("b", "2", 0)

	stats := cache.Stats()

	if stats["items"].(int) != 2 {
		t.Fatal("stats incorrect")
	}
}

func TestSnapshotEntries(t *testing.T) {
	cache := NewCache()

	_ = cache.Set("a", "1", 0)
	_ = cache.Set("b", "2", 0)

	entries := cache.SnapshotEntries()

	if len(entries) != 2 {
		t.Fatal("snapshot entries incorrect")
	}
}

func TestRestoreEntry(t *testing.T) {
	cache := NewCache()

	cache.RestoreEntry("x", "100", 0, time.Time{})

	val, ok := cache.Get("x")

	if !ok || val != "100" {
		t.Fatal("restore failed")
	}
}

func TestRestoreDelete(t *testing.T) {
	cache := NewCache()

	cache.RestoreEntry("x", "100", 0, time.Time{})
	cache.RestoreDelete("x")

	_, ok := cache.Get("x")

	if ok {
		t.Fatal("restore delete failed")
	}
}

func TestConcurrentSet(t *testing.T) {
	cache := NewCache()

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("k%d", i)
			_ = cache.Set(key, "v", 0)
		}(i)
	}

	wg.Wait()
}

func TestConcurrentGet(t *testing.T) {
	cache := NewCache()

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("k%d", i)
		_ = cache.Set(key, "v", 0)
	}

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("k%d", i)
			cache.Get(key)
		}(i)
	}

	wg.Wait()
}

func TestConcurrentSetGet(t *testing.T) {
	cache := NewCache()

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {

		wg.Add(2)

		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("k%d", i)
			_ = cache.Set(key, "v", 0)
		}(i)

		go func(i int) {
			defer wg.Done()

			key := fmt.Sprintf("k%d", i)
			cache.Get(key)
		}(i)
	}

	wg.Wait()
}

func TestCurrentMemory(t *testing.T) {
	cache := NewCache()

	_ = cache.Set("abc", "123", 0)

	if cache.currentMemory == 0 {
		t.Fatal("memory accounting failed")
	}
}