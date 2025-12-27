package tile_proxy

import (
	"sync"
	"time"
)

// CacheItem 缓存项
type CacheItem struct {
	Data      []byte
	ExpiresAt time.Time
}

// TileCache 瓦片缓存
type TileCache struct {
	mu      sync.RWMutex
	items   map[string]*CacheItem
	maxSize int
	ttl     time.Duration
}

// NewTileCache 创建瓦片缓存
func NewTileCache(maxSize int, ttl time.Duration) *TileCache {
	cache := &TileCache{
		items:   make(map[string]*CacheItem),
		maxSize: maxSize,
		ttl:     ttl,
	}

	// 启动清理协程
	go cache.cleanupLoop()

	return cache
}

// Get 获取缓存
func (c *TileCache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.items[key]
	if !ok {
		return nil, false
	}

	if time.Now().After(item.ExpiresAt) {
		return nil, false
	}

	return item.Data, true
}

// Set 设置缓存
func (c *TileCache) Set(key string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果缓存已满，删除最旧的项
	if len(c.items) >= c.maxSize {
		c.evictOldest()
	}

	c.items[key] = &CacheItem{
		Data:      data,
		ExpiresAt: time.Now().Add(c.ttl),
	}
}

// evictOldest 删除最旧的缓存项
func (c *TileCache) evictOldest() {
	var oldestKey string
	var oldestTime time.Time

	for key, item := range c.items {
		if oldestKey == "" || item.ExpiresAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = item.ExpiresAt
		}
	}

	if oldestKey != "" {
		delete(c.items, oldestKey)
	}
}

// cleanupLoop 定期清理过期缓存
func (c *TileCache) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.cleanup()
	}
}

// cleanup 清理过期缓存
func (c *TileCache) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, item := range c.items {
		if now.After(item.ExpiresAt) {
			delete(c.items, key)
		}
	}
}

// Clear 清空缓存
func (c *TileCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]*CacheItem)
}

// Size 获取缓存大小
func (c *TileCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}
