// tile_cache.go
package tile_proxy

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// TileCache SQLite瓦片缓存
type TileCache struct {
	db         *sql.DB
	mu         sync.RWMutex
	ttl        time.Duration
	dbPath     string
	memCache   map[string]*MemCacheItem // 热点数据内存缓存
	memMu      sync.RWMutex
	memMaxSize int
}

// MemCacheItem 内存缓存项（用于热点数据）
type MemCacheItem struct {
	Data       []byte
	AccessTime time.Time
}

// NewTileCache 创建瓦片缓存
func NewTileCache(maxMemItems int, ttl time.Duration) *TileCache {
	// 确保缓存目录存在
	homeDir, _ := os.UserHomeDir()
	cacheDir := filepath.Join(homeDir, "BoundlessMap", "cache")
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		fmt.Printf("Warning: failed to create cache directory: %v\n", err)
	}

	dbPath := filepath.Join(cacheDir, "tile_cache.db")

	cache := &TileCache{
		ttl:        ttl,
		dbPath:     dbPath,
		memCache:   make(map[string]*MemCacheItem),
		memMaxSize: maxMemItems,
	}

	if err := cache.initDB(); err != nil {
		fmt.Printf("Warning: failed to init cache db: %v, using memory only\n", err)
		return cache
	}

	// 启动清理协程
	go cache.cleanupLoop()

	return cache
}

// initDB 初始化数据库
func (c *TileCache) initDB() error {
	var err error
	c.db, err = sql.Open("sqlite3", c.dbPath+"?cache=shared&mode=rwc&_journal_mode=WAL")
	if err != nil {
		return fmt.Errorf("open database failed: %v", err)
	}

	// 设置连接池
	c.db.SetMaxOpenConns(1) // SQLite 建议单连接
	c.db.SetMaxIdleConns(1)
	c.db.SetConnMaxLifetime(0)

	// 创建表
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS tile_cache (
		cache_key TEXT PRIMARY KEY,
		tile_data BLOB NOT NULL,
		content_type TEXT DEFAULT 'image/png',
		created_at INTEGER NOT NULL,
		expires_at INTEGER NOT NULL,
		access_count INTEGER DEFAULT 0,
		last_access INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_expires_at ON tile_cache(expires_at);
	CREATE INDEX IF NOT EXISTS idx_last_access ON tile_cache(last_access);
	`

	_, err = c.db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("create table failed: %v", err)
	}

	// 优化SQLite性能
	_, err = c.db.Exec(`
		PRAGMA synchronous = NORMAL;
		PRAGMA temp_store = MEMORY;
		PRAGMA mmap_size = 268435456;
	`)
	if err != nil {
		fmt.Printf("Warning: failed to set pragma: %v\n", err)
	}

	return nil
}

// Get 获取缓存
func (c *TileCache) Get(key string) ([]byte, bool) {
	// 先检查内存缓存
	c.memMu.RLock()
	if item, ok := c.memCache[key]; ok {
		c.memMu.RUnlock()
		// 更新访问时间
		c.memMu.Lock()
		item.AccessTime = time.Now()
		c.memMu.Unlock()
		return item.Data, true
	}
	c.memMu.RUnlock()

	// 从SQLite获取
	if c.db == nil {
		return nil, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	var data []byte
	var expiresAt int64

	err := c.db.QueryRow(
		"SELECT tile_data, expires_at FROM tile_cache WHERE cache_key = ?",
		key,
	).Scan(&data, &expiresAt)

	if err != nil {
		return nil, false
	}

	// 检查是否过期
	if time.Now().Unix() > expiresAt {
		// 异步删除过期数据
		go c.Delete(key)
		return nil, false
	}

	// 更新访问统计
	go c.updateAccessStats(key)

	// 添加到内存缓存
	c.addToMemCache(key, data)

	return data, true
}

// Set 设置缓存
func (c *TileCache) Set(key string, data []byte) {
	c.SetWithType(key, data, "image/png")
}

// SetWithType 设置缓存（带内容类型）
func (c *TileCache) SetWithType(key string, data []byte, contentType string) {
	if len(data) == 0 {
		return
	}

	// 添加到内存缓存
	c.addToMemCache(key, data)

	// 保存到SQLite
	if c.db == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().Unix()
	expiresAt := time.Now().Add(c.ttl).Unix()

	_, err := c.db.Exec(`
		INSERT OR REPLACE INTO tile_cache 
		(cache_key, tile_data, content_type, created_at, expires_at, access_count, last_access)
		VALUES (?, ?, ?, ?, ?, 0, ?)
	`, key, data, contentType, now, expiresAt, now)

	if err != nil {
		fmt.Printf("Warning: failed to cache tile: %v\n", err)
	}
}

// addToMemCache 添加到内存缓存
func (c *TileCache) addToMemCache(key string, data []byte) {
	c.memMu.Lock()
	defer c.memMu.Unlock()

	// 如果内存缓存已满，删除最旧的
	if len(c.memCache) >= c.memMaxSize {
		c.evictOldestMem()
	}

	c.memCache[key] = &MemCacheItem{
		Data:       data,
		AccessTime: time.Now(),
	}
}

// evictOldestMem 删除最旧的内存缓存项
func (c *TileCache) evictOldestMem() {
	var oldestKey string
	var oldestTime time.Time

	for key, item := range c.memCache {
		if oldestKey == "" || item.AccessTime.Before(oldestTime) {
			oldestKey = key
			oldestTime = item.AccessTime
		}
	}

	if oldestKey != "" {
		delete(c.memCache, oldestKey)
	}
}

// updateAccessStats 更新访问统计
func (c *TileCache) updateAccessStats(key string) {
	if c.db == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	_, _ = c.db.Exec(`
		UPDATE tile_cache 
		SET access_count = access_count + 1, last_access = ?
		WHERE cache_key = ?
	`, time.Now().Unix(), key)
}

// Delete 删除缓存
func (c *TileCache) Delete(key string) {
	// 从内存缓存删除
	c.memMu.Lock()
	delete(c.memCache, key)
	c.memMu.Unlock()

	// 从SQLite删除
	if c.db == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	_, _ = c.db.Exec("DELETE FROM tile_cache WHERE cache_key = ?", key)
}

// cleanupLoop 定期清理过期缓存
func (c *TileCache) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		c.cleanup()
	}
}

// cleanup 清理过期缓存
func (c *TileCache) cleanup() {
	// 清理内存缓存中的旧数据
	c.memMu.Lock()
	threshold := time.Now().Add(-c.ttl)
	for key, item := range c.memCache {
		if item.AccessTime.Before(threshold) {
			delete(c.memCache, key)
		}
	}
	c.memMu.Unlock()

	// 清理SQLite中的过期数据
	if c.db == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	result, err := c.db.Exec("DELETE FROM tile_cache WHERE expires_at < ?", time.Now().Unix())
	if err != nil {
		fmt.Printf("Warning: cleanup failed: %v\n", err)
		return
	}

	if rows, _ := result.RowsAffected(); rows > 0 {
		fmt.Printf("Cleaned up %d expired cache entries\n", rows)
		// 执行VACUUM优化数据库
		_, _ = c.db.Exec("VACUUM")
	}
}

// Clear 清空缓存
func (c *TileCache) Clear() {
	// 清空内存缓存
	c.memMu.Lock()
	c.memCache = make(map[string]*MemCacheItem)
	c.memMu.Unlock()

	// 清空SQLite
	if c.db == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	_, _ = c.db.Exec("DELETE FROM tile_cache")
	_, _ = c.db.Exec("VACUUM")
}

// Size 获取缓存大小
func (c *TileCache) Size() int {
	if c.db == nil {
		c.memMu.RLock()
		defer c.memMu.RUnlock()
		return len(c.memCache)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	var count int
	err := c.db.QueryRow("SELECT COUNT(*) FROM tile_cache").Scan(&count)
	if err != nil {
		return 0
	}
	return count
}

// Stats 获取缓存统计信息
func (c *TileCache) Stats() map[string]interface{} {
	stats := make(map[string]interface{})

	c.memMu.RLock()
	stats["memory_items"] = len(c.memCache)
	c.memMu.RUnlock()

	if c.db != nil {
		c.mu.RLock()
		defer c.mu.RUnlock()

		var totalCount int
		var totalSize int64
		c.db.QueryRow("SELECT COUNT(*), COALESCE(SUM(LENGTH(tile_data)), 0) FROM tile_cache").Scan(&totalCount, &totalSize)

		stats["sqlite_items"] = totalCount
		stats["sqlite_size_mb"] = float64(totalSize) / 1024 / 1024
	}

	return stats
}

// Close 关闭缓存
func (c *TileCache) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
