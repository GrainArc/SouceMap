// tile_proxy/tile_cache_manager.go
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

const (
	DefaultCacheTTL = 30 * 24 * time.Hour // 30天
)

// TileCacheManager 瓦片缓存管理器
type TileCacheManager struct {
	cacheDir   string
	caches     map[uint]*MapTileCache // mapID -> cache
	cacheMutex sync.RWMutex
}

// MapTileCache 单个地图的瓦片缓存
type MapTileCache struct {
	mapID       uint
	db          *sql.DB
	dbPath      string
	maxSizeMB   int
	mu          sync.RWMutex
	memCache    map[string]*MemCacheItem
	memMu       sync.RWMutex
	memMaxItems int
}

// MemCacheItem 内存缓存项
type MemCacheItem struct {
	Data       []byte
	AccessTime time.Time
}

// NewTileCacheManager 创建缓存管理器
func NewTileCacheManager(cacheDir string) *TileCacheManager {
	if cacheDir == "" {
		cacheDir = "./cache/tiles"
	}

	// 确保缓存目录存在
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		fmt.Printf("Warning: failed to create cache directory: %v\n", err)
	}

	manager := &TileCacheManager{
		cacheDir: cacheDir,
		caches:   make(map[uint]*MapTileCache),
	}

	// 启动清理协程
	go manager.cleanupLoop()

	return manager
}

// GetCache 获取或创建地图缓存
func (m *TileCacheManager) GetCache(mapID uint, maxSizeMB int) (*MapTileCache, error) {
	// 如果不启用缓存
	if maxSizeMB <= 0 {
		return nil, nil
	}

	m.cacheMutex.RLock()
	cache, exists := m.caches[mapID]
	m.cacheMutex.RUnlock()

	if exists {
		return cache, nil
	}

	// 创建新缓存
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()

	// 双重检查
	if cache, exists := m.caches[mapID]; exists {
		return cache, nil
	}

	cache, err := m.createMapCache(mapID, maxSizeMB)
	if err != nil {
		return nil, err
	}

	m.caches[mapID] = cache
	return cache, nil
}

// createMapCache 创建地图缓存
func (m *TileCacheManager) createMapCache(mapID uint, maxSizeMB int) (*MapTileCache, error) {
	dbPath := filepath.Join(m.cacheDir, fmt.Sprintf("map_%d.db", mapID))

	cache := &MapTileCache{
		mapID:       mapID,
		dbPath:      dbPath,
		maxSizeMB:   maxSizeMB,
		memCache:    make(map[string]*MemCacheItem),
		memMaxItems: 100, // 内存缓存100个热点瓦片
	}

	if err := cache.initDB(); err != nil {
		return nil, fmt.Errorf("init cache db failed: %v", err)
	}

	return cache, nil
}

// initDB 初始化数据库
func (c *MapTileCache) initDB() error {
	var err error
	c.db, err = sql.Open("sqlite3", c.dbPath+"?cache=shared&mode=rwc&_journal_mode=WAL")
	if err != nil {
		return fmt.Errorf("open database failed: %v", err)
	}

	// 设置连接池
	c.db.SetMaxOpenConns(10)
	c.db.SetMaxIdleConns(5)
	c.db.SetConnMaxLifetime(time.Hour)

	// 创建表
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS tile_cache (
		z INTEGER NOT NULL,
		x INTEGER NOT NULL,
		y INTEGER NOT NULL,
		tile_data BLOB NOT NULL,
		content_type TEXT DEFAULT 'image/png',
		data_size INTEGER NOT NULL,
		created_at INTEGER NOT NULL,
		expires_at INTEGER NOT NULL,
		access_count INTEGER DEFAULT 0,
		last_access INTEGER NOT NULL,
		PRIMARY KEY (z, x, y)
	);
	CREATE INDEX IF NOT EXISTS idx_expires_at ON tile_cache(expires_at);
	CREATE INDEX IF NOT EXISTS idx_last_access ON tile_cache(last_access);
	CREATE INDEX IF NOT EXISTS idx_access_count ON tile_cache(access_count DESC);
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
		PRAGMA cache_size = -64000;
	`)
	if err != nil {
		fmt.Printf("Warning: failed to set pragma: %v\n", err)
	}

	return nil
}

// Get 获取缓存瓦片
func (c *MapTileCache) Get(z, x, y int) ([]byte, string, bool) {
	if c == nil {
		return nil, "", false
	}

	// 先检查内存缓存
	cacheKey := fmt.Sprintf("%d_%d_%d", z, x, y)
	c.memMu.RLock()
	if item, ok := c.memCache[cacheKey]; ok {
		c.memMu.RUnlock()
		// 更新访问时间
		c.memMu.Lock()
		item.AccessTime = time.Now()
		c.memMu.Unlock()
		return item.Data, "image/png", true
	}
	c.memMu.RUnlock()

	// 从SQLite获取
	c.mu.RLock()
	defer c.mu.RUnlock()

	var data []byte
	var contentType string
	var expiresAt int64

	err := c.db.QueryRow(
		"SELECT tile_data, content_type, expires_at FROM tile_cache WHERE z = ? AND x = ? AND y = ?",
		z, x, y,
	).Scan(&data, &contentType, &expiresAt)

	if err != nil {
		return nil, "", false
	}

	// 检查是否过期
	if time.Now().Unix() > expiresAt {
		go c.Delete(z, x, y)
		return nil, "", false
	}

	// 更新访问统计
	go c.updateAccessStats(z, x, y)

	// 添加到内存缓存
	c.addToMemCache(cacheKey, data)

	return data, contentType, true
}

// Set 设置缓存瓦片
func (c *MapTileCache) Set(z, x, y int, data []byte, contentType string) error {
	if c == nil || len(data) == 0 {
		return nil
	}

	// 检查缓存大小限制
	if err := c.checkAndCleanupSize(len(data)); err != nil {
		return err
	}

	// 添加到内存缓存
	cacheKey := fmt.Sprintf("%d_%d_%d", z, x, y)
	c.addToMemCache(cacheKey, data)

	// 保存到SQLite
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().Unix()
	expiresAt := time.Now().Add(DefaultCacheTTL).Unix()

	_, err := c.db.Exec(`
		INSERT OR REPLACE INTO tile_cache 
		(z, x, y, tile_data, content_type, data_size, created_at, expires_at, access_count, last_access)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
	`, z, x, y, data, contentType, len(data), now, expiresAt, now)

	if err != nil {
		return fmt.Errorf("failed to cache tile: %v", err)
	}

	return nil
}

// Delete 删除缓存瓦片
func (c *MapTileCache) Delete(z, x, y int) {
	if c == nil {
		return
	}

	// 从内存缓存删除
	cacheKey := fmt.Sprintf("%d_%d_%d", z, x, y)
	c.memMu.Lock()
	delete(c.memCache, cacheKey)
	c.memMu.Unlock()

	// 从SQLite删除
	c.mu.Lock()
	defer c.mu.Unlock()

	_, _ = c.db.Exec("DELETE FROM tile_cache WHERE z = ? AND x = ? AND y = ?", z, x, y)
}

// addToMemCache 添加到内存缓存
func (c *MapTileCache) addToMemCache(key string, data []byte) {
	c.memMu.Lock()
	defer c.memMu.Unlock()

	// 如果内存缓存已满，删除最旧的
	if len(c.memCache) >= c.memMaxItems {
		c.evictOldestMem()
	}

	c.memCache[key] = &MemCacheItem{
		Data:       data,
		AccessTime: time.Now(),
	}
}

// evictOldestMem 删除最旧的内存缓存项
func (c *MapTileCache) evictOldestMem() {
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
func (c *MapTileCache) updateAccessStats(z, x, y int) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	_, _ = c.db.Exec(`
		UPDATE tile_cache 
		SET access_count = access_count + 1, last_access = ?
		WHERE z = ? AND x = ? AND y = ?
	`, time.Now().Unix(), z, x, y)
}

// checkAndCleanupSize 检查并清理缓存大小
func (c *MapTileCache) checkAndCleanupSize(newDataSize int) error {
	if c.maxSizeMB <= 0 {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 获取当前缓存大小
	var currentSize int64
	err := c.db.QueryRow("SELECT COALESCE(SUM(data_size), 0) FROM tile_cache").Scan(&currentSize)
	if err != nil {
		return err
	}

	maxSizeBytes := int64(c.maxSizeMB) * 1024 * 1024
	newTotalSize := currentSize + int64(newDataSize)

	// 如果超过限制，删除最少访问的瓦片
	if newTotalSize > maxSizeBytes {
		// 删除访问次数最少且最久未访问的瓦片
		_, err = c.db.Exec(`
			DELETE FROM tile_cache 
			WHERE rowid IN (
				SELECT rowid FROM tile_cache 
				ORDER BY access_count ASC, last_access ASC 
				LIMIT (
					SELECT COUNT(*) FROM tile_cache 
					WHERE (SELECT SUM(data_size) FROM tile_cache) > ?
				) / 10
			)
		`, maxSizeBytes)

		if err != nil {
			fmt.Printf("Warning: cleanup failed: %v\n", err)
		}
	}

	return nil
}

// cleanup 清理过期缓存
func (c *MapTileCache) cleanup() {
	if c == nil {
		return
	}

	// 清理内存缓存
	c.memMu.Lock()
	threshold := time.Now().Add(-DefaultCacheTTL)
	for key, item := range c.memCache {
		if item.AccessTime.Before(threshold) {
			delete(c.memCache, key)
		}
	}
	c.memMu.Unlock()

	// 清理SQLite过期数据
	c.mu.Lock()
	defer c.mu.Unlock()

	result, err := c.db.Exec("DELETE FROM tile_cache WHERE expires_at < ?", time.Now().Unix())
	if err != nil {
		fmt.Printf("Warning: cleanup failed for map %d: %v\n", c.mapID, err)
		return
	}

	if rows, _ := result.RowsAffected(); rows > 0 {
		fmt.Printf("Cleaned up %d expired tiles for map %d\n", rows, c.mapID)
		_, _ = c.db.Exec("VACUUM")
	}
}

// GetStats 获取缓存统计
func (c *MapTileCache) GetStats() map[string]interface{} {
	if c == nil {
		return map[string]interface{}{"enabled": false}
	}

	stats := make(map[string]interface{})
	stats["map_id"] = c.mapID
	stats["max_size_mb"] = c.maxSizeMB

	c.memMu.RLock()
	stats["memory_items"] = len(c.memCache)
	c.memMu.RUnlock()

	c.mu.RLock()
	defer c.mu.RUnlock()

	var totalCount int
	var totalSize int64
	c.db.QueryRow("SELECT COUNT(*), COALESCE(SUM(data_size), 0) FROM tile_cache").Scan(&totalCount, &totalSize)

	stats["total_tiles"] = totalCount
	stats["total_size_mb"] = float64(totalSize) / 1024 / 1024
	stats["usage_percent"] = float64(totalSize) / float64(c.maxSizeMB*1024*1024) * 100

	return stats
}

// Close 关闭缓存
func (c *MapTileCache) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	return c.db.Close()
}

// cleanupLoop 定期清理
func (m *TileCacheManager) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		m.cacheMutex.RLock()
		caches := make([]*MapTileCache, 0, len(m.caches))
		for _, cache := range m.caches {
			caches = append(caches, cache)
		}
		m.cacheMutex.RUnlock()

		for _, cache := range caches {
			cache.cleanup()
		}
	}
}

// CloseAll 关闭所有缓存
func (m *TileCacheManager) CloseAll() {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()

	for _, cache := range m.caches {
		cache.Close()
	}
	m.caches = make(map[uint]*MapTileCache)
}

// GetAllStats 获取所有缓存统计
func (m *TileCacheManager) GetAllStats() []map[string]interface{} {
	m.cacheMutex.RLock()
	defer m.cacheMutex.RUnlock()

	stats := make([]map[string]interface{}, 0, len(m.caches))
	for _, cache := range m.caches {
		stats = append(stats, cache.GetStats())
	}
	return stats
}
