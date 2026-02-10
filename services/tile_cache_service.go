// services/tile_cache_service.go
package services

import (
	"fmt"
	"sync"

	"github.com/GrainArc/SouceMap/models"
	"gorm.io/gorm"
)

// TileCacheService 瓦片缓存服务
type TileCacheService struct {
	db            *gorm.DB
	mu            sync.RWMutex
	existingTable map[string]bool // 记录已确认存在的表，避免重复检查
}

var (
	tileCacheInstance *TileCacheService
	tileCacheOnce     sync.Once
)

// InitTileCacheService 初始化缓存服务（在应用启动时调用）
func InitTileCacheService(db *gorm.DB) {
	tileCacheOnce.Do(func() {
		tileCacheInstance = &TileCacheService{
			db:            db,
			existingTable: make(map[string]bool),
		}
	})
}

// GetTileCacheService 获取缓存服务单例
func GetTileCacheService() *TileCacheService {
	return tileCacheInstance
}

// EnsureCacheTable 确保缓存表存在（服务启动时调用）
func (s *TileCacheService) EnsureCacheTable(serviceName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tableName := models.TileCacheTableName(serviceName)

	if s.existingTable[tableName] {
		return nil
	}

	sql := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			z         INTEGER NOT NULL,
			x         INTEGER NOT NULL,
			y         INTEGER NOT NULL,
			tile_data BYTEA   NOT NULL,
			tile_type VARCHAR(20) NOT NULL DEFAULT 'raster',
			encoding  VARCHAR(20) NOT NULL DEFAULT '',
			PRIMARY KEY (z, x, y, tile_type, encoding)
		);
		CREATE INDEX IF NOT EXISTS idx_%s_zxy ON %s (z, x, y);
	`, tableName, serviceName, tableName)

	if err := s.db.Exec(sql).Error; err != nil {
		return fmt.Errorf("failed to create cache table %s: %w", tableName, err)
	}

	s.existingTable[tableName] = true
	return nil
}

// DropCacheTable 删除缓存表（服务删除时调用）
func (s *TileCacheService) DropCacheTable(serviceName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tableName := models.TileCacheTableName(serviceName)

	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if err := s.db.Exec(sql).Error; err != nil {
		return fmt.Errorf("failed to drop cache table %s: %w", tableName, err)
	}

	delete(s.existingTable, tableName)
	return nil
}

// GetCachedTile 从缓存获取瓦片
// 返回: tileData, found, error
func (s *TileCacheService) GetCachedTile(serviceName string, z, x, y int, tileType, encoding string) ([]byte, bool, error) {
	tableName := models.TileCacheTableName(serviceName)

	var cache models.TileCache
	result := s.db.Table(tableName).
		Where("z = ? AND x = ? AND y = ? AND tile_type = ? AND encoding = ?", z, x, y, tileType, encoding).
		First(&cache)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, false, nil
		}
		return nil, false, result.Error
	}

	return cache.TileData, true, nil
}

// SetCachedTile 写入缓存
func (s *TileCacheService) SetCachedTile(serviceName string, z, x, y int, tileType, encoding string, data []byte) error {
	tableName := models.TileCacheTableName(serviceName)

	cache := models.TileCache{
		Z:        z,
		X:        x,
		Y:        y,
		TileData: data,
		TileType: tileType,
		Encoding: encoding,
	}

	// UPSERT: 冲突时更新 tile_data
	sql := fmt.Sprintf(`
		INSERT INTO %s (z, x, y, tile_data, tile_type, encoding)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (z, x, y, tile_type, encoding)
		DO UPDATE SET tile_data = EXCLUDED.tile_data
	`, tableName)

	return s.db.Exec(sql, cache.Z, cache.X, cache.Y, cache.TileData, cache.TileType, cache.Encoding).Error
}

// ClearCache 清空某个服务的缓存（刷新服务时调用）
func (s *TileCacheService) ClearCache(serviceName string) error {
	tableName := models.TileCacheTableName(serviceName)
	sql := fmt.Sprintf("TRUNCATE TABLE %s", tableName)
	return s.db.Exec(sql).Error
}
