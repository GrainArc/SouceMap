// views/tile_server_manager.go
package services

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/models"
	"gorm.io/gorm"
)

// TileServerManager 瓦片服务管理器
type TileServerManager struct {
	servers map[string]*Gogeo.TileServer
	configs map[string]*models.DynamicRaster
	mu      sync.RWMutex
	db      *gorm.DB
}

var (
	tileServerManager *TileServerManager
	tileServerOnce    sync.Once
)

// GetTileServerManager 获取单例管理器
func GetTileServerManager() *TileServerManager {
	return tileServerManager
}

// InitTileServerManager 初始化管理器（在程序启动时调用）
func InitTileServerManager(db *gorm.DB) *TileServerManager {
	tileServerOnce.Do(func() {
		tileServerManager = &TileServerManager{
			servers: make(map[string]*Gogeo.TileServer),
			configs: make(map[string]*models.DynamicRaster),
			db:      db,
		}
		// 加载已有的服务配置
		tileServerManager.LoadAllServices()
	})
	return tileServerManager
}

// LoadAllServices 从数据库加载所有服务配置并启动
func (m *TileServerManager) LoadAllServices() {
	var rasters []models.DynamicRaster
	m.db.Find(&rasters)

	for _, raster := range rasters {
		if raster.Status == 1 {
			// 尝试启动服务
			if err := m.StartServer(raster.Name); err != nil {
				// 更新状态为错误
				m.db.Model(&raster).Updates(map[string]interface{}{
					"status":    2,
					"error_msg": err.Error(),
				})
			}
		}
	}
}

// AddService 添加新的服务配置
func (m *TileServerManager) AddService(raster *models.DynamicRaster) error {
	// 检查文件是否存在
	if _, err := os.Stat(raster.ImagePath); os.IsNotExist(err) {
		return fmt.Errorf("image file not found: %s", raster.ImagePath)
	}

	// 设置默认值
	if raster.TileSize <= 0 {
		raster.TileSize = 256
	}
	if raster.PoolSize <= 0 {
		raster.PoolSize = 4
	}
	if raster.ServiceType == "" {
		raster.ServiceType = "raster"
	}
	if raster.Encoding == "" {
		raster.Encoding = "mapbox"
	}

	// 保存到数据库
	if err := m.db.Create(raster).Error; err != nil {
		return fmt.Errorf("failed to save service config: %w", err)
	}
	if cacheService := GetTileCacheService(); cacheService != nil {
		if err := cacheService.EnsureCacheTable(raster.Name); err != nil {
			// 缓存表创建失败不影响主流程，仅记录日志
			fmt.Printf("[WARN] failed to create cache table for %s: %v\n", raster.Name, err)
		}
	}
	return nil
}

// UpdateService 更新服务配置
func (m *TileServerManager) UpdateService(name string, updates map[string]interface{}) error {
	// 如果服务正在运行，先停止
	m.StopServer(name)

	// 更新数据库
	result := m.db.Model(&models.DynamicRaster{}).Where("name = ?", name).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("service not found: %s", name)
	}

	return nil
}

// DeleteService 删除服务
func (m *TileServerManager) DeleteService(name string) error {
	// 先停止服务
	m.StopServer(name)

	// 从数据库删除
	result := m.db.Where("name = ?", name).Delete(&models.DynamicRaster{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("service not found: %s", name)
	}
	if cacheService := GetTileCacheService(); cacheService != nil {
		if err := cacheService.DropCacheTable(name); err != nil {
			fmt.Printf("[WARN] failed to drop cache table for %s: %v\n", name, err)
		}
	}
	return nil
}

// views/tile_server_manager.go (继续)

// StartServer 启动服务
func (m *TileServerManager) StartServer(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查是否已经运行
	if _, ok := m.servers[name]; ok {
		return nil // 已经在运行
	}

	// 从数据库获取配置
	var raster models.DynamicRaster
	if err := m.db.Where("name = ?", name).First(&raster).Error; err != nil {
		return fmt.Errorf("service not found: %s", name)
	}

	// 检查文件是否存在
	if _, err := os.Stat(raster.ImagePath); os.IsNotExist(err) {
		m.db.Model(&raster).Updates(map[string]interface{}{
			"status":    2,
			"error_msg": "image file not found",
		})
		return fmt.Errorf("image file not found: %s", raster.ImagePath)
	}

	// 创建TileServer
	options := &Gogeo.TileServerOptions{
		TileSize: raster.TileSize,
		PoolSize: raster.PoolSize,
	}

	server, err := Gogeo.NewTileServer(raster.ImagePath, options)
	if err != nil {
		m.db.Model(&raster).Updates(map[string]interface{}{
			"status":    2,
			"error_msg": err.Error(),
		})
		return fmt.Errorf("failed to create tile server: %w", err)
	}

	// 获取边界信息并更新
	minLon, minLat, maxLon, maxLat := server.GetBounds()
	bounds := fmt.Sprintf("%.6f,%.6f,%.6f,%.6f", minLon, minLat, maxLon, maxLat)
	center := fmt.Sprintf("%.6f,%.6f", (minLon+maxLon)/2, (minLat+maxLat)/2)

	// 更新数据库状态
	m.db.Model(&raster).Updates(map[string]interface{}{
		"status":    1,
		"error_msg": "",
		"bounds":    bounds,
		"center":    center,
	})

	// 保存到内存
	m.servers[name] = server
	m.configs[name] = &raster
	if cacheService := GetTileCacheService(); cacheService != nil {
		_ = cacheService.EnsureCacheTable(name)
	}
	return nil
}

// StopServer 停止服务
func (m *TileServerManager) StopServer(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if server, ok := m.servers[name]; ok {
		server.Close()
		delete(m.servers, name)
		delete(m.configs, name)

		// 更新数据库状态
		m.db.Model(&models.DynamicRaster{}).Where("name = ?", name).Updates(map[string]interface{}{
			"status": 0,
		})
	}
}

// GetServer 获取服务实例
func (m *TileServerManager) GetServer(name string) (*Gogeo.TileServer, *models.DynamicRaster, error) {
	m.mu.RLock()
	server, ok := m.servers[name]
	config := m.configs[name]
	m.mu.RUnlock()

	if !ok {
		// 尝试自动启动
		if err := m.StartServer(name); err != nil {
			return nil, nil, err
		}
		m.mu.RLock()
		server = m.servers[name]
		config = m.configs[name]
		m.mu.RUnlock()
	}

	return server, config, nil
}

// GetServiceConfig 获取服务配置
func (m *TileServerManager) GetServiceConfig(name string) (*models.DynamicRaster, error) {
	var raster models.DynamicRaster
	if err := m.db.Where("name = ?", name).First(&raster).Error; err != nil {
		return nil, fmt.Errorf("service not found: %s", name)
	}
	return &raster, nil
}

// ListServices 列出所有服务
func (m *TileServerManager) ListServices() ([]models.DynamicRaster, error) {
	var rasters []models.DynamicRaster
	if err := m.db.Find(&rasters).Error; err != nil {
		return nil, err
	}
	return rasters, nil
}

// ListRunningServices 列出运行中的服务
func (m *TileServerManager) ListRunningServices() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.servers))
	for name := range m.servers {
		names = append(names, name)
	}
	return names
}

// CloseAll 关闭所有服务
func (m *TileServerManager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, server := range m.servers {
		server.Close()
		m.db.Model(&models.DynamicRaster{}).Where("name = ?", name).Update("status", 0)
	}
	m.servers = make(map[string]*Gogeo.TileServer)
	m.configs = make(map[string]*models.DynamicRaster)
}

// RefreshService 刷新服务（重新加载）
func (m *TileServerManager) RefreshService(name string) error {
	m.StopServer(name)

	return m.StartServer(name)
}

// ParseBounds 解析边界字符串
func ParseBounds(boundsStr string) (minLon, minLat, maxLon, maxLat float64, err error) {
	parts := strings.Split(boundsStr, ",")
	if len(parts) != 4 {
		return 0, 0, 0, 0, fmt.Errorf("invalid bounds format")
	}
	minLon, _ = strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	minLat, _ = strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	maxLon, _ = strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
	maxLat, _ = strconv.ParseFloat(strings.TrimSpace(parts[3]), 64)
	return
}

// ParseCenter 解析中心点字符串
func ParseCenter(centerStr string) (lon, lat float64, err error) {
	parts := strings.Split(centerStr, ",")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid center format")
	}
	lon, _ = strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	lat, _ = strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	return
}
