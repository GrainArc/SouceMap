// models/dynamic_raster.go
package models

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// DynamicRaster 动态栅格服务配置表
type DynamicRaster struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	Name        string    `gorm:"uniqueIndex;size:255;not null" json:"name"`    // 服务名称（唯一标识）
	ImagePath   string    `gorm:"size:1024;not null" json:"image_path"`         // 影像文件路径
	Description string    `gorm:"size:512" json:"description"`                  // 描述
	TileSize    int       `gorm:"default:256" json:"tile_size"`                 // 瓦片大小
	PoolSize    int       `gorm:"default:4" json:"pool_size"`                   // 连接池大小
	MinZoom     int       `gorm:"default:0" json:"min_zoom"`                    // 最小缩放级别
	MaxZoom     int       `gorm:"default:18" json:"max_zoom"`                   // 最大缩放级别
	Bounds      string    `gorm:"size:255" json:"bounds"`                       // 边界 "minLon,minLat,maxLon,maxLat"
	Center      string    `gorm:"size:128" json:"center"`                       // 中心点 "lon,lat"
	ServiceType string    `gorm:"size:32;default:'raster'" json:"service_type"` // 服务类型: raster/terrain
	Encoding    string    `gorm:"size:32;default:'mapbox'" json:"encoding"`     // 地形编码: mapbox/terrarium
	Status      int       `gorm:"default:0" json:"status"`                      // 状态: 0-未启动, 1-运行中, 2-错误
	ErrorMsg    string    `gorm:"size:512" json:"error_msg"`                    // 错误信息
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// TableName 指定表名
func (DynamicRaster) TableName() string {
	return "dynamic_raster"
}

// TileCacheTableName 根据服务名生成安全的缓存表名
func TileCacheTableName(serviceName string) string {
	safe := regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)
	if safe.MatchString(serviceName) && len(serviceName) <= 50 {
		return fmt.Sprintf("tile_cache_%s", strings.ToLower(serviceName))
	}
	hash := md5.Sum([]byte(serviceName))
	return fmt.Sprintf("tile_cache_%s", hex.EncodeToString(hash[:]))
}

// TileCache 瓦片缓存记录
type TileCache struct {
	Z        int    `gorm:"column:z;primaryKey" json:"z"`
	X        int    `gorm:"column:x;primaryKey" json:"x"`
	Y        int    `gorm:"column:y;primaryKey" json:"y"`
	TileData []byte `gorm:"column:tile_data;type:bytea" json:"-"`
	TileType string `gorm:"column:tile_type;size:20;primaryKey;default:'raster'" json:"tile_type"`
	Encoding string `gorm:"column:encoding;size:20;primaryKey;default:''" json:"encoding"`
}
