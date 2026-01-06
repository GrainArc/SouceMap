// models/dynamic_raster.go
package models

import (
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
