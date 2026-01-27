package models

import (
	"gorm.io/datatypes"
	"time"
)

type WmtsSchema struct {
	ID          int64          `gorm:"primary_key;autoIncrement"`
	LayerName   string         `gorm:"type:varchar(255);uniqueIndex;not null"` // 图层名称
	Opacity     float64        `gorm:"type:float;default:1.0"`                 // 透明度
	TileSize    int64          `gorm:"default:256"`                            // 瓦片大小
	ColorConfig datatypes.JSON `gorm:"type:jsonb"`                             // 颜色配置
	CreatedAt   time.Time      `gorm:"autoCreateTime"`
	UpdatedAt   time.Time      `gorm:"autoUpdateTime"`
}

func (WmtsSchema) TableName() string {
	return "wmts_schema"
}

type WmtsTileCache struct {
	ID   int64  `gorm:"primaryKey;autoIncrement"`
	X    int64  `gorm:"index:idx_xyz,priority:1;not null"`
	Y    int64  `gorm:"index:idx_xyz,priority:2;not null"`
	Z    int64  `gorm:"index:idx_xyz,priority:3;not null"`
	Byte []byte `gorm:"type:bytea"`
}

// TableName 动态设置表名
func (WmtsTileCache) TableName(layerName string) string {
	return layerName + "_wmts"
}
