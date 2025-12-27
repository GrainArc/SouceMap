package models

import "time"

type NetMap struct {
	ID              uint       `gorm:"primaryKey" json:"id"`
	MapName         string     `gorm:"column:map_name;index" json:"mapName"`                      // 地图名称
	GroupName       string     `gorm:"column:group_name;index" json:"groupName"`                  // 分组名称
	MapType         string     `gorm:"column:map_type" json:"mapType"`                            // 地图类型
	Protocol        string     `gorm:"column:protocol" json:"protocol"`                           // 协议 (http/https)
	Hostname        string     `gorm:"column:hostname" json:"hostname"`                           // 主机名
	Port            int        `gorm:"column:port" json:"port"`                                   // 端口号
	Projection      string     `gorm:"column:projection" json:"projection"`                       // 投影方式
	ImageFormat     string     `gorm:"column:image_format" json:"imageFormat"`                    // 图片格式
	MinLevel        int        `gorm:"column:min_level" json:"minLevel"`                          // 最小缩放级别
	MaxLevel        int        `gorm:"column:max_level" json:"maxLevel"`                          // 最大缩放级别
	UrlPath         string     `gorm:"column:url_path" json:"urlPath"`                            // URL路径
	TileUrlTemplate string     `gorm:"column:tile_url_template;type:text" json:"tileUrlTemplate"` // 完整URL模板
	Status          int        `gorm:"column:status;default:1" json:"status"`                     // 状态：0禁用，1启用
	CreatedAt       time.Time  `gorm:"autoCreateTime" json:"createdAt"`
	UpdatedAt       time.Time  `gorm:"autoUpdateTime" json:"updatedAt"`
	DeletedAt       *time.Time `gorm:"index" json:"deletedAt"`
}

func (NetMap) TableName() string {
	return "net_map"
}
