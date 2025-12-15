package models

import (
	"gorm.io/datatypes"
)

type Texture struct {
	ID          uint           `gorm:"primaryKey" json:"id"`
	Name        string         `gorm:"index;not null" json:"name"`
	MimeType    string         `gorm:"not null" json:"mime_type"`
	Width       int            `gorm:"not null" json:"width"`
	Height      int            `gorm:"not null" json:"height"`
	ImageData   datatypes.JSON `gorm:"type:longblob" json:"image_data"` // Base64编码的图片数据
	Description string         `json:"description"`
	CreatedAt   int64          `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt   int64          `gorm:"autoUpdateTime" json:"updated_at"`
}

func (Texture) TableName() string {
	return "textures"
}
