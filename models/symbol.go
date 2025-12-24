package models

type Symbol struct {
	ID          uint   `gorm:"primaryKey" json:"id"`
	Name        string `gorm:"index;not null" json:"name"`
	MimeType    string `gorm:"not null" json:"mime_type"`
	Width       int    `gorm:"not null" json:"width"`
	Height      int    `gorm:"not null" json:"height"`
	ImageData   []byte `gorm:"type:BLOB" json:"image_data"`
	Description string `json:"description"`
	Category    string `gorm:"index" json:"category"` // 图标分类，如：poi、marker、arrow等

	CreatedAt int64 `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt int64 `gorm:"autoUpdateTime" json:"updated_at"`
}

func (Symbol) TableName() string {
	return "symbols"
}
