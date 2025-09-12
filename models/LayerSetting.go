package models

type AttColor struct {
	ID        int64  `gorm:"primary_key"`
	LayerName string `gorm:"type:varchar(255)"`
	AttName   string `gorm:"type:varchar(255)"`
	Property  string `gorm:"type:varchar(255)"`
	Color     string `gorm:"type:varchar(255)"`
}

type ChineseProperty struct {
	ID        int64  `gorm:"primary_key"`
	LayerName string `gorm:"type:varchar(255)"`

	CName string `gorm:"type:varchar(255)"`
	EName string `gorm:"type:varchar(255)"`
}
