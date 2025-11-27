package models

type UpdateMessage struct {
	ID          int64  `gorm:"primary_key;autoIncrement"`
	LayerNameEN string `gorm:"type:varchar(255)"`
	LayerNameCN string `gorm:"type:varchar(255)"`
	UpdatedUser string `gorm:"type:varchar(255)"`
	Date        string `gorm:"type:varchar(255)"`
	MSG         string `gorm:"type:varchar(255)"`
}
