package models

import "gorm.io/datatypes"

type Report struct {
	ID         int64          `gorm:"primary_key;autoIncrement"`
	ReportName string         `gorm:"type:varchar(255)"`
	Layers     datatypes.JSON `gorm:"type:jsonb"`
}
