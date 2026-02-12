package models

import "gorm.io/datatypes"

type GeoRecord struct {
	TableName    string `gorm:"type:varchar(255)"`
	Username     string `gorm:"type:varchar(255)"`
	Type         string `gorm:"type:varchar(255)"`
	Date         string `gorm:"type:varchar(255)"`
	BZ           string `gorm:"type:varchar(255)"`
	ID           int64  `gorm:"primary_key;autoIncrement"`
	GeoID        int32
	OldGeojson   datatypes.JSON `gorm:"type:jsonb"`
	NewGeojson   datatypes.JSON `gorm:"type:jsonb"`
	DelObjectIDs datatypes.JSON `gorm:"type:jsonb"`
	// 新增
	SessionID int64          `gorm:"index"`      // 所属编辑会话
	SeqNo     int            `gorm:"default:0"`  // 会话内操作序号，用于有序回退
	InputIDs  datatypes.JSON `gorm:"type:jsonb"` // 输入的PostGIS IDs
	OutputIDs datatypes.JSON `gorm:"type:jsonb"`
}

type FieldRecord struct {
	TableName    string `gorm:"type:varchar(255)"`
	Type         string `gorm:"type:varchar(255)"`
	BZ           string `gorm:"type:varchar(255)"`
	ID           int64  `gorm:"primary_key;autoIncrement"`
	OldFieldName string `gorm:"type:varchar(255)"`
	OldFieldType string `gorm:"type:varchar(255)"`
	NewFieldName string `gorm:"type:varchar(255)"`
	NewFieldType string `gorm:"type:varchar(255)"`
}
