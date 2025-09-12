package models

type GeoRecord struct {
	TableName  string `gorm:"type:varchar(255)"`
	Username   string `gorm:"type:varchar(255)"`
	Type       string `gorm:"type:varchar(255)"`
	Date       string `gorm:"type:varchar(255)"`
	BZ         string `gorm:"type:varchar(255)"`
	ID         int64  `gorm:"primary_key"`
	GeoID      int32
	OldGeojson []byte `gorm:"type:bytea"`
	NewGeojson []byte `gorm:"type:bytea"`
}
