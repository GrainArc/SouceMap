package models

type SpatialRefSys struct {
	SRID      int    `gorm:"column:srid;primaryKey" json:"srid"`
	AuthName  string `gorm:"column:auth_name" json:"auth_name"`
	AuthSRID  int    `gorm:"column:auth_srid" json:"auth_srid"`
	SRText    string `gorm:"column:srtext" json:"srtext"`
	Proj4Text string `gorm:"column:proj4text" json:"proj4text"`
}

func (SpatialRefSys) TableName() string {
	return "spatial_ref_sys"
}
