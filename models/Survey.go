package models

type TempGeo struct {
	BSM     string `gorm:"type:varchar(255);primary_key"`
	TBMJ    float64
	TBID    string `gorm:"type:varchar(255)"`
	MAC     string `gorm:"type:varchar(255)"`
	ZT      string `gorm:"type:varchar(255)"`
	Name    string `gorm:"type:varchar(255)"`
	Date    string `gorm:"type:varchar(255)"`
	Geojson []byte `gorm:"type:bytea"`
}

type TempLayer struct {
	ID        int64  `gorm:"primary_key"`
	TBID      string `gorm:"type:varchar(255)"`
	Layername string `gorm:"type:varchar(255)"`
	BSM       string `gorm:"type:varchar(255)"`
	Name      string `gorm:"type:varchar(255)"`
	ZT        string `gorm:"type:varchar(255)"`
	MAC       string `gorm:"type:varchar(255)"`
	Geojson   []byte `gorm:"type:bytea"`
}

type TempLayHeader struct {
	ID        int64  `gorm:"primary_key"`
	BSM       string `gorm:"type:varchar(255)"`
	Layername string `gorm:"type:varchar(255)"`
	MAC       string `gorm:"type:varchar(255)"`
	Progress  float64
	Date      string `gorm:"type:varchar(255)"`
}

type GeoPic struct {
	Pic_bsm string `gorm:"primary_key;type:varchar(255)"`
	Url     string `gorm:"type:varchar(255)" json:"url"`
	BSM     string `gorm:"type:varchar(255)"`
	X       string `gorm:"type:varchar(255)"`
	Y       string `gorm:"type:varchar(255)"`
	Angel   string `gorm:"type:varchar(255)"`
	TBID    string `gorm:"type:varchar(255)"`
	Date    string `gorm:"type:varchar(255)"`
}
type ZDTPic struct {
	TBID string `gorm:"primary_key;type:varchar(255)"`
	Url  string `gorm:"type:varchar(255)" json:"url"`
	BSM  string `gorm:"type:varchar(255)"`
	Date string `gorm:"type:varchar(255)"`
}

type TempLayerAttribute struct {
	ID        int64  `gorm:"primary_key"`
	TBID      string `gorm:"type:varchar(255)"`
	QKSM      string `gorm:"type:varchar(255)"`
	Layername string `gorm:"type:varchar(255)"`
	B         string `gorm:"type:varchar(255)"`
	D         string `gorm:"type:varchar(255)"`
	N         string `gorm:"type:varchar(255)"`
	X         string `gorm:"type:varchar(255)"`
	BZ        string `gorm:"type:varchar(255)"`
	ZJR       []byte
	DCR       []byte
}
