package models

import (
	"gorm.io/datatypes"
)

type LoginUser struct {
	ID        int64  `gorm:"primary_key"`
	Username  string `gorm:"type:varchar(255)"`
	Password  string `gorm:"type:varchar(255)"`
	Name      string `gorm:"type:varchar(255)"`
	Phone     string `gorm:"type:varchar(255)"`
	Userunits string `gorm:"type:varchar(255)"`
	Location  string `gorm:"type:varchar(255)"`
	Post      string `gorm:"type:varchar(255)"`
	Token     string `gorm:"type:varchar(255)"`
	Grade     string `gorm:"type:varchar(255)"`
	Mac       string
	Date      string `gorm:"type:varchar(255)"`
	CentX     float64
	CentY     float64
	CentZ     float64
	InitX     float64
	InitY     float64
	InitZ     float64
}

type MySchema struct {
	ID          int64          `gorm:"primary_key;autoIncrement"`
	Main        string         `gorm:"type:varchar(255)"`
	CN          string         `gorm:"type:varchar(255)"`
	EN          string         `gorm:"type:varchar(255)"`
	Type        string         `gorm:"type:varchar(255)"`
	Opacity     string         `gorm:"type:varchar(254)"`
	Color       string         `gorm:"type:varchar(255)"`
	LineWidth   string         `gorm:"type:varchar(55)"`
	FillType    string         `gorm:"type:varchar(255)"`
	LineColor   string         `gorm:"type:varchar(255)"`
	UpdatedDate string         `gorm:"type:varchar(255)"`
	Source      datatypes.JSON `gorm:"type:jsonb"`
	Userunits   string         `gorm:"type:varchar(255)"`
}

// 配置表
type LayerMXD struct {
	ID          int64  `gorm:"primary_key;autoIncrement"`
	EN          string `gorm:"type:varchar(255)"`
	Main        string `gorm:"type:varchar(255)"`
	CN          string `gorm:"type:varchar(255)"`
	MXDName     string `gorm:"type:varchar(255)"`
	MXDUid      string `gorm:"type:varchar(255)"`
	LineWidth   string `gorm:"type:varchar(55)"`
	LayerSortID int64
	Opacity     string         `gorm:"type:varchar(254)"`
	FillType    string         `gorm:"type:varchar(255)"`
	LineColor   string         `gorm:"type:varchar(255)"`
	ColorSet    datatypes.JSON `gorm:"type:jsonb"`
}

type LayerHeader struct {
	ID      int64  `gorm:"primary_key;autoIncrement"`
	MXDName string `gorm:"type:varchar(255)"`
	MXDUid  string `gorm:"type:varchar(255)"`
}
