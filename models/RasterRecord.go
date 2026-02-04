package models

import "gorm.io/datatypes"

type RasterRecord struct {
	ID         int64          `gorm:"primary_key;autoIncrement"`
	SourcePath string         `gorm:"type:varchar(255)"` //栅格源路径
	OutputPath string         `gorm:"type:varchar(255)"` //栅格操作的输出路径，投影接口是直接修改原始数据则不填这项
	Status     int            //栅格操作运行状态 0 运行中 1 执行完成  2 执行失败
	TypeName   string         `gorm:"type:varchar(255)"` //栅格操作的类型
	Args       datatypes.JSON `gorm:"type:jsonb"`        //栅格操作的输入参数
}

func (RasterRecord) TableName() string {
	return "raster_record"
}
