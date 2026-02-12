package models

// models/edit_session.go

// EditSession 编辑会话，追踪一组关联操作
type EditSession struct {
	ID        int64  `gorm:"primaryKey;autoIncrement"`
	TableName string `gorm:"type:varchar(255);index"`
	Username  string `gorm:"type:varchar(255)"`
	CreatedAt string `gorm:"type:varchar(255)"`
	Status    string `gorm:"type:varchar(50)"` // active / committed / rolledback
}

// OriginMapping 要素溯源映射表
// 记录每个PostGIS中的要素与源文件要素的对应关系
type OriginMapping struct {
	ID              int64  `gorm:"primaryKey;autoIncrement"`
	TableName       string `gorm:"type:varchar(255);index:idx_table_pgid"`
	PostGISID       int32  `gorm:"index:idx_table_pgid"`   // PostGIS中的id
	SourceObjectID  int64  `gorm:"index:idx_table_source"` // 源文件中的objectid/fid，-1表示纯新增
	Origin          string `gorm:"type:varchar(50)"`       // "original" | "derived"
	ParentPostGISID int32  `gorm:"default:0"`              // 派生自哪个PostGIS要素（用于分割/打散）
	SessionID       int64  `gorm:"index"`                  // 哪个编辑会话产生的
	IsDeleted       bool   `gorm:"default:false"`          // 是否已被后续操作删除
}
