// models/field.go
package models

// FieldOperation 字段操作的请求结构
type FieldOperation struct {
	TableName    string `json:"table_name" binding:"required"`
	FieldName    string `json:"field_name" binding:"required"`
	FieldType    string `json:"field_type,omitempty"`
	NewFieldName string `json:"new_field_name,omitempty"`
	DefaultValue string `json:"default_value,omitempty"`
	IsNullable   bool   `json:"is_nullable,omitempty"`
	Comment      string `json:"comment,omitempty"`
}

// FieldInfo 字段信息结构
type FieldInfo struct {
	FieldName    string  `json:"field_name"`    // 字段名
	DataType     string  `json:"data_type"`     // 数据类型
	ColumnType   string  `json:"column_type"`   // 完整列类型
	IsNullable   string  `json:"is_nullable"`   // 是否可为空 YES/NO
	DefaultValue *string `json:"default_value"` // 默认值
	Extra        string  `json:"extra"`         // 额外信息（如auto_increment）
	Comment      string  `json:"comment"`       // 注释
	Position     int     `json:"position"`      // 字段位置
}

// TableStructure 表结构信息
type TableStructure struct {
	TableName    string      `json:"table_name"`
	TableComment string      `json:"table_comment"`
	Engine       string      `json:"engine"`
	Charset      string      `json:"charset"`
	Collation    string      `json:"collation"`
	Fields       []FieldInfo `json:"fields"`
	FieldCount   int         `json:"field_count"`
}

// Response 通用响应结构
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}
