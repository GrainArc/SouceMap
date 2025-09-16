// models/field.go
package models

// FieldOperation 字段操作的请求结构
type FieldOperation struct {
	TableName    string `json:"table_name" binding:"required"`
	FieldName    string `json:"field_name" binding:"required"`
	FieldType    string `json:"field_type,omitempty"` // int, float, varchar, bytes
	Length       int    `json:"length,omitempty"`     // varchar类型的长度参数
	NewFieldName string `json:"new_field_name,omitempty"`
	DefaultValue string `json:"default_value,omitempty"`
	IsNullable   bool   `json:"is_nullable,omitempty"`
	Comment      string `json:"comment,omitempty"`
}

// FieldInfo 字段信息结构
type FieldInfo struct {
	FieldName    string  `json:"field_name"`    // 字段名
	FieldType    string  `json:"field_type"`    // 简化的数据类型：int, float, varchar, bytes
	Length       int     `json:"length"`        // varchar类型的长度
	IsNullable   string  `json:"is_nullable"`   // 是否可为空 YES/NO
	DefaultValue *string `json:"default_value"` // 默认值
	Comment      string  `json:"comment"`       // 注释
	Position     int     `json:"position"`      // 字段位置
}

// TableStructure 表结构信息
type TableStructure struct {
	TableName    string      `json:"table_name"`
	TableComment string      `json:"table_comment"`
	Fields       []FieldInfo `json:"fields"`
	FieldCount   int         `json:"field_count"`
}

// Response 通用响应结构
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}
