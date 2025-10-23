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

// FieldCalculatorRequest 字段计算器请求结构
type FieldCalculatorRequest struct {
	TableName     string               `json:"table_name" binding:"required"`     // 表名
	TargetField   string               `json:"target_field" binding:"required"`   // 目标字段
	OperationType string               `json:"operation_type" binding:"required"` // 操作类型: assign, copy, concat, calculate
	Expression    *CalculateExpression `json:"expression,omitempty"`              // 计算表达式
	Condition     string               `json:"condition,omitempty"`               // 过滤条件 (WHERE子句)
}

// CalculateExpression 计算表达式
type CalculateExpression struct {
	Type      string               `json:"type"`                // 类型: value, field, expression
	Value     interface{}          `json:"value,omitempty"`     // 直接赋值的值
	Field     string               `json:"field,omitempty"`     // 字段名
	Fields    []string             `json:"fields,omitempty"`    // 多字段组合(concat)
	Separator string               `json:"separator,omitempty"` // 字段组合分隔符
	Operator  string               `json:"operator,omitempty"`  // 运算符: +, -, *, /
	Left      *CalculateExpression `json:"left,omitempty"`      // 左操作数
	Right     *CalculateExpression `json:"right,omitempty"`     // 右操作数
}

// FieldCalculatorResponse 字段计算器响应
type FieldCalculatorResponse struct {
	TableName     string `json:"table_name"`
	TargetField   string `json:"target_field"`
	OperationType string `json:"operation_type"`
	AffectedRows  int64  `json:"affected_rows"`
	SQLStatement  string `json:"sql_statement"`
}
