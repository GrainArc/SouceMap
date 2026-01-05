// models/field.go
package models

// FieldOperation 字段操作的请求结构
type FieldOperation struct {
	TableName    string `json:"table_name" binding:"required"`
	FieldName    string `json:"field_name" binding:"required"`
	FieldType    string `json:"field_type,omitempty"` // 扩展的字段类型
	Length       int    `json:"length,omitempty"`     // 字符串类型的长度
	Precision    int    `json:"precision,omitempty"`  // 数值类型的精度（总位数）
	Scale        int    `json:"scale,omitempty"`      // 数值类型的小数位数
	NewFieldName string `json:"new_field_name,omitempty"`
	DefaultValue string `json:"default_value,omitempty"`
	IsNullable   bool   `json:"is_nullable,omitempty"`
	Comment      string `json:"comment,omitempty"`
}

// FieldInfo 字段信息结构
type FieldInfo struct {
	FieldName    string  `json:"field_name"`    // 字段名
	FieldType    string  `json:"field_type"`    // 简化的数据类型
	Length       int     `json:"length"`        // varchar类型的长度
	Precision    int     `json:"precision"`     // 数值精度
	Scale        int     `json:"scale"`         // 小数位数
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

// FieldTypeInfo 字段类型详细信息
type FieldTypeInfo struct {
	TypeName    string `json:"type_name"`   // 类型名称
	Category    string `json:"category"`    // 类型分类: integer, float, string, datetime, binary, geometry
	NeedLength  bool   `json:"need_length"` // 是否需要长度参数
	NeedScale   bool   `json:"need_scale"`  // 是否需要精度/小数位参数
	Description string `json:"description"` // 类型描述
}

// SupportedFieldTypes 支持的字段类型列表
var SupportedFieldTypes = map[string]FieldTypeInfo{
	// 整数类型
	"smallint": {
		TypeName:    "smallint",
		Category:    "integer",
		NeedLength:  false,
		NeedScale:   false,
		Description: "小整数 (-32768 到 32767)",
	},
	"integer": {
		TypeName:    "integer",
		Category:    "integer",
		NeedLength:  false,
		NeedScale:   false,
		Description: "整数 (-2147483648 到 2147483647)",
	},
	"bigint": {
		TypeName:    "bigint",
		Category:    "integer",
		NeedLength:  false,
		NeedScale:   false,
		Description: "大整数 (-9223372036854775808 到 9223372036854775807)",
	},
	"serial": {
		TypeName:    "serial",
		Category:    "integer",
		NeedLength:  false,
		NeedScale:   false,
		Description: "自增整数",
	},
	"bigserial": {
		TypeName:    "bigserial",
		Category:    "integer",
		NeedLength:  false,
		NeedScale:   false,
		Description: "自增大整数",
	},

	// 浮点数类型
	"real": {
		TypeName:    "real",
		Category:    "float",
		NeedLength:  false,
		NeedScale:   false,
		Description: "单精度浮点数 (6位小数精度)",
	},
	"double": {
		TypeName:    "double precision",
		Category:    "float",
		NeedLength:  false,
		NeedScale:   false,
		Description: "双精度浮点数 (15位小数精度)",
	},
	"numeric": {
		TypeName:    "numeric",
		Category:    "float",
		NeedLength:  true,
		NeedScale:   true,
		Description: "精确数值类型，需指定精度(precision)和小数位(scale)",
	},
	"decimal": {
		TypeName:    "decimal",
		Category:    "float",
		NeedLength:  true,
		NeedScale:   true,
		Description: "精确数值类型，等同于numeric",
	},

	// 字符串类型
	"char": {
		TypeName:    "character",
		Category:    "string",
		NeedLength:  true,
		NeedScale:   false,
		Description: "定长字符串，需指定长度",
	},
	"varchar": {
		TypeName:    "character varying",
		Category:    "string",
		NeedLength:  true,
		NeedScale:   false,
		Description: "变长字符串，需指定最大长度",
	},
	"text": {
		TypeName:    "text",
		Category:    "string",
		NeedLength:  false,
		NeedScale:   false,
		Description: "无限长度文本",
	},

	// 日期时间类型
	"date": {
		TypeName:    "date",
		Category:    "datetime",
		NeedLength:  false,
		NeedScale:   false,
		Description: "日期 (年-月-日)",
	},
	"time": {
		TypeName:    "time without time zone",
		Category:    "datetime",
		NeedLength:  false,
		NeedScale:   false,
		Description: "时间 (时:分:秒)",
	},
	"timetz": {
		TypeName:    "time with time zone",
		Category:    "datetime",
		NeedLength:  false,
		NeedScale:   false,
		Description: "带时区的时间",
	},
	"timestamp": {
		TypeName:    "timestamp without time zone",
		Category:    "datetime",
		NeedLength:  false,
		NeedScale:   false,
		Description: "时间戳 (日期+时间)",
	},
	"timestamptz": {
		TypeName:    "timestamp with time zone",
		Category:    "datetime",
		NeedLength:  false,
		NeedScale:   false,
		Description: "带时区的时间戳",
	},

	// 二进制类型
	"bytea": {
		TypeName:    "bytea",
		Category:    "binary",
		NeedLength:  false,
		NeedScale:   false,
		Description: "二进制数据",
	},

	// 布尔类型
	"boolean": {
		TypeName:    "boolean",
		Category:    "boolean",
		NeedLength:  false,
		NeedScale:   false,
		Description: "布尔值 (true/false)",
	},

	// UUID类型
	"uuid": {
		TypeName:    "uuid",
		Category:    "string",
		NeedLength:  false,
		NeedScale:   false,
		Description: "UUID唯一标识符",
	},

	// 兼容旧版本的简化类型名
	"int": {
		TypeName:    "integer",
		Category:    "integer",
		NeedLength:  false,
		NeedScale:   false,
		Description: "整数 (integer的别名)",
	},
	"float": {
		TypeName:    "double precision",
		Category:    "float",
		NeedLength:  false,
		NeedScale:   false,
		Description: "浮点数 (double precision的别名)",
	},
	"bytes": {
		TypeName:    "bytea",
		Category:    "binary",
		NeedLength:  false,
		NeedScale:   false,
		Description: "二进制数据 (bytea的别名)",
	},
}

// FieldCalculatorRequest 字段计算器请求结构
type FieldCalculatorRequest struct {
	TableName     string               `json:"table_name" binding:"required"`     // 表名
	TargetField   string               `json:"target_field" binding:"required"`   // 目标字段
	OperationType string               `json:"operation_type" binding:"required"` // 操作类型: assign, copy, concat, calculate, round
	Expression    *CalculateExpression `json:"expression,omitempty"`              // 计算表达式
	Condition     string               `json:"condition,omitempty"`               // 过滤条件 (WHERE子句)
	DecimalPlaces *int                 `json:"decimal_places,omitempty"`          // 小数位数 (用于round操作)
	ReplaceConfig *ReplaceConfig       `json:"replace_config,omitempty"`          // 替换配置 (用于replace操作)
}

// ReplaceConfig 字符串替换配置
type ReplaceConfig struct {
	Mode        string `json:"mode" binding:"required"` // 替换模式: "normal" 普通替换, "regex" 正则替换
	SearchValue string `json:"search_value"`            // 要查找的值（普通模式）或正则表达式（正则模式）
	ReplaceWith string `json:"replace_with"`            // 替换后的值
	GlobalFlag  bool   `json:"global_flag"`             // 是否全局替换（替换所有匹配项），默认true
	CaseIgnore  bool   `json:"case_ignore"`             // 是否忽略大小写，默认false
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

// CalcType 计算类型
type CalcType string

const (
	CalcTypeArea      CalcType = "area"       // 面积
	CalcTypePerimeter CalcType = "perimeter"  // 周长
	CalcTypeCentroidX CalcType = "centroid_x" // 中心点X坐标(经度)
	CalcTypeCentroidY CalcType = "centroid_y" // 中心点Y坐标(纬度)
)

// AreaType 面积计算类型
type AreaType string

const (
	AreaTypePlanar    AreaType = "planar"    // 平面面积
	AreaTypeEllipsoid AreaType = "ellipsoid" // 椭球面积
)

// GeometryUpdateRequest 几何字段更新请求
type GeometryUpdateRequest struct {
	TableName   string   `json:"table_name" binding:"required"`   // 表名
	TargetField string   `json:"target_field" binding:"required"` // 目标字段名
	GeomField   string   `json:"geom_field"`                      // 几何字段名(默认geom)
	CalcType    CalcType `json:"calc_type" binding:"required"`    // 计算类型
	AreaType    AreaType `json:"area_type,omitempty"`             // 面积类型(仅area时需要)
	WhereClause string   `json:"where_clause,omitempty"`          // 可选的WHERE条件
}

// GeometryUpdateResponse 几何字段更新响应
type GeometryUpdateResponse struct {
	TableName    string `json:"table_name"`
	TargetField  string `json:"target_field"`
	CalcType     string `json:"calc_type"`
	RowsAffected int64  `json:"rows_affected"` // 影响的行数
	Success      bool   `json:"success"`
	Message      string `json:"message"`
}
