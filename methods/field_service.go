package methods

import (
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"regexp"
	"strconv"
	"strings"
)

type FieldService struct{}

func NewFieldService() *FieldService {
	return &FieldService{}
}

type FieldTypeMapping struct {
	PostgreSQLType string
	GDALType       string
	Category       string
	DefaultWidth   int
	DefaultPrec    int
}

var PostgreSQLToGDALTypeMap = map[string]FieldTypeMapping{
	// 整数类型
	"smallint":  {PostgreSQLType: "SMALLINT", GDALType: "OFTInteger", Category: "integer", DefaultWidth: 0, DefaultPrec: 0},
	"int2":      {PostgreSQLType: "SMALLINT", GDALType: "OFTInteger", Category: "integer", DefaultWidth: 0, DefaultPrec: 0},
	"integer":   {PostgreSQLType: "INTEGER", GDALType: "OFTInteger", Category: "integer", DefaultWidth: 0, DefaultPrec: 0},
	"int":       {PostgreSQLType: "INTEGER", GDALType: "OFTInteger", Category: "integer", DefaultWidth: 0, DefaultPrec: 0},
	"int4":      {PostgreSQLType: "INTEGER", GDALType: "OFTInteger", Category: "integer", DefaultWidth: 0, DefaultPrec: 0},
	"bigint":    {PostgreSQLType: "BIGINT", GDALType: "OFTInteger64", Category: "integer", DefaultWidth: 0, DefaultPrec: 0},
	"int8":      {PostgreSQLType: "BIGINT", GDALType: "OFTInteger64", Category: "integer", DefaultWidth: 0, DefaultPrec: 0},
	"serial":    {PostgreSQLType: "SERIAL", GDALType: "OFTInteger", Category: "integer", DefaultWidth: 0, DefaultPrec: 0},
	"bigserial": {PostgreSQLType: "BIGSERIAL", GDALType: "OFTInteger64", Category: "integer", DefaultWidth: 0, DefaultPrec: 0},
	// 浮点数类型
	"real":             {PostgreSQLType: "REAL", GDALType: "OFTReal", Category: "float", DefaultWidth: 0, DefaultPrec: 0},
	"float4":           {PostgreSQLType: "REAL", GDALType: "OFTReal", Category: "float", DefaultWidth: 0, DefaultPrec: 0},
	"double precision": {PostgreSQLType: "DOUBLE PRECISION", GDALType: "OFTReal", Category: "float", DefaultWidth: 0, DefaultPrec: 0},
	"double":           {PostgreSQLType: "DOUBLE PRECISION", GDALType: "OFTReal", Category: "float", DefaultWidth: 0, DefaultPrec: 0},
	"float8":           {PostgreSQLType: "DOUBLE PRECISION", GDALType: "OFTReal", Category: "float", DefaultWidth: 0, DefaultPrec: 0},
	"float":            {PostgreSQLType: "DOUBLE PRECISION", GDALType: "OFTReal", Category: "float", DefaultWidth: 0, DefaultPrec: 0},
	"numeric":          {PostgreSQLType: "NUMERIC", GDALType: "OFTReal", Category: "float", DefaultWidth: 18, DefaultPrec: 6},
	"decimal":          {PostgreSQLType: "NUMERIC", GDALType: "OFTReal", Category: "float", DefaultWidth: 18, DefaultPrec: 6},
	// 字符串类型
	"character varying": {PostgreSQLType: "VARCHAR", GDALType: "OFTString", Category: "string", DefaultWidth: 255, DefaultPrec: 0},
	"varchar":           {PostgreSQLType: "VARCHAR", GDALType: "OFTString", Category: "string", DefaultWidth: 255, DefaultPrec: 0},
	"character":         {PostgreSQLType: "CHAR", GDALType: "OFTString", Category: "string", DefaultWidth: 1, DefaultPrec: 0},
	"char":              {PostgreSQLType: "CHAR", GDALType: "OFTString", Category: "string", DefaultWidth: 1, DefaultPrec: 0},
	"text":              {PostgreSQLType: "TEXT", GDALType: "OFTString", Category: "string", DefaultWidth: 0, DefaultPrec: 0},
	// 日期时间类型
	"date":                        {PostgreSQLType: "DATE", GDALType: "OFTDate", Category: "datetime", DefaultWidth: 0, DefaultPrec: 0},
	"time":                        {PostgreSQLType: "TIME", GDALType: "OFTTime", Category: "datetime", DefaultWidth: 0, DefaultPrec: 0},
	"time without time zone":      {PostgreSQLType: "TIME", GDALType: "OFTTime", Category: "datetime", DefaultWidth: 0, DefaultPrec: 0},
	"time with time zone":         {PostgreSQLType: "TIMETZ", GDALType: "OFTTime", Category: "datetime", DefaultWidth: 0, DefaultPrec: 0},
	"timetz":                      {PostgreSQLType: "TIMETZ", GDALType: "OFTTime", Category: "datetime", DefaultWidth: 0, DefaultPrec: 0},
	"timestamp":                   {PostgreSQLType: "TIMESTAMP", GDALType: "OFTDateTime", Category: "datetime", DefaultWidth: 0, DefaultPrec: 0},
	"timestamp without time zone": {PostgreSQLType: "TIMESTAMP", GDALType: "OFTDateTime", Category: "datetime", DefaultWidth: 0, DefaultPrec: 0},
	"timestamp with time zone":    {PostgreSQLType: "TIMESTAMPTZ", GDALType: "OFTDateTime", Category: "datetime", DefaultWidth: 0, DefaultPrec: 0},
	"timestamptz":                 {PostgreSQLType: "TIMESTAMPTZ", GDALType: "OFTDateTime", Category: "datetime", DefaultWidth: 0, DefaultPrec: 0},
	// 二进制类型
	"bytea": {PostgreSQLType: "BYTEA", GDALType: "OFTBinary", Category: "binary", DefaultWidth: 0, DefaultPrec: 0},
	"bytes": {PostgreSQLType: "BYTEA", GDALType: "OFTBinary", Category: "binary", DefaultWidth: 0, DefaultPrec: 0},
	// 布尔类型
	"boolean": {PostgreSQLType: "BOOLEAN", GDALType: "OFTInteger", Category: "boolean", DefaultWidth: 0, DefaultPrec: 0},
	"bool":    {PostgreSQLType: "BOOLEAN", GDALType: "OFTInteger", Category: "boolean", DefaultWidth: 0, DefaultPrec: 0},
	// UUID类型
	"uuid": {PostgreSQLType: "UUID", GDALType: "OFTString", Category: "string", DefaultWidth: 36, DefaultPrec: 0},
}

// convertToPostgreSQLType 将简化的数据类型转换为PostgreSQL类型
func (fs *FieldService) convertToPostgreSQLType(fieldType string, length, precision, scale int) (string, error) {
	fieldType = strings.ToLower(strings.TrimSpace(fieldType))
	// 查找类型映射
	mapping, exists := PostgreSQLToGDALTypeMap[fieldType]
	if !exists {
		return "", fmt.Errorf("不支持的字段类型: %s", fieldType)
	}
	// 根据类型构建完整的PostgreSQL类型定义
	switch fieldType {
	case "varchar", "character varying":
		if length <= 0 {
			length = mapping.DefaultWidth
		}
		return fmt.Sprintf("VARCHAR(%d)", length), nil
	case "char", "character":
		if length <= 0 {
			length = mapping.DefaultWidth
		}
		return fmt.Sprintf("CHAR(%d)", length), nil
	case "numeric", "decimal":
		if precision <= 0 {
			precision = mapping.DefaultWidth
		}
		if scale < 0 {
			scale = mapping.DefaultPrec
		}
		if scale > 0 {
			return fmt.Sprintf("NUMERIC(%d,%d)", precision, scale), nil
		}
		return fmt.Sprintf("NUMERIC(%d)", precision), nil
	default:
		return mapping.PostgreSQLType, nil
	}
}

// convertFromPostgreSQLType 将PostgreSQL类型转换为简化类型
func (fs *FieldService) convertFromPostgreSQLType(pgType string) (fieldType string, length, precision, scale int) {
	pgType = strings.ToLower(strings.TrimSpace(pgType))
	// 提取基础类型和参数
	baseType, params := fs.parseTypeWithParams(pgType)
	// 根据基础类型进行映射
	switch {
	// 整数类型
	case strings.Contains(baseType, "smallint") || baseType == "int2":
		return "smallint", 0, 0, 0
	case strings.Contains(baseType, "bigint") || baseType == "int8":
		return "bigint", 0, 0, 0
	case strings.Contains(baseType, "integer") || baseType == "int4" || baseType == "int":
		return "integer", 0, 0, 0
	case strings.Contains(baseType, "serial"):
		if strings.Contains(baseType, "big") {
			return "bigserial", 0, 0, 0
		}
		return "serial", 0, 0, 0
	// 浮点数类型
	case strings.Contains(baseType, "double precision") || baseType == "float8":
		return "double", 0, 0, 0
	case strings.Contains(baseType, "real") || baseType == "float4":
		return "real", 0, 0, 0
	case strings.Contains(baseType, "numeric") || strings.Contains(baseType, "decimal"):
		if len(params) >= 2 {
			return "numeric", 0, params[0], params[1]
		} else if len(params) == 1 {
			return "numeric", 0, params[0], 0
		}
		return "numeric", 0, 18, 6
	// 字符串类型
	case strings.Contains(baseType, "character varying") || strings.Contains(baseType, "varchar"):
		if len(params) > 0 {
			return "varchar", params[0], 0, 0
		}
		return "varchar", 255, 0, 0
	case strings.Contains(baseType, "character") || baseType == "char":
		if len(params) > 0 {
			return "char", params[0], 0, 0
		}
		return "char", 1, 0, 0
	case strings.Contains(baseType, "text"):
		return "text", 0, 0, 0
	// 日期时间类型
	case strings.Contains(baseType, "timestamp"):
		if strings.Contains(baseType, "with time zone") || strings.Contains(baseType, "timestamptz") {
			return "timestamptz", 0, 0, 0
		}
		return "timestamp", 0, 0, 0
	case strings.Contains(baseType, "time"):
		if strings.Contains(baseType, "with time zone") || strings.Contains(baseType, "timetz") {
			return "timetz", 0, 0, 0
		}
		return "time", 0, 0, 0
	case strings.Contains(baseType, "date"):
		return "date", 0, 0, 0
	// 二进制类型
	case strings.Contains(baseType, "bytea"):
		return "bytea", 0, 0, 0
	// 布尔类型
	case strings.Contains(baseType, "boolean") || strings.Contains(baseType, "bool"):
		return "boolean", 0, 0, 0
	// UUID类型
	case strings.Contains(baseType, "uuid"):
		return "uuid", 0, 0, 0
	default:
		// 默认返回varchar
		return "varchar", 255, 0, 0
	}
}
func (fs *FieldService) parseTypeWithParams(pgType string) (baseType string, params []int) {
	// 使用正则表达式提取类型和参数
	re := regexp.MustCompile(`^([a-z\s]+)(?:$([^)]+)$)?$`)
	matches := re.FindStringSubmatch(pgType)
	if len(matches) < 2 {
		return pgType, nil
	}
	baseType = strings.TrimSpace(matches[1])
	if len(matches) >= 3 && matches[2] != "" {
		paramStrs := strings.Split(matches[2], ",")
		for _, p := range paramStrs {
			if val, err := strconv.Atoi(strings.TrimSpace(p)); err == nil {
				params = append(params, val)
			}
		}
	}
	return baseType, params
}

func (fs *FieldService) AddField(tableName, fieldName, fieldType string, length, precision, scale int, defaultValue, comment string, isNullable bool) error {
	// 验证字段类型
	if !fs.isValidFieldType(fieldType) {
		return fmt.Errorf("不支持的字段类型: %s，请使用 GetSupportedFieldTypes 查看支持的类型", fieldType)
	}
	// 转换为PostgreSQL类型
	pgType, err := fs.convertToPostgreSQLType(fieldType, length, precision, scale)
	if err != nil {
		return fmt.Errorf("转换字段类型失败: %v", err)
	}
	// 检查表是否有数据
	var rowCount int64
	if err := models.DB.Raw(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, tableName)).Scan(&rowCount).Error; err != nil {
		return fmt.Errorf("检查表数据失败: %v", err)
	}
	// 构建 SQL 语句
	var sql strings.Builder
	sql.WriteString(fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN "%s" %s`, tableName, fieldName, pgType))
	// 如果表中有数据且字段不允许为空，必须提供默认值
	if rowCount > 0 && !isNullable {
		if defaultValue == "" {
			// 如果没有提供默认值，根据字段类型设置默认值
			defaultValue = fs.getDefaultValueForType(fieldType)
		}
		formattedDefault := fs.formatDefaultValue(fieldType, defaultValue)
		sql.WriteString(fmt.Sprintf(" DEFAULT %s", formattedDefault))
		sql.WriteString(" NOT NULL")
	} else {
		// 表为空或字段允许为空
		if !isNullable {
			sql.WriteString(" NOT NULL")
		}
		// 添加默认值（如果提供了）
		if defaultValue != "" {
			formattedDefault := fs.formatDefaultValue(fieldType, defaultValue)
			sql.WriteString(fmt.Sprintf(" DEFAULT %s", formattedDefault))
		}
	}
	// 执行 SQL
	if err := models.DB.Exec(sql.String()).Error; err != nil {
		return fmt.Errorf("执行添加字段SQL失败: %v", err)
	}
	// PostgreSQL 需要单独添加注释
	if comment != "" {
		commentSQL := fmt.Sprintf(`COMMENT ON COLUMN "%s"."%s" IS '%s'`, tableName, fieldName, strings.ReplaceAll(comment, "'", "''"))
		if err := models.DB.Exec(commentSQL).Error; err != nil {
			return fmt.Errorf("添加字段注释失败: %v", err)
		}
	}
	return nil
}
func (fs *FieldService) getDefaultValueForType(fieldType string) string {
	fieldType = strings.ToLower(fieldType)
	mapping, exists := PostgreSQLToGDALTypeMap[fieldType]
	if !exists {
		return "''"
	}
	switch mapping.Category {
	case "integer":
		return "0"
	case "float":
		return "0.0"
	case "string":
		return "''"
	case "datetime":
		switch fieldType {
		case "date":
			return "CURRENT_DATE"
		case "time", "timetz":
			return "CURRENT_TIME"
		default:
			return "CURRENT_TIMESTAMP"
		}
	case "binary":
		return "'\\x00'"
	case "boolean":
		return "false"
	default:
		return "''"
	}
}

// formatDefaultValue 格式化默认值
func (fs *FieldService) formatDefaultValue(fieldType, defaultValue string) string {
	fieldType = strings.ToLower(fieldType)
	mapping, exists := PostgreSQLToGDALTypeMap[fieldType]
	if !exists {
		return fmt.Sprintf("'%s'", strings.ReplaceAll(defaultValue, "'", "''"))
	}
	switch mapping.Category {
	case "integer", "float":
		// 数值类型不需要引号
		return defaultValue
	case "boolean":
		// 布尔类型
		lower := strings.ToLower(defaultValue)
		if lower == "true" || lower == "1" || lower == "t" || lower == "yes" {
			return "true"
		}
		return "false"
	case "datetime":
		// 日期时间类型，检查是否是函数调用
		upper := strings.ToUpper(defaultValue)
		if strings.HasPrefix(upper, "CURRENT_") || strings.HasPrefix(upper, "NOW") {
			return defaultValue
		}
		return fmt.Sprintf("'%s'", strings.ReplaceAll(defaultValue, "'", "''"))
	default:
		// 字符串和其他类型需要引号
		return fmt.Sprintf("'%s'", strings.ReplaceAll(defaultValue, "'", "''"))
	}
}

// DeleteField 删除字段
func (fs *FieldService) DeleteField(tableName, fieldName string) error {
	sql := fmt.Sprintf(`ALTER TABLE "%s" DROP COLUMN "%s"`, tableName, fieldName)
	return models.DB.Exec(sql).Error
}

// isValidFieldType 验证字段类型是否有效
func (fs *FieldService) isValidFieldType(fieldType string) bool {
	fieldType = strings.ToLower(strings.TrimSpace(fieldType))
	_, exists := PostgreSQLToGDALTypeMap[fieldType]
	return exists
}
func (fs *FieldService) GetSupportedFieldTypes() map[string]models.FieldTypeInfo {
	return models.SupportedFieldTypes
}
func (fs *FieldService) GetFieldTypeCategory(fieldType string) string {
	fieldType = strings.ToLower(strings.TrimSpace(fieldType))
	if mapping, exists := PostgreSQLToGDALTypeMap[fieldType]; exists {
		return mapping.Category
	}
	return "unknown"
}

// needsQuotes 判断字段类型是否需要引号包围默认值
func (fs *FieldService) needsQuotes(fieldType string) bool {
	fieldType = strings.ToLower(fieldType)
	// int 和 float 不需要引号
	return fieldType != "int" && fieldType != "float"
}

// CheckTableExists 检查表是否存在
func (fs *FieldService) CheckTableExists(tableName string) bool {
	var count int64
	models.DB.Raw(`
		SELECT COUNT(*) 
		FROM information_schema.tables 
		WHERE table_schema = CURRENT_SCHEMA() 
		AND table_name = ?`, tableName).Scan(&count)
	return count > 0
}

// CheckFieldExists 检查字段是否存在
func (fs *FieldService) CheckFieldExists(tableName, fieldName string) bool {
	var count int64
	models.DB.Raw(`
		SELECT COUNT(*) 
		FROM information_schema.columns 
		WHERE table_schema = CURRENT_SCHEMA() 
		AND table_name = ? 
		AND column_name = ?`, tableName, fieldName).Scan(&count)
	return count > 0
}

// GetTableStructure 获取表结构信息
func (fs *FieldService) GetTableStructure(tableName string) (*models.TableStructure, error) {
	// 检查表是否存在
	if !fs.CheckTableExists(tableName) {
		return nil, fmt.Errorf("表 %s 不存在", tableName)
	}
	// 获取表基本信息
	tableInfo, err := fs.getTableInfo(tableName)
	if err != nil {
		return nil, err
	}
	// 获取字段信息
	fields, err := fs.getFieldsInfo(tableName)
	if err != nil {
		return nil, err
	}
	tableStructure := &models.TableStructure{
		TableName:    tableName,
		TableComment: tableInfo.Comment,
		Fields:       fields,
		FieldCount:   len(fields),
	}
	return tableStructure, nil
}

// getTableInfo 获取表基本信息
func (fs *FieldService) getTableInfo(tableName string) (*struct {
	Comment string
}, error) {
	var result struct {
		Comment string `gorm:"column:table_comment"`
	}

	sql := `
		SELECT 
			COALESCE(obj_description(c.oid), '') as table_comment
		FROM pg_class c
		LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = CURRENT_SCHEMA()
		AND c.relname = ?
		AND c.relkind = 'r'
	`

	err := models.DB.Raw(sql, tableName).Scan(&result).Error
	if err != nil {
		return nil, err
	}

	return &struct {
		Comment string
	}{
		Comment: result.Comment,
	}, nil
}

// getFieldsInfo 获取字段详细信息
func (fs *FieldService) getFieldsInfo(tableName string) ([]models.FieldInfo, error) {
	var fields []models.FieldInfo
	sql := `
		SELECT 
			c.column_name as field_name,
			c.data_type as data_type,
			CASE 
				WHEN c.character_maximum_length IS NOT NULL THEN
					c.data_type || '(' || c.character_maximum_length || ')'
				WHEN c.numeric_precision IS NOT NULL AND c.numeric_scale IS NOT NULL THEN
					c.data_type || '(' || c.numeric_precision || ',' || c.numeric_scale || ')'
				WHEN c.numeric_precision IS NOT NULL THEN
					c.data_type || '(' || c.numeric_precision || ')'
				ELSE c.data_type
			END as column_type,
			c.is_nullable,
			c.column_default as default_value,
			COALESCE(c.numeric_precision, 0) as numeric_precision,
			COALESCE(c.numeric_scale, 0) as numeric_scale,
			COALESCE(pgd.description, '') as comment,
			c.ordinal_position as position
		FROM information_schema.columns c
		LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
		LEFT JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace
		LEFT JOIN pg_attribute pga ON pga.attrelid = pgc.oid AND pga.attname = c.column_name
		LEFT JOIN pg_description pgd ON pgd.objoid = pgc.oid AND pgd.objsubid = pga.attnum
		WHERE c.table_schema = CURRENT_SCHEMA()
		AND c.table_name = ?
		ORDER BY c.ordinal_position
	`
	rows, err := models.DB.Raw(sql, tableName).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var field models.FieldInfo
		var defaultValue *string
		var pgDataType, pgColumnType string
		var numericPrecision, numericScale int
		err := rows.Scan(
			&field.FieldName,
			&pgDataType,
			&pgColumnType,
			&field.IsNullable,
			&defaultValue,
			&numericPrecision,
			&numericScale,
			&field.Comment,
			&field.Position,
		)
		if err != nil {
			return nil, err
		}
		// 转换为简化的字段类型
		field.FieldType, field.Length, field.Precision, field.Scale = fs.convertFromPostgreSQLType(pgColumnType)
		field.DefaultValue = defaultValue
		fields = append(fields, field)
	}
	return fields, nil
}

// GetSingleFieldInfo 获取单个字段信息
func (fs *FieldService) GetSingleFieldInfo(tableName, fieldName string) (*models.FieldInfo, error) {
	// 检查表是否存在
	if !fs.CheckTableExists(tableName) {
		return nil, fmt.Errorf("表 %s 不存在", tableName)
	}
	// 检查字段是否存在
	if !fs.CheckFieldExists(tableName, fieldName) {
		return nil, fmt.Errorf("字段 %s 不存在", fieldName)
	}
	var field models.FieldInfo
	var defaultValue *string
	var pgDataType, pgColumnType string
	var numericPrecision, numericScale int
	sql := `
		SELECT 
			c.column_name as field_name,
			c.data_type as data_type,
			CASE 
				WHEN c.character_maximum_length IS NOT NULL THEN
					c.data_type || '(' || c.character_maximum_length || ')'
				WHEN c.numeric_precision IS NOT NULL AND c.numeric_scale IS NOT NULL THEN
					c.data_type || '(' || c.numeric_precision || ',' || c.numeric_scale || ')'
				WHEN c.numeric_precision IS NOT NULL THEN
					c.data_type || '(' || c.numeric_precision || ')'
				ELSE c.data_type
			END as column_type,
			c.is_nullable,
			c.column_default as default_value,
			COALESCE(c.numeric_precision, 0) as numeric_precision,
			COALESCE(c.numeric_scale, 0) as numeric_scale,
			COALESCE(pgd.description, '') as comment,
			c.ordinal_position as position
		FROM information_schema.columns c
		LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
		LEFT JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace
		LEFT JOIN pg_attribute pga ON pga.attrelid = pgc.oid AND pga.attname = c.column_name
		LEFT JOIN pg_description pgd ON pgd.objoid = pgc.oid AND pgd.objsubid = pga.attnum
		WHERE c.table_schema = CURRENT_SCHEMA()
		AND c.table_name = ?
		AND c.column_name = ?
	`
	err := models.DB.Raw(sql, tableName, fieldName).Row().Scan(
		&field.FieldName,
		&pgDataType,
		&pgColumnType,
		&field.IsNullable,
		&defaultValue,
		&numericPrecision,
		&numericScale,
		&field.Comment,
		&field.Position,
	)
	if err != nil {
		return nil, err
	}
	// 转换为简化的字段类型
	field.FieldType, field.Length, field.Precision, field.Scale = fs.convertFromPostgreSQLType(pgColumnType)
	field.DefaultValue = defaultValue
	return &field, nil
}

// GetTableList 获取数据库中所有表的列表
func (fs *FieldService) GetTableList() ([]string, error) {
	var tables []string

	sql := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = CURRENT_SCHEMA()
		AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`

	err := models.DB.Raw(sql).Pluck("table_name", &tables).Error
	return tables, err
}

// BuildFieldTypeString 构建完整的字段类型字符串（用于记录）
func (fs *FieldService) BuildFieldTypeString(fieldType string, length, precision, scale int) string {
	fieldType = strings.ToLower(fieldType)
	switch fieldType {
	case "varchar", "character varying", "char", "character":
		if length > 0 {
			return fmt.Sprintf("%s(%d)", fieldType, length)
		}
		return fieldType
	case "numeric", "decimal":
		if precision > 0 && scale > 0 {
			return fmt.Sprintf("%s(%d,%d)", fieldType, precision, scale)
		} else if precision > 0 {
			return fmt.Sprintf("%s(%d)", fieldType, precision)
		}
		return fieldType
	default:
		return fieldType
	}
}

// SaveFieldRecord 保存字段操作记录
func (fs *FieldService) SaveFieldRecord(record *models.FieldRecord) error {
	return models.DB.Create(record).Error
}

// getFieldTypeInfo 获取字段的完整类型信息
func (fs *FieldService) getFieldTypeInfo(tableName, fieldName string) (string, error) {
	var columnType string
	sql := `
		SELECT 
			CASE 
				WHEN character_maximum_length IS NOT NULL THEN
					data_type || '(' || character_maximum_length || ')'
				WHEN numeric_precision IS NOT NULL AND numeric_scale IS NOT NULL THEN
					data_type || '(' || numeric_precision || ',' || numeric_scale || ')'
				WHEN numeric_precision IS NOT NULL THEN
					data_type || '(' || numeric_precision || ')'
				ELSE data_type
			END as column_type
		FROM information_schema.columns
		WHERE table_schema = CURRENT_SCHEMA()
		AND table_name = ?
		AND column_name = ?
	`
	err := models.DB.Raw(sql, tableName, fieldName).Scan(&columnType).Error
	if err != nil {
		return "", err
	}
	return columnType, nil
}
