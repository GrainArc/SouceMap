package methods

import (
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"strings"
)

type FieldService struct{}

func NewFieldService() *FieldService {
	return &FieldService{}
}

// convertToPostgreSQLType 将简化的数据类型转换为PostgreSQL类型
func (fs *FieldService) convertToPostgreSQLType(fieldType string, length int) string {
	switch strings.ToLower(fieldType) {
	case "int":
		return "INTEGER"
	case "float":
		return "DOUBLE PRECISION"
	case "varchar":
		if length <= 0 {
			length = 255 // 默认长度
		}
		return fmt.Sprintf("VARCHAR(%d)", length)
	case "bytes":
		return "BYTEA"
	default:
		return "VARCHAR(255)" // 默认类型
	}
}

// convertFromPostgreSQLType 将PostgreSQL类型转换为简化类型
func (fs *FieldService) convertFromPostgreSQLType(pgType string) (fieldType string, length int) {
	pgType = strings.ToLower(pgType)

	if strings.Contains(pgType, "integer") || strings.Contains(pgType, "int4") ||
		strings.Contains(pgType, "bigint") || strings.Contains(pgType, "int8") ||
		strings.Contains(pgType, "smallint") || strings.Contains(pgType, "int2") ||
		strings.Contains(pgType, "serial") {
		return "int", 0
	}

	if strings.Contains(pgType, "double precision") || strings.Contains(pgType, "float8") ||
		strings.Contains(pgType, "real") || strings.Contains(pgType, "float4") ||
		strings.Contains(pgType, "decimal") || strings.Contains(pgType, "numeric") {
		return "float", 0
	}

	if strings.Contains(pgType, "varchar") {
		// 提取长度
		if strings.Contains(pgType, "(") && strings.Contains(pgType, ")") {
			start := strings.Index(pgType, "(") + 1
			end := strings.Index(pgType, ")")
			if start < end {
				lengthStr := pgType[start:end]
				var extractedLength int
				fmt.Sscanf(lengthStr, "%d", &extractedLength)
				return "varchar", extractedLength
			}
		}
		return "varchar", 255
	}

	if strings.Contains(pgType, "bytea") {
		return "bytes", 0
	}

	// 默认返回varchar
	return "varchar", 255
}

// AddField 添加字段
func (fs *FieldService) AddField(tableName, fieldName, fieldType string, length int, defaultValue, comment string, isNullable bool) error {
	// 验证字段类型
	if !fs.isValidFieldType(fieldType) {
		return fmt.Errorf("不支持的字段类型: %s，仅支持 int, float, varchar, bytes", fieldType)
	}

	// 转换为PostgreSQL类型
	pgType := fs.convertToPostgreSQLType(fieldType, length)

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

		if fs.needsQuotes(fieldType) {
			sql.WriteString(fmt.Sprintf(" DEFAULT '%s'", defaultValue))
		} else {
			sql.WriteString(fmt.Sprintf(" DEFAULT %s", defaultValue))
		}
		sql.WriteString(" NOT NULL")
	} else {
		// 表为空或字段允许为空
		if !isNullable {
			sql.WriteString(" NOT NULL")
		}

		// 添加默认值（如果提供了）
		if defaultValue != "" {
			if fs.needsQuotes(fieldType) {
				sql.WriteString(fmt.Sprintf(" DEFAULT '%s'", defaultValue))
			} else {
				sql.WriteString(fmt.Sprintf(" DEFAULT %s", defaultValue))
			}
		}
	}

	// 执行 SQL
	if err := models.DB.Exec(sql.String()).Error; err != nil {
		return fmt.Errorf("执行添加字段SQL失败: %v", err)
	}

	// PostgreSQL 需要单独添加注释
	if comment != "" {
		commentSQL := fmt.Sprintf(`COMMENT ON COLUMN "%s"."%s" IS '%s'`, tableName, fieldName, comment)
		if err := models.DB.Exec(commentSQL).Error; err != nil {
			return fmt.Errorf("添加字段注释失败: %v", err)
		}
	}

	return nil
}

// getDefaultValueForType 根据字段类型获取默认值
func (fs *FieldService) getDefaultValueForType(fieldType string) string {
	switch strings.ToLower(fieldType) {
	case "int":
		return "0"
	case "float":
		return "0.0"
	case "varchar":
		return ""
	case "bytes":
		return ""
	default:
		return ""
	}
}

// DeleteField 删除字段
func (fs *FieldService) DeleteField(tableName, fieldName string) error {
	sql := fmt.Sprintf(`ALTER TABLE "%s" DROP COLUMN "%s"`, tableName, fieldName)
	return models.DB.Exec(sql).Error
}

// ModifyField 修改字段
func (fs *FieldService) ModifyField(tableName, oldFieldName, newFieldName, fieldType string, length int, defaultValue, comment string, isNullable bool) error {
	// 验证字段类型
	if fieldType != "" && !fs.isValidFieldType(fieldType) {
		return fmt.Errorf("不支持的字段类型: %s，仅支持 int, float, varchar, bytes", fieldType)
	}

	// 如果没有提供新字段名，使用原字段名
	if newFieldName == "" {
		newFieldName = oldFieldName
	}

	var err error

	// 1. 修改字段类型
	if fieldType != "" {
		pgType := fs.convertToPostgreSQLType(fieldType, length)
		sql := fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" TYPE %s`, tableName, oldFieldName, pgType)
		if err = models.DB.Exec(sql).Error; err != nil {
			return err
		}
	}

	// 2. 修改 NULL/NOT NULL 约束
	var nullSQL string
	if isNullable {
		nullSQL = fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" DROP NOT NULL`, tableName, oldFieldName)
	} else {
		nullSQL = fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" SET NOT NULL`, tableName, oldFieldName)
	}
	if err = models.DB.Exec(nullSQL).Error; err != nil {
		return err
	}

	// 3. 修改默认值
	if defaultValue != "" {
		var defaultSQL string
		if fs.needsQuotes(fieldType) {
			defaultSQL = fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" SET DEFAULT '%s'`, tableName, oldFieldName, defaultValue)
		} else {
			defaultSQL = fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" SET DEFAULT %s`, tableName, oldFieldName, defaultValue)
		}
		if err = models.DB.Exec(defaultSQL).Error; err != nil {
			return err
		}
	}

	// 4. 重命名字段（如果需要）
	if newFieldName != oldFieldName {
		renameSQL := fmt.Sprintf(`ALTER TABLE "%s" RENAME COLUMN "%s" TO "%s"`, tableName, oldFieldName, newFieldName)
		if err = models.DB.Exec(renameSQL).Error; err != nil {
			return err
		}
	}

	// 5. 修改注释
	if comment != "" {
		commentSQL := fmt.Sprintf(`COMMENT ON COLUMN "%s"."%s" IS '%s'`, tableName, newFieldName, comment)
		if err = models.DB.Exec(commentSQL).Error; err != nil {
			return err
		}
	}

	return nil
}

// isValidFieldType 验证字段类型是否有效
func (fs *FieldService) isValidFieldType(fieldType string) bool {
	validTypes := []string{"int", "float", "varchar", "bytes"}
	fieldType = strings.ToLower(fieldType)
	for _, t := range validTypes {
		if t == fieldType {
			return true
		}
	}
	return false
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

		err := rows.Scan(
			&field.FieldName,
			&pgDataType,
			&pgColumnType,
			&field.IsNullable,
			&defaultValue,
			&field.Comment,
			&field.Position,
		)
		if err != nil {
			return nil, err
		}

		// 转换为简化的字段类型
		field.FieldType, field.Length = fs.convertFromPostgreSQLType(pgColumnType)
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
		&field.Comment,
		&field.Position,
	)

	if err != nil {
		return nil, err
	}

	// 转换为简化的字段类型
	field.FieldType, field.Length = fs.convertFromPostgreSQLType(pgColumnType)
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
