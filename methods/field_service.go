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

// AddField 添加字段
func (fs *FieldService) AddField(tableName, fieldName, fieldType, defaultValue, comment string, isNullable bool) error {
	// 构建 SQL 语句
	var sql strings.Builder
	sql.WriteString(fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s", tableName, fieldName, fieldType))

	// 添加 NULL/NOT NULL 约束
	if isNullable {
		sql.WriteString(" NULL")
	} else {
		sql.WriteString(" NOT NULL")
	}

	// 添加默认值
	if defaultValue != "" {
		sql.WriteString(fmt.Sprintf(" DEFAULT '%s'", defaultValue))
	}

	// 添加注释
	if comment != "" {
		sql.WriteString(fmt.Sprintf(" COMMENT '%s'", comment))
	}

	// 执行 SQL
	return models.DB.Exec(sql.String()).Error
}

// DeleteField 删除字段
func (fs *FieldService) DeleteField(tableName, fieldName string) error {
	sql := fmt.Sprintf("ALTER TABLE `%s` DROP COLUMN `%s`", tableName, fieldName)
	return models.DB.Exec(sql).Error
}

// ModifyField 修改字段
func (fs *FieldService) ModifyField(tableName, oldFieldName, newFieldName, fieldType, defaultValue, comment string, isNullable bool) error {
	// 如果没有提供新字段名，使用原字段名
	if newFieldName == "" {
		newFieldName = oldFieldName
	}

	// 构建 SQL 语句
	var sql strings.Builder
	sql.WriteString(fmt.Sprintf("ALTER TABLE `%s` CHANGE COLUMN `%s` `%s` %s",
		tableName, oldFieldName, newFieldName, fieldType))

	// 添加 NULL/NOT NULL 约束
	if isNullable {
		sql.WriteString(" NULL")
	} else {
		sql.WriteString(" NOT NULL")
	}

	// 添加默认值
	if defaultValue != "" {
		sql.WriteString(fmt.Sprintf(" DEFAULT '%s'", defaultValue))
	}

	// 添加注释
	if comment != "" {
		sql.WriteString(fmt.Sprintf(" COMMENT '%s'", comment))
	}

	// 执行 SQL
	return models.DB.Exec(sql.String()).Error
}

// CheckTableExists 检查表是否存在
func (fs *FieldService) CheckTableExists(tableName string) bool {
	var count int64
	models.DB.Raw("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", tableName).Scan(&count)
	return count > 0
}

// CheckFieldExists 检查字段是否存在
func (fs *FieldService) CheckFieldExists(tableName, fieldName string) bool {
	var count int64
	models.DB.Raw("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?",
		tableName, fieldName).Scan(&count)
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
		Engine:       tableInfo.Engine,
		Charset:      tableInfo.Charset,
		Collation:    tableInfo.Collation,
		Fields:       fields,
		FieldCount:   len(fields),
	}

	return tableStructure, nil
}

// getTableInfo 获取表基本信息
func (fs *FieldService) getTableInfo(tableName string) (*struct {
	Comment   string
	Engine    string
	Charset   string
	Collation string
}, error) {
	var result struct {
		Comment   string `gorm:"column:TABLE_COMMENT"`
		Engine    string `gorm:"column:ENGINE"`
		Charset   string `gorm:"column:TABLE_COLLATION"`
		Collation string `gorm:"column:TABLE_COLLATION"`
	}

	sql := `
        SELECT 
            TABLE_COMMENT,
            ENGINE,
            TABLE_COLLATION
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = ?
    `

	err := models.DB.Raw(sql, tableName).Scan(&result).Error
	if err != nil {
		return nil, err
	}

	// 从 collation 中提取 charset
	if result.Charset != "" {
		parts := strings.Split(result.Charset, "_")
		if len(parts) > 0 {
			result.Charset = parts[0]
		}
	}

	return &struct {
		Comment   string
		Engine    string
		Charset   string
		Collation string
	}{
		Comment:   result.Comment,
		Engine:    result.Engine,
		Charset:   result.Charset,
		Collation: result.Collation,
	}, nil
}

// getFieldsInfo 获取字段详细信息
func (fs *FieldService) getFieldsInfo(tableName string) ([]models.FieldInfo, error) {
	var fields []models.FieldInfo

	sql := `
        SELECT 
            COLUMN_NAME as field_name,
            DATA_TYPE as data_type,
            COLUMN_TYPE as column_type,
            IS_NULLABLE as is_nullable,
            COLUMN_DEFAULT as default_value,
            EXTRA as extra,
            COLUMN_COMMENT as comment,
            ORDINAL_POSITION as position
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = ? 
        ORDER BY ORDINAL_POSITION
    `

	rows, err := models.DB.Raw(sql, tableName).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var field models.FieldInfo
		var defaultValue *string

		err := rows.Scan(
			&field.FieldName,
			&field.DataType,
			&field.ColumnType,
			&field.IsNullable,
			&defaultValue,
			&field.Extra,
			&field.Comment,
			&field.Position,
		)
		if err != nil {
			return nil, err
		}

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

	sql := `
        SELECT 
            COLUMN_NAME as field_name,
            DATA_TYPE as data_type,
            COLUMN_TYPE as column_type,
            IS_NULLABLE as is_nullable,
            COLUMN_DEFAULT as default_value,
            EXTRA as extra,
            COLUMN_COMMENT as comment,
            ORDINAL_POSITION as position
        FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = ? 
        AND COLUMN_NAME = ?
    `

	err := models.DB.Raw(sql, tableName, fieldName).Row().Scan(
		&field.FieldName,
		&field.DataType,
		&field.ColumnType,
		&field.IsNullable,
		&defaultValue,
		&field.Extra,
		&field.Comment,
		&field.Position,
	)

	if err != nil {
		return nil, err
	}

	field.DefaultValue = defaultValue
	return &field, nil
}

// GetTableList 获取数据库中所有表的列表
func (fs *FieldService) GetTableList() ([]string, error) {
	var tables []string

	sql := `
        SELECT TABLE_NAME 
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
    `

	err := models.DB.Raw(sql).Pluck("TABLE_NAME", &tables).Error
	return tables, err
}
