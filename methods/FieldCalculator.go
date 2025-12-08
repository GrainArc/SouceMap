package methods

import (
	"context"
	"errors"
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"gorm.io/gorm"
	"strconv"
	"strings"
)

type FieldCalculatorService struct{}

func NewFieldCalculatorService() *FieldCalculatorService {
	return &FieldCalculatorService{}
}

// CalculateField 执行字段计算
func (s *FieldCalculatorService) CalculateField(req models.FieldCalculatorRequest) (*models.FieldCalculatorResponse, error) {
	// 1. 验证表和字段是否存在
	if err := s.validateTableAndField(req.TableName, req.TargetField); err != nil {
		return nil, err
	}

	// 2. 根据操作类型构建SQL
	var sqlStatement string
	var err error

	switch req.OperationType {
	case "assign":
		sqlStatement, err = s.buildAssignSQL(req)
	case "copy":
		sqlStatement, err = s.buildCopySQL(req)
	case "concat":
		sqlStatement, err = s.buildConcatSQL(req)
	case "calculate":
		sqlStatement, err = s.buildCalculateSQL(req)
	default:
		return nil, errors.New("不支持的操作类型")
	}

	if err != nil {
		return nil, err
	}

	// 3. 执行SQL
	result := models.DB.Exec(sqlStatement)
	if result.Error != nil {
		return nil, fmt.Errorf("执行SQL失败: %v", result.Error)
	}

	return &models.FieldCalculatorResponse{
		TableName:     req.TableName,
		TargetField:   req.TargetField,
		OperationType: req.OperationType,
		AffectedRows:  result.RowsAffected,
		SQLStatement:  sqlStatement,
	}, nil
}

// buildAssignSQL 构建直接赋值SQL
func (s *FieldCalculatorService) buildAssignSQL(req models.FieldCalculatorRequest) (string, error) {
	if req.Expression == nil || req.Expression.Value == nil {
		return "", errors.New("赋值操作需要提供value")
	}

	value := s.formatValue(req.Expression.Value)
	sql := fmt.Sprintf(`UPDATE "%s" SET "%s" = %s`, req.TableName, req.TargetField, value)

	if req.Condition != "" {
		sql += " WHERE " + req.Condition
	}

	return sql, nil
}

// buildCopySQL 构建字段复制SQL
func (s *FieldCalculatorService) buildCopySQL(req models.FieldCalculatorRequest) (string, error) {
	if req.Expression == nil || req.Expression.Field == "" {
		return "", errors.New("复制操作需要提供源字段")
	}

	// 验证源字段是否存在
	if err := s.validateField(req.TableName, req.Expression.Field); err != nil {
		return "", err
	}

	sql := fmt.Sprintf(`UPDATE "%s" SET "%s" = "%s"`,
		req.TableName, req.TargetField, req.Expression.Field)

	if req.Condition != "" {
		sql += " WHERE " + req.Condition
	}

	return sql, nil
}

// buildConcatSQL 构建字段组合SQL
func (s *FieldCalculatorService) buildConcatSQL(req models.FieldCalculatorRequest) (string, error) {
	if req.Expression == nil || len(req.Expression.Fields) == 0 {
		return "", errors.New("组合操作需要提供至少一个字段")
	}

	// 验证所有字段是否存在
	for _, field := range req.Expression.Fields {
		if err := s.validateField(req.TableName, field); err != nil {
			return "", err
		}
	}

	separator := req.Expression.Separator
	if separator == "" {
		separator = "" // 默认无分隔符
	}

	// 构建CONCAT表达式
	var concatParts []string
	for i, field := range req.Expression.Fields {
		// 使用COALESCE处理NULL值
		concatParts = append(concatParts, fmt.Sprintf(`COALESCE("%s"::text, '')`, field))

		// 添加分隔符(除了最后一个字段)
		if i < len(req.Expression.Fields)-1 && separator != "" {
			concatParts = append(concatParts, fmt.Sprintf(`'%s'`, separator))
		}
	}

	concatExpr := strings.Join(concatParts, " || ")
	sql := fmt.Sprintf(`UPDATE "%s" SET "%s" = %s`, req.TableName, req.TargetField, concatExpr)

	if req.Condition != "" {
		sql += " WHERE " + req.Condition
	}

	return sql, nil
}

// buildCalculateSQL 构建数学计算SQL
func (s *FieldCalculatorService) buildCalculateSQL(req models.FieldCalculatorRequest) (string, error) {
	if req.Expression == nil {
		return "", errors.New("计算操作需要提供表达式")
	}

	// 递归构建计算表达式
	calcExpr, err := s.buildExpression(req.TableName, req.Expression)
	if err != nil {
		return "", err
	}

	// 包装整个计算表达式，确保结果也是数字类型
	sql := fmt.Sprintf(`UPDATE "%s" SET "%s" = %s`, req.TableName, req.TargetField, calcExpr)

	if req.Condition != "" {
		sql += " WHERE " + req.Condition
	}

	return sql, nil
}

// buildExpression 递归构建计算表达式
// buildExpression 递归构建计算表达式
func (s *FieldCalculatorService) buildExpression(tableName string, expr *models.CalculateExpression) (string, error) {
	switch expr.Type {
	case "value":
		// 直接值 - 确保是数字类型
		return s.formatNumericValue(expr.Value)

	case "field":
		// 字段引用 - 转换为数字类型
		if err := s.validateField(tableName, expr.Field); err != nil {
			return "", err
		}
		// 使用 CAST 或 :: 语法将字段转换为 NUMERIC 类型
		// 使用 NULLIF 处理空字符串，避免转换错误
		return s.buildFieldToNumeric(expr.Field), nil

	case "expression":
		// 复合表达式
		if expr.Left == nil || expr.Right == nil {
			return "", errors.New("表达式需要左右两个操作数")
		}

		if !s.isValidOperator(expr.Operator) {
			return "", fmt.Errorf("不支持的运算符: %s", expr.Operator)
		}

		leftExpr, err := s.buildExpression(tableName, expr.Left)
		if err != nil {
			return "", err
		}

		rightExpr, err := s.buildExpression(tableName, expr.Right)
		if err != nil {
			return "", err
		}

		return fmt.Sprintf("(%s %s %s)", leftExpr, expr.Operator, rightExpr), nil

	default:
		return "", fmt.Errorf("不支持的表达式类型: %s", expr.Type)
	}
}

// buildFieldToNumeric 将字段转换为数字类型的SQL表达式
func (s *FieldCalculatorService) buildFieldToNumeric(fieldName string) string {
	// 使用 CASE 语句进行安全转换
	// 1. 先 TRIM 去除空格
	// 2. 使用 NULLIF 将空字符串转为 NULL
	// 3. 使用正则表达式验证是否为有效数字格式
	// 4. 转换为 NUMERIC 类型
	return fmt.Sprintf(`
		CASE 
			WHEN TRIM(COALESCE("%s"::text, '')) = '' THEN 0
			WHEN TRIM("%s"::text) ~ '^-?[0-9]+\.?[0-9]*$' THEN TRIM("%s"::text)::numeric
			ELSE (
				SELECT CASE 
					WHEN TRIM("%s"::text) ~ '^-?[0-9]+\.?[0-9]*$' THEN NULL
					ELSE NULL / 0  -- 这会触发除零错误，提示转换失败
				END
			)
		END`,
		fieldName, fieldName, fieldName, fieldName)
}

// buildFieldToNumericSimple 简化版本 - 直接转换,失败则报错
func (s *FieldCalculatorService) buildFieldToNumericSimple(fieldName string) string {
	// 更简洁的版本：直接尝试转换,PostgreSQL会在转换失败时抛出错误
	return fmt.Sprintf(`
		(CASE 
			WHEN TRIM(COALESCE("%s"::text, '')) = '' THEN 0
			ELSE CAST(TRIM("%s"::text) AS numeric)
		END)`,
		fieldName, fieldName)
}

// formatNumericValue 格式化数字值
func (s *FieldCalculatorService) formatNumericValue(value interface{}) (string, error) {
	switch v := value.(type) {
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", v), nil
	case string:
		// 验证字符串是否为有效数字
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			return "", fmt.Errorf("值 '%s' 不是有效的数字", v)
		}
		return v, nil
	case nil:
		return "0", nil // 或者返回 "NULL"，根据业务需求
	default:
		return "", fmt.Errorf("不支持的数值类型: %T", v)
	}
}

// formatValue 格式化值
func (s *FieldCalculatorService) formatValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		return fmt.Sprintf("%t", v)
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("'%v'", v)
	}
}

// isValidOperator 验证运算符
func (s *FieldCalculatorService) isValidOperator(operator string) bool {
	validOperators := []string{"+", "-", "*", "/", "%"}
	for _, op := range validOperators {
		if operator == op {
			return true
		}
	}
	return false
}

// validateTableAndField 验证表和字段
func (s *FieldCalculatorService) validateTableAndField(tableName, fieldName string) error {
	// 检查表是否存在
	var exists bool
	err := models.DB.Raw(`
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = ?
        )
    `, tableName).Scan(&exists).Error

	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("表 %s 不存在", tableName)
	}

	// 检查字段是否存在
	return s.validateField(tableName, fieldName)
}

// validateField 验证字段
func (s *FieldCalculatorService) validateField(tableName, fieldName string) error {
	var exists bool
	err := models.DB.Raw(`
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = ? AND column_name = ?
        )
    `, tableName, fieldName).Scan(&exists).Error

	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("字段 %s 在表 %s 中不存在", fieldName, tableName)
	}
	return nil
}

type GeometryService struct {
}

// UpdateGeometryField 批量更新几何计算字段
func (s *GeometryService) UpdateGeometryField(DB *gorm.DB, ctx context.Context, req *models.GeometryUpdateRequest) (*models.GeometryUpdateResponse, error) {
	// 设置默认几何字段名
	if req.GeomField == "" {
		req.GeomField = "geom"
	}

	// 验证标识符
	if err := s.validateIdentifier(req.TableName); err != nil {
		return nil, fmt.Errorf("invalid table_name: %w", err)
	}
	if err := s.validateIdentifier(req.TargetField); err != nil {
		return nil, fmt.Errorf("invalid target_field: %w", err)
	}
	if err := s.validateIdentifier(req.GeomField); err != nil {
		return nil, fmt.Errorf("invalid geom_field: %w", err)
	}

	// 构建更新SQL
	updateSQL, err := s.buildUpdateSQL(req)
	if err != nil {
		return nil, err
	}

	// 执行更新
	db, _ := DB.DB()
	result, err := db.ExecContext(ctx, updateSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to update geometry field: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()

	return &models.GeometryUpdateResponse{
		TableName:    req.TableName,
		TargetField:  req.TargetField,
		CalcType:     string(req.CalcType),
		RowsAffected: rowsAffected,
		Success:      true,
		Message:      fmt.Sprintf("Successfully updated %d rows", rowsAffected),
	}, nil
}

// buildUpdateSQL 构建更新SQL
func (s *GeometryService) buildUpdateSQL(req *models.GeometryUpdateRequest) (string, error) {
	var calcSQL string

	switch req.CalcType {
	case models.CalcTypeArea:
		if req.AreaType == "" {
			return "", fmt.Errorf("area_type is required for area calculation")
		}
		calcSQL = s.buildAreaSQL(req.GeomField, req.AreaType)

	case models.CalcTypePerimeter:
		calcSQL = s.buildPerimeterSQL(req.GeomField)

	case models.CalcTypeCentroidX:
		calcSQL = s.buildCentroidXSQL(req.GeomField)

	case models.CalcTypeCentroidY:
		calcSQL = s.buildCentroidYSQL(req.GeomField)

	default:
		return "", fmt.Errorf("unsupported calc_type: %s", req.CalcType)
	}

	// 构建UPDATE语句
	updateSQL := fmt.Sprintf(`
        UPDATE "%s"
        SET "%s" = %s
    `, req.TableName, req.TargetField, calcSQL)

	// 添加WHERE子句（如果有）
	if req.WhereClause != "" {
		updateSQL += fmt.Sprintf("\nWHERE %s", req.WhereClause)
	}

	return updateSQL, nil
}

// buildAreaSQL 构建面积计算SQL
func (s *GeometryService) buildAreaSQL(geomField string, areaType models.AreaType) string {
	switch areaType {
	case models.AreaTypePlanar:
		// 平面面积 - 使用CGCS2000 3度带投影
		return fmt.Sprintf(`
            ST_Area(
                CASE 
                    WHEN ST_SRID(%s) = 4326 THEN ST_Transform(%s, 4523)
                    WHEN ST_SRID(%s) = 4490 THEN ST_Transform(%s, 4523)
                    ELSE %s
                END
            )`, geomField, geomField, geomField, geomField, geomField)

	case models.AreaTypeEllipsoid:
		// 椭球面积 - 使用CGCS2000地理坐标系
		return fmt.Sprintf(`
            ST_Area(
                CASE 
                    WHEN ST_SRID(%s) = 4326 THEN ST_Transform(%s, 4490)::geography
                    WHEN ST_SRID(%s) = 4490 THEN %s::geography
                    ELSE ST_Transform(ST_SetSRID(%s, 4326), 4490)::geography
                END
            )`, geomField, geomField, geomField, geomField, geomField)

	default:
		return fmt.Sprintf("ST_Area(%s::geography)", geomField)
	}
}

// buildPerimeterSQL 构建周长计算SQL
func (s *GeometryService) buildPerimeterSQL(geomField string) string {
	return fmt.Sprintf(`
        ST_Perimeter(
            CASE 
                WHEN ST_SRID(%s) = 4326 THEN ST_Transform(%s, 4490)::geography
                WHEN ST_SRID(%s) = 4490 THEN %s::geography
                ELSE ST_Transform(ST_SetSRID(%s, 4326), 4490)::geography
            END
        )`, geomField, geomField, geomField, geomField, geomField)
}

// buildCentroidXSQL 构建中心点X坐标(经度)计算SQL
func (s *GeometryService) buildCentroidXSQL(geomField string) string {
	return fmt.Sprintf(`
        ST_X(
            ST_Transform(
                ST_Centroid(%s), 
                4326
            )
        )`, geomField)
}

// buildCentroidYSQL 构建中心点Y坐标(纬度)计算SQL
func (s *GeometryService) buildCentroidYSQL(geomField string) string {
	return fmt.Sprintf(`
        ST_Y(
            ST_Transform(
                ST_Centroid(%s), 
                4326
            )
        )`, geomField)
}

// validateIdentifier 验证标识符（防止SQL注入）
func (s *GeometryService) validateIdentifier(identifier string) error {
	if identifier == "" {
		return fmt.Errorf("identifier cannot be empty")
	}

	// 只允许字母、数字、下划线
	for _, r := range identifier {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_') {
			return fmt.Errorf("invalid character in identifier: %s", identifier)
		}
	}

	// 不能以数字开头
	if identifier[0] >= '0' && identifier[0] <= '9' {
		return fmt.Errorf("identifier cannot start with a number: %s", identifier)
	}

	return nil
}

// PreviewUpdateSQL 预览将要执行的SQL（用于调试）
func (s *GeometryService) PreviewUpdateSQL(req *models.GeometryUpdateRequest) (string, error) {
	if req.GeomField == "" {
		req.GeomField = "geom"
	}

	if err := s.validateIdentifier(req.TableName); err != nil {
		return "", err
	}
	if err := s.validateIdentifier(req.TargetField); err != nil {
		return "", err
	}
	if err := s.validateIdentifier(req.GeomField); err != nil {
		return "", err
	}

	return s.buildUpdateSQL(req)
}

// GetUpdateStatistics 获取更新统计信息（更新前预览）
func (s *GeometryService) GetUpdateStatistics(DB *gorm.DB, ctx context.Context, req *models.GeometryUpdateRequest) (*UpdateStatistics, error) {
	if req.GeomField == "" {
		req.GeomField = "geom"
	}

	// 验证标识符
	if err := s.validateIdentifier(req.TableName); err != nil {
		return nil, err
	}
	if err := s.validateIdentifier(req.GeomField); err != nil {
		return nil, err
	}

	// 构建统计查询
	whereClause := ""
	if req.WhereClause != "" {
		whereClause = fmt.Sprintf("WHERE %s", req.WhereClause)
	}

	statsSQL := fmt.Sprintf(`
        SELECT 
            COUNT(*) as total_rows,
            COUNT(%s) as geom_not_null,
            COUNT(*) - COUNT(%s) as geom_null
        FROM %s
        %s
    `, req.GeomField, req.GeomField, req.TableName, whereClause)

	var stats UpdateStatistics
	db, _ := DB.DB()
	err := db.QueryRowContext(ctx, statsSQL).Scan(
		&stats.TotalRows,
		&stats.GeomNotNull,
		&stats.GeomNull,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get statistics: %w", err)
	}

	stats.TableName = req.TableName
	stats.TargetField = req.TargetField

	return &stats, nil
}

// UpdateStatistics 更新统计信息
type UpdateStatistics struct {
	TableName   string `json:"table_name"`
	TargetField string `json:"target_field"`
	TotalRows   int64  `json:"total_rows"`
	GeomNotNull int64  `json:"geom_not_null"`
	GeomNull    int64  `json:"geom_null"`
}
