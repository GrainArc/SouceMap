package methods

import (
	"errors"
	"fmt"
	"github.com/GrainArc/SouceMap/models"
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

	sql := fmt.Sprintf(`UPDATE "%s" SET "%s" = %s`, req.TableName, req.TargetField, calcExpr)

	if req.Condition != "" {
		sql += " WHERE " + req.Condition
	}

	return sql, nil
}

// buildExpression 递归构建计算表达式
func (s *FieldCalculatorService) buildExpression(tableName string, expr *models.CalculateExpression) (string, error) {
	switch expr.Type {
	case "value":
		// 直接值
		return s.formatValue(expr.Value), nil

	case "field":
		// 字段引用
		if err := s.validateField(tableName, expr.Field); err != nil {
			return "", err
		}
		return fmt.Sprintf(`"%s"`, expr.Field), nil

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

// PreviewCalculation 预览计算结果(不实际执行)
func (s *FieldCalculatorService) PreviewCalculation(req models.FieldCalculatorRequest, limit int) ([]map[string]interface{}, error) {
	// 构建预览SQL
	var calcExpr string
	var err error

	switch req.OperationType {
	case "assign":
		if req.Expression == nil || req.Expression.Value == nil {
			return nil, errors.New("赋值操作需要提供value")
		}
		calcExpr = s.formatValue(req.Expression.Value)

	case "copy":
		if req.Expression == nil || req.Expression.Field == "" {
			return nil, errors.New("复制操作需要提供源字段")
		}
		calcExpr = fmt.Sprintf(`"%s"`, req.Expression.Field)

	case "concat":
		if req.Expression == nil || len(req.Expression.Fields) == 0 {
			return nil, errors.New("组合操作需要提供至少一个字段")
		}
		separator := req.Expression.Separator
		var concatParts []string
		for i, field := range req.Expression.Fields {
			concatParts = append(concatParts, fmt.Sprintf(`COALESCE("%s"::text, '')`, field))
			if i < len(req.Expression.Fields)-1 && separator != "" {
				concatParts = append(concatParts, fmt.Sprintf(`'%s'`, separator))
			}
		}
		calcExpr = strings.Join(concatParts, " || ")

	case "calculate":
		calcExpr, err = s.buildExpression(req.TableName, req.Expression)
		if err != nil {
			return nil, err
		}
	}

	// 构建预览查询
	sql := fmt.Sprintf(`SELECT *, (%s) as calculated_value FROM "%s"`, calcExpr, req.TableName)
	if req.Condition != "" {
		sql += " WHERE " + req.Condition
	}
	sql += fmt.Sprintf(" LIMIT %d", limit)

	// 执行查询
	var results []map[string]interface{}
	rows, err := models.DB.Raw(sql).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		rows.Scan(valuePtrs...)

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}

	return results, nil
}
