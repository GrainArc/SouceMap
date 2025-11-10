package views

import (
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"net/http"
	"strconv"
	"strings"
)

// 统计请求参数
type StatsRequest struct {
	Table      string      `json:"table" binding:"required"`      // 表名
	StatField  string      `json:"stat_field" binding:"required"` // 要统计的字段
	StatTypes  []string    `json:"stat_types" binding:"required"` // 统计类型：count, min, max, sum, avg, stddev
	GroupBy    []string    `json:"group_by"`                      // 分组字段
	Conditions []Condition `json:"conditions"`                    // 查询条件
}

// 查询条件
type Condition struct {
	Field    string      `json:"field"`
	Operator string      `json:"operator"` // =, >, <, >=, <=, !=, like, in
	Value    interface{} `json:"value"`
}

// 统计结果
type StatsResult struct {
	GroupValues map[string]interface{} `json:"group_values,omitempty"` // 分组字段值
	Count       *int64                 `json:"count,omitempty"`
	Min         *float64               `json:"min,omitempty"`
	Max         *float64               `json:"max,omitempty"`
	Sum         *float64               `json:"sum,omitempty"`
	Avg         *float64               `json:"avg,omitempty"`
	Stddev      *float64               `json:"stddev,omitempty"`
}

// 统计响应
type StatsResponse struct {
	Success bool          `json:"success"`
	Message string        `json:"message"`
	Data    []StatsResult `json:"data"`
}

// 字段统计，统计模式包括：计数、最小值、最大值、和、平均值、标准差，并支持按照其他字段值进行分组
func (uc *UserController) LayerStatistics(c *gin.Context) {
	DB := models.DB

	// 解析请求参数
	var req StatsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, StatsResponse{
			Success: false,
			Message: fmt.Sprintf("参数解析失败: %v", err),
		})
		return
	}

	// 验证统计类型
	validStatTypes := map[string]bool{
		"count": true, "min": true, "max": true,
		"sum": true, "avg": true, "stddev": true,
	}
	for _, statType := range req.StatTypes {
		if !validStatTypes[statType] {
			c.JSON(http.StatusBadRequest, StatsResponse{
				Success: false,
				Message: fmt.Sprintf("不支持的统计类型: %s", statType),
			})
			return
		}
	}

	// 构建查询
	query := DB.Table(req.Table)

	// 应用查询条件
	for _, cond := range req.Conditions {
		query = applyCondition(query, cond)
	}

	// 构建SELECT子句
	selectFields := buildSelectFields(req.StatField, req.StatTypes, req.GroupBy)
	query = query.Select(selectFields)

	// 应用分组
	if len(req.GroupBy) > 0 {
		query = query.Group(strings.Join(req.GroupBy, ", "))
	}

	// 执行查询
	var results []map[string]interface{}
	if err := query.Find(&results).Error; err != nil {
		c.JSON(http.StatusInternalServerError, StatsResponse{
			Success: false,
			Message: fmt.Sprintf("查询失败: %v", err),
		})
		return
	}

	// 转换结果
	statsResults := convertResults(results, req.StatTypes, req.GroupBy)

	c.JSON(http.StatusOK, StatsResponse{
		Success: true,
		Message: "统计成功",
		Data:    statsResults,
	})
}

// 应用查询条件
func applyCondition(query *gorm.DB, cond Condition) *gorm.DB {
	switch strings.ToLower(cond.Operator) {
	case "=":
		return query.Where(fmt.Sprintf("%s = ?", cond.Field), cond.Value)
	case ">":
		return query.Where(fmt.Sprintf("%s > ?", cond.Field), cond.Value)
	case "<":
		return query.Where(fmt.Sprintf("%s < ?", cond.Field), cond.Value)
	case ">=":
		return query.Where(fmt.Sprintf("%s >= ?", cond.Field), cond.Value)
	case "<=":
		return query.Where(fmt.Sprintf("%s <= ?", cond.Field), cond.Value)
	case "!=", "<>":
		return query.Where(fmt.Sprintf("%s != ?", cond.Field), cond.Value)
	case "like":
		return query.Where(fmt.Sprintf("%s LIKE ?", cond.Field), cond.Value)
	case "in":
		return query.Where(fmt.Sprintf("%s IN ?", cond.Field), cond.Value)
	default:
		return query
	}
}

// 构建SELECT字段（支持字符串转数值）
func buildSelectFields(statField string, statTypes []string, groupBy []string) string {
	var fields []string

	// 添加分组字段
	for _, field := range groupBy {
		fields = append(fields, field)
	}

	// 构建字段表达式：尝试将字符串转换为数值，失败则为NULL
	fieldExpr := fmt.Sprintf(`
		CASE 
			WHEN %s ~ '^-?[0-9]+\.?[0-9]*([eE][+-]?[0-9]+)?$' 
			THEN CAST(%s AS DOUBLE PRECISION)
			ELSE NULL 
		END`, statField, statField)

	// 添加统计字段
	for _, statType := range statTypes {
		switch statType {
		case "count":
			// count统计可转换为数值的记录数
			fields = append(fields, fmt.Sprintf("COUNT(%s) as count", fieldExpr))
		case "min":
			fields = append(fields, fmt.Sprintf("MIN(%s) as min", fieldExpr))
		case "max":
			fields = append(fields, fmt.Sprintf("MAX(%s) as max", fieldExpr))
		case "sum":
			fields = append(fields, fmt.Sprintf("SUM(%s) as sum", fieldExpr))
		case "avg":
			fields = append(fields, fmt.Sprintf("AVG(%s) as avg", fieldExpr))
		case "stddev":
			fields = append(fields, fmt.Sprintf("STDDEV_SAMP(%s) as stddev", fieldExpr))
		}
	}

	return strings.Join(fields, ", ")
}

// 转换查询结果
func convertResults(results []map[string]interface{}, statTypes []string, groupBy []string) []StatsResult {
	statsResults := make([]StatsResult, 0, len(results))

	for _, row := range results {
		result := StatsResult{}

		// 处理分组字段
		if len(groupBy) > 0 {
			result.GroupValues = make(map[string]interface{})
			for _, field := range groupBy {
				if val, ok := row[field]; ok {
					result.GroupValues[field] = val
				}
			}
		}

		// 处理统计值
		for _, statType := range statTypes {
			switch statType {
			case "count":
				if val, ok := row["count"]; ok && val != nil {
					count := convertToInt64(val)
					result.Count = &count
				}
			case "min":
				if val, ok := row["min"]; ok && val != nil {
					min := convertToFloat64(val)
					result.Min = &min
				}
			case "max":
				if val, ok := row["max"]; ok && val != nil {
					max := convertToFloat64(val)
					result.Max = &max
				}
			case "sum":
				if val, ok := row["sum"]; ok && val != nil {
					sum := convertToFloat64(val)
					result.Sum = &sum
				}
			case "avg":
				if val, ok := row["avg"]; ok && val != nil {
					avg := convertToFloat64(val)
					result.Avg = &avg
				}
			case "stddev":
				if val, ok := row["stddev"]; ok && val != nil {
					stddev := convertToFloat64(val)
					result.Stddev = &stddev
				}
			}
		}

		statsResults = append(statsResults, result)
	}

	return statsResults
}

// 转换为int64
func convertToInt64(val interface{}) int64 {
	if val == nil {
		return 0
	}

	switch v := val.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	case string:
		if i, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return int64(f)
		}
		return 0
	case []uint8:
		str := strings.TrimSpace(string(v))
		if i, err := strconv.ParseInt(str, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(str, 64); err == nil {
			return int64(f)
		}
		return 0
	default:
		return 0
	}
}

// 转换为float64
func convertToFloat64(val interface{}) float64 {
	if val == nil {
		return 0
	}

	switch v := val.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int64:
		return float64(v)
	case int:
		return float64(v)
	case int32:
		return float64(v)
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return f
		}
		return 0
	case []uint8:
		if f, err := strconv.ParseFloat(strings.TrimSpace(string(v)), 64); err == nil {
			return f
		}
		return 0
	default:
		str := fmt.Sprintf("%v", v)
		if f, err := strconv.ParseFloat(strings.TrimSpace(str), 64); err == nil {
			return f
		}
		return 0
	}
}
