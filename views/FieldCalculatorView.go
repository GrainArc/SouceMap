package views

import (
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/gin-gonic/gin"
	"net/http"
)

type FieldCalculatorController struct {
	calculatorService *methods.FieldCalculatorService
}

func NewFieldCalculatorController() *FieldCalculatorController {
	return &FieldCalculatorController{
		calculatorService: methods.NewFieldCalculatorService(),
	}
}

// CalculateField 执行字段计算
func (fc *FieldCalculatorController) CalculateField(c *gin.Context) {
	var req models.FieldCalculatorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "参数错误: " + err.Error(),
		})
		return
	}

	result, err := fc.calculatorService.CalculateField(req)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "计算失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, models.Response{
		Code:    200,
		Message: "字段计算成功",
		Data:    result,
	})
}

// PreviewCalculation 预览计算结果
func (fc *FieldCalculatorController) PreviewCalculation(c *gin.Context) {
	var req models.FieldCalculatorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "参数错误: " + err.Error(),
		})
		return
	}

	limit := 10 // 默认预览10条
	results, err := fc.calculatorService.PreviewCalculation(req, limit)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "预览失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, models.Response{
		Code:    200,
		Message: "预览成功",
		Data: map[string]interface{}{
			"preview_rows": results,
			"limit":        limit,
		},
	})
}
