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

type GeometryHandler struct {
	service *methods.GeometryService
}

func NewGeometryHandler(service *methods.GeometryService) *GeometryHandler {
	return &GeometryHandler{service: service}
}

// UpdateGeometryField 批量更新几何计算字段
// POST /api/geometry/update-field
func (h *GeometryHandler) UpdateGeometryField(c *gin.Context) {

	var req models.GeometryUpdateRequest
	DB := models.DB
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// 参数校验
	if req.CalcType == models.CalcTypeArea && req.AreaType == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "area_type is required when calc_type is 'area'",
		})
		return
	}

	result, err := h.service.UpdateGeometryField(DB, c.Request.Context(), &req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}
