package views

import (
	"fmt"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/gin-gonic/gin"
	"net/http"
	"strings"
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

// 添加字段
func (uc *UserController) AddField(c *gin.Context) {
	var req models.FieldOperation
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "参数错误: " + err.Error(),
		})
		return
	}

	// 验证必要参数
	if req.FieldType == "" {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "字段类型不能为空",
		})
		return
	}

	// 验证varchar类型必须有长度
	if strings.ToLower(req.FieldType) == "varchar" && req.Length <= 0 {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "varchar类型必须指定长度参数",
		})
		return
	}

	// 检查表是否存在
	if !uc.fieldService.CheckTableExists(req.TableName) {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "表不存在",
		})
		return
	}

	// 检查字段是否已存在
	if uc.fieldService.CheckFieldExists(req.TableName, req.FieldName) {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "字段已存在",
		})
		return
	}

	// 检查表是否有数据，如果有数据且字段不允许为空，必须提供默认值
	var rowCount int64
	if err := models.DB.Raw(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, req.TableName)).Scan(&rowCount).Error; err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Code:    500,
			Message: "检查表数据失败: " + err.Error(),
		})
		return
	}

	if rowCount > 0 && !req.IsNullable && req.DefaultValue == "" {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "表中已有数据，添加非空字段时必须提供默认值",
		})
		return
	}

	// 添加字段
	err := uc.fieldService.AddField(req.TableName, req.FieldName, req.FieldType,
		req.Length, req.DefaultValue, req.Comment, req.IsNullable)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Code:    500,
			Message: "添加字段失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, models.Response{
		Code:    200,
		Message: "字段添加成功",
		Data: map[string]interface{}{
			"table_name": req.TableName,
			"field_name": req.FieldName,
			"field_type": req.FieldType,
			"length":     req.Length,
		},
	})
}

// 修改字段
func (uc *UserController) ModifyField(c *gin.Context) {
	var req models.FieldOperation
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "参数错误: " + err.Error(),
		})
		return
	}

	// 验证必要参数
	if req.FieldType == "" {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "字段类型不能为空",
		})
		return
	}

	// 验证varchar类型必须有长度
	if strings.ToLower(req.FieldType) == "varchar" && req.Length <= 0 {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "varchar类型必须指定长度参数",
		})
		return
	}

	// 检查表是否存在
	if !uc.fieldService.CheckTableExists(req.TableName) {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "表不存在",
		})
		return
	}

	// 检查原字段是否存在
	if !uc.fieldService.CheckFieldExists(req.TableName, req.FieldName) {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "原字段不存在",
		})
		return
	}

	// 如果要重命名字段，检查新字段名是否已存在
	if req.NewFieldName != "" && req.NewFieldName != req.FieldName {
		if uc.fieldService.CheckFieldExists(req.TableName, req.NewFieldName) {
			c.JSON(http.StatusBadRequest, models.Response{
				Code:    400,
				Message: "新字段名已存在",
			})
			return
		}
	}

	// 修改字段
	err := uc.fieldService.ModifyField(req.TableName, req.FieldName, req.NewFieldName,
		req.FieldType, req.Length, req.DefaultValue, req.Comment, req.IsNullable)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Code:    500,
			Message: "修改字段失败: " + err.Error(),
		})
		return
	}

	finalFieldName := req.NewFieldName
	if finalFieldName == "" {
		finalFieldName = req.FieldName
	}

	c.JSON(http.StatusOK, models.Response{
		Code:    200,
		Message: "字段修改成功",
		Data: map[string]interface{}{
			"table_name":     req.TableName,
			"old_field_name": req.FieldName,
			"new_field_name": finalFieldName,
			"field_type":     req.FieldType,
			"length":         req.Length,
		},
	})
}

// 删除字段
func (uc *UserController) DeleteField(c *gin.Context) {
	var req models.FieldOperation
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "参数错误: " + err.Error(),
		})
		return
	}

	// 检查表是否存在
	if !uc.fieldService.CheckTableExists(req.TableName) {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "表不存在",
		})
		return
	}

	// 检查字段是否存在
	if !uc.fieldService.CheckFieldExists(req.TableName, req.FieldName) {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "字段不存在",
		})
		return
	}

	// 删除字段
	err := uc.fieldService.DeleteField(req.TableName, req.FieldName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Code:    500,
			Message: "删除字段失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, models.Response{
		Code:    200,
		Message: "字段删除成功",
		Data: map[string]interface{}{
			"table_name": req.TableName,
			"field_name": req.FieldName,
		},
	})
}

// GetFieldInfo 获取单个字段信息接口
func (fc *UserController) GetFieldInfo(c *gin.Context) {
	tableName := c.Query("table_name")
	fieldName := c.Query("field_name")

	if tableName == "" || fieldName == "" {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "表名和字段名不能为空",
		})
		return
	}

	fieldInfo, err := fc.fieldService.GetSingleFieldInfo(tableName, fieldName)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, models.Response{
		Code:    200,
		Message: "获取字段信息成功",
		Data:    fieldInfo,
	})
}
