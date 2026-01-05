package views

import (
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"github.com/gin-gonic/gin"
	"net/http"
	"strings"
)

// CalculateField 执行字段计算
func (uc *UserController) CalculateField(c *gin.Context) {
	var req models.FieldCalculatorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "参数错误: " + err.Error(),
		})
		return
	}

	result, err := uc.calculatorService.CalculateField(req)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "计算失败: " + err.Error(),
		})
		return
	}
	record := &models.FieldRecord{
		TableName:    req.TableName,
		Type:         "value", // 操作类型：删除
		OldFieldName: req.TargetField,
	}

	if err := uc.fieldService.SaveFieldRecord(record); err != nil {
		// 记录保存失败，只记录日志，不影响主流程
		fmt.Printf("保存字段操作记录失败: %v\n", err)
	}

	c.JSON(http.StatusOK, models.Response{
		Code:    200,
		Message: "字段计算成功",
		Data:    result,
	})
}

// UpdateGeometryField 批量更新几何计算字段
// POST /api/geometry/update-field
func (uc *UserController) UpdateGeometryField(c *gin.Context) {

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

	result, err := uc.service.UpdateGeometryField(DB, c.Request.Context(), &req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// 保存字段操作记录
	record := &models.FieldRecord{
		TableName:    req.TableName,
		Type:         "value", // 操作类型：删除
		OldFieldName: req.TargetField,
	}

	if err := uc.fieldService.SaveFieldRecord(record); err != nil {
		// 记录保存失败，只记录日志，不影响主流程
		fmt.Printf("保存字段操作记录失败: %v\n", err)
	}

	c.JSON(http.StatusOK, result)
}

// 添加字段
// AddField 添加字段
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
	// 获取字段类型信息
	typeInfo, typeExists := models.SupportedFieldTypes[strings.ToLower(req.FieldType)]
	if !typeExists {
		// 返回支持的类型列表
		supportedTypes := make([]string, 0, len(models.SupportedFieldTypes))
		for typeName := range models.SupportedFieldTypes {
			supportedTypes = append(supportedTypes, typeName)
		}
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: fmt.Sprintf("不支持的字段类型: %s", req.FieldType),
			Data: map[string]interface{}{
				"supported_types": supportedTypes,
			},
		})
		return
	}
	// 验证需要长度的类型
	if typeInfo.NeedLength && req.Length <= 0 {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: fmt.Sprintf("%s 类型必须指定长度参数 (length)", req.FieldType),
		})
		return
	}
	// 验证需要精度的类型
	if typeInfo.NeedScale && req.Precision <= 0 {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: fmt.Sprintf("%s 类型必须指定精度参数 (precision)", req.FieldType),
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
			Message: "表中已有数据,添加非空字段时必须提供默认值",
		})
		return
	}
	// 添加字段
	err := uc.fieldService.AddField(
		req.TableName,
		req.FieldName,
		req.FieldType,
		req.Length,
		req.Precision,
		req.Scale,
		req.DefaultValue,
		req.Comment,
		req.IsNullable,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Code:    500,
			Message: "添加字段失败: " + err.Error(),
		})
		return
	}
	// 构建字段类型字符串用于记录
	fieldTypeStr := uc.fieldService.BuildFieldTypeString(req.FieldType, req.Length, req.Precision, req.Scale)
	// 保存字段操作记录
	record := &models.FieldRecord{
		TableName:    req.TableName,
		Type:         "add",
		BZ:           req.Comment,
		OldFieldName: "",
		OldFieldType: "",
		NewFieldName: req.FieldName,
		NewFieldType: fieldTypeStr,
	}
	if err := uc.fieldService.SaveFieldRecord(record); err != nil {
		fmt.Printf("保存字段操作记录失败: %v\n", err)
	}
	c.JSON(http.StatusOK, models.Response{
		Code:    200,
		Message: "字段添加成功",
		Data: map[string]interface{}{
			"table_name": req.TableName,
			"field_name": req.FieldName,
			"field_type": req.FieldType,
			"length":     req.Length,
			"precision":  req.Precision,
			"scale":      req.Scale,
			"category":   typeInfo.Category,
		},
	})
}

// GetSupportedFieldTypes 获取支持的字段类型列表
func (uc *UserController) GetSupportedFieldTypes(c *gin.Context) {
	// 按分类组织类型
	categorizedTypes := make(map[string][]map[string]interface{})
	for typeName, typeInfo := range models.SupportedFieldTypes {
		typeData := map[string]interface{}{
			"name":        typeName,
			"need_length": typeInfo.NeedLength,
			"need_scale":  typeInfo.NeedScale,
			"description": typeInfo.Description,
		}
		if categorizedTypes[typeInfo.Category] == nil {
			categorizedTypes[typeInfo.Category] = make([]map[string]interface{}, 0)
		}
		categorizedTypes[typeInfo.Category] = append(categorizedTypes[typeInfo.Category], typeData)
	}
	c.JSON(http.StatusOK, models.Response{
		Code:    200,
		Message: "获取成功",
		Data: map[string]interface{}{
			"types":      models.SupportedFieldTypes,
			"categories": categorizedTypes,
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

	// 获取删除前的字段信息
	oldFieldInfo, err := uc.fieldService.GetSingleFieldInfo(req.TableName, req.FieldName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Code:    500,
			Message: "获取字段信息失败: " + err.Error(),
		})
		return
	}

	// 构建字段类型字符串
	oldFieldTypeStr := oldFieldInfo.FieldType
	if oldFieldInfo.Length > 0 {
		oldFieldTypeStr = fmt.Sprintf("%s(%d)", oldFieldInfo.FieldType, oldFieldInfo.Length)
	}

	// 删除字段
	err = uc.fieldService.DeleteField(req.TableName, req.FieldName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Code:    500,
			Message: "删除字段失败: " + err.Error(),
		})
		return
	}

	// 保存字段操作记录
	record := &models.FieldRecord{
		TableName:    req.TableName,
		Type:         "delete", // 操作类型：删除
		BZ:           oldFieldInfo.Comment,
		OldFieldName: req.FieldName,
		OldFieldType: oldFieldTypeStr,
		NewFieldName: "",
		NewFieldType: "",
	}

	if err := uc.fieldService.SaveFieldRecord(record); err != nil {
		// 记录保存失败，只记录日志，不影响主流程
		fmt.Printf("保存字段操作记录失败: %v\n", err)
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
