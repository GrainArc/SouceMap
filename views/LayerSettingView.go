package views

import (
	"fmt"
	"github.com/fmecool/SouceMap/models"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"strings"
)

// 颜色配置
type CMap struct {
	Property string
	Color    string
}
type ColorData struct {
	LayerName string
	AttName   string
	ColorMap  []CMap
}

func (uc *UserController) AddUpdateColorSet(c *gin.Context) {
	var jsonData ColorData
	c.BindJSON(&jsonData)
	DB := models.DB
	//提前检查有没有相同的字段配置,有则删除配置
	var searchData []models.AttColor
	DB.Where("layer_name = ? ", jsonData.LayerName).Find(&searchData)
	if len(searchData) > 0 {
		DB.Delete(&searchData)
	}
	// 预分配切片容量，避免动态扩展时的内存重分配
	data := make([]models.AttColor, 0, len(jsonData.ColorMap))

	// 遍历颜色映射数据，构建数据库记录
	for _, colorItem := range jsonData.ColorMap {
		// 创建新的属性颜色记录
		attColor := models.AttColor{
			Color:     colorItem.Color,    // 设置颜色值
			Property:  colorItem.Property, // 设置属性值
			LayerName: jsonData.LayerName, // 设置图层名称
			AttName:   jsonData.AttName,   // 设置属性名称
		}
		// 将记录添加到数据切片中
		data = append(data, attColor)
	}

	// 批量插入数据到数据库，并处理可能的错误
	if err := DB.Create(&data).Error; err != nil {
		// 记录错误日志或返回错误给调用者
		log.Printf("Failed to create AttColor records: %v", err)

	}

	c.JSON(http.StatusOK, jsonData)
}

// 获取配置表
func GetColor(LayerName string) []ColorData {
	DB := models.DB

	colorDataMap := make(map[string]map[string][]CMap)
	var searchData []models.AttColor
	DB.Where("layer_name = ? ", LayerName).Find(&searchData)
	// 按 LayerName 和 AttName 分组
	for _, item := range searchData {
		if colorDataMap[item.LayerName] == nil {
			colorDataMap[item.LayerName] = make(map[string][]CMap)
		}

		colorDataMap[item.LayerName][item.AttName] = append(
			colorDataMap[item.LayerName][item.AttName],
			CMap{
				Property: item.Property,
				Color:    item.Color,
			},
		)
	}

	// 转换为 ColorData 切片
	var result []ColorData
	for layerName, attMap := range colorDataMap {
		for attName, colorMaps := range attMap {
			result = append(result, ColorData{
				LayerName: layerName,
				AttName:   attName,
				ColorMap:  colorMaps,
			})
		}
	}
	return result
}

func (uc *UserController) GetColorSet(c *gin.Context) {
	LayerName := c.Query("LayerName")
	result := GetColor(LayerName)
	c.JSON(http.StatusOK, result)
}

// 中文字段映射配置
type CEMap struct {
	CName string
	EName string
}
type CEData struct {
	LayerName string
	CEMap     []CEMap
}

func (uc *UserController) AddUpdateCESet(c *gin.Context) {
	var jsonData CEData
	c.BindJSON(&jsonData)
	DB := models.DB
	//提前检查有没有相同的字段配置,有则删除配置
	var searchData []models.ChineseProperty
	DB.Where("layer_name = ? ", jsonData.LayerName).Find(&searchData)
	if len(searchData) > 0 {
		DB.Delete(&searchData)
	}
	// 预分配切片容量，避免动态扩展时的内存重分配
	data := make([]models.ChineseProperty, 0, len(jsonData.CEMap))

	// 遍历颜色映射数据，构建数据库记录
	for _, colorItem := range jsonData.CEMap {
		// 创建新的属性颜色记录
		attColor := models.ChineseProperty{
			CName:     colorItem.CName,    // 设置颜色值
			EName:     colorItem.EName,    // 设置属性值
			LayerName: jsonData.LayerName, // 设置图层名称

		}
		// 将记录添加到数据切片中
		data = append(data, attColor)
	}

	// 批量插入数据到数据库，并处理可能的错误
	if err := DB.Create(&data).Error; err != nil {
		// 记录错误日志或返回错误给调用者
		log.Printf("Failed to create AttColor records: %v", err)

	}

	c.JSON(http.StatusOK, jsonData)
}

func GetCEMap(LayerName string) map[string]string {
	DB := models.DB

	// Fetch Chinese property mappings first
	var searchData []models.ChineseProperty
	if err := DB.Where("layer_name = ?", LayerName).Find(&searchData).Error; err != nil {
		fmt.Println(err)
		return nil
	}

	// Create a lookup map for faster searching
	enameToCname := make(map[string]string)
	for _, item := range searchData {
		enameToCname[item.EName] = item.CName
	}

	// Get column names from information_schema
	TableName := strings.ToLower(LayerName)
	var resultx []Res
	sql := `SELECT column_name FROM information_schema.columns WHERE table_name = ?`
	if err := DB.Raw(sql, TableName).Scan(&resultx).Error; err != nil {
		fmt.Println(err)
		return nil
	}

	// Process columns and build result map
	result := make(map[string]string)
	for _, item := range resultx {
		columnName := item.ColumnName
		if columnName == "id" || columnName == "geom" {
			continue
		}

		// Check if we have a Chinese name mapping
		if cname, exists := enameToCname[columnName]; exists {
			result[columnName] = cname
		} else {
			result[columnName] = columnName
		}
	}

	return result
}

type ChineseProperty struct {
	ID        int64  `gorm:"primary_key"`
	LayerName string `gorm:"type:varchar(255)"`
	CName     string `gorm:"type:varchar(255)"`
	EName     string `gorm:"type:varchar(255)"`
}

func (uc *UserController) GetCESet(c *gin.Context) {
	LayerName := c.Query("LayerName")
	result := GetCEMap(LayerName)
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

	// 添加字段
	err := uc.fieldService.AddField(req.TableName, req.FieldName, req.FieldType,
		req.DefaultValue, req.Comment, req.IsNullable)
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
		req.FieldType, req.DefaultValue, req.Comment, req.IsNullable)
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

// GetTableStructure 获取表结构接口
func (fc *UserController) GetTableStructure(c *gin.Context) {
	tableName := c.Param("table_name")
	if tableName == "" {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: "表名不能为空",
		})
		return
	}

	structure, err := fc.fieldService.GetTableStructure(tableName)
	if err != nil {
		c.JSON(http.StatusBadRequest, models.Response{
			Code:    400,
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, models.Response{
		Code:    200,
		Message: "获取表结构成功",
		Data:    structure,
	})
}

// GetFieldInfo 获取单个字段信息接口
func (fc *UserController) GetFieldInfo(c *gin.Context) {
	tableName := c.Param("table_name")
	fieldName := c.Param("field_name")

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

// GetTableList 获取所有表列表接口
func (fc *UserController) GetTableList(c *gin.Context) {
	tables, err := fc.fieldService.GetTableList()
	if err != nil {
		c.JSON(http.StatusInternalServerError, models.Response{
			Code:    500,
			Message: "获取表列表失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, models.Response{
		Code:    200,
		Message: "获取表列表成功",
		Data: map[string]interface{}{
			"tables": tables,
			"count":  len(tables),
		},
	})
}
