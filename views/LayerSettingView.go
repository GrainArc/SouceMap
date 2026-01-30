package views

import (
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"github.com/GrainArc/SouceMap/response"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"log"
	"math"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
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

func isValidColor(color string) bool {
	// 移除所有空格
	color = strings.ReplaceAll(color, " ", "")

	// 尝试匹配 RGB 格式
	if isValidRGB(color) {
		return true
	}

	// 尝试匹配 RGBA 格式
	if isValidRGBA(color) {
		return true
	}

	// 尝试匹配 HEX 格式
	if isValidHex(color) {
		return true
	}

	return false
}

// isValidRGB 验证 RGB 格式
func isValidRGB(color string) bool {
	// 正则匹配 RGB(r,g,b) 格式（不区分大小写）
	rgbPattern := `^(?i)RGB\((\d{1,3}),(\d{1,3}),(\d{1,3})\)$`
	re := regexp.MustCompile(rgbPattern)

	matches := re.FindStringSubmatch(color)
	if matches == nil {
		return false
	}

	// 验证每个颜色值是否在 0-255 范围内
	for i := 1; i <= 3; i++ {
		val, err := strconv.Atoi(matches[i])
		if err != nil || val < 0 || val > 255 {
			return false
		}
	}

	return true
}

// isValidRGBA 验证 RGBA 格式
func isValidRGBA(color string) bool {
	// 正则匹配 RGBA(r,g,b,a) 格式（不区分大小写）
	// alpha 值可以是 0-1 的小数或 0-255 的整数
	rgbaPattern := `^(?i)RGBA\((\d{1,3}),(\d{1,3}),(\d{1,3}),([0-9]*\.?[0-9]+)\)$`
	re := regexp.MustCompile(rgbaPattern)

	matches := re.FindStringSubmatch(color)
	if matches == nil {
		return false
	}

	// 验证 RGB 值是否在 0-255 范围内
	for i := 1; i <= 3; i++ {
		val, err := strconv.Atoi(matches[i])
		if err != nil || val < 0 || val > 255 {
			return false
		}
	}

	// 验证 Alpha 值
	alpha, err := strconv.ParseFloat(matches[4], 64)
	if err != nil {
		return false
	}

	// Alpha 值可以是 0-1 或 0-255
	if (alpha >= 0 && alpha <= 1) || (alpha >= 0 && alpha <= 255) {
		return true
	}

	return false
}

// isValidHex 验证 HEX 格式
func isValidHex(color string) bool {
	// 移除可能的 # 前缀
	color = strings.TrimPrefix(color, "#")

	// HEX 格式可以是 3 位或 6 位或 8 位（包含 alpha）
	hexPattern := `^(?i)[0-9A-F]{3}$|^(?i)[0-9A-F]{6}$|^(?i)[0-9A-F]{8}$`
	re := regexp.MustCompile(hexPattern)

	return re.MatchString(color)
}

// normalizeColor 将颜色格式标准化为统一格式（可选功能）
// 将所有颜色转换为 RGB(r,g,b) 或 RGBA(r,g,b,a) 格式
func normalizeColor(color string) (string, error) {
	// 移除所有空格
	color = strings.ReplaceAll(color, " ", "")

	// 如果已经是 RGB 或 RGBA 格式，直接返回
	if isValidRGB(color) || isValidRGBA(color) {
		return strings.ToUpper(color[:4]) + color[4:], nil
	}

	// 如果是 HEX 格式，转换为 RGB
	if isValidHex(color) {
		return hexToRGB(color)
	}

	return "", fmt.Errorf("无效的颜色格式: %s", color)
}

// hexToRGB 将 HEX 格式转换为 RGB 格式

func hexToRGB(hex string) (string, error) {
	// 移除 # 前缀
	hex = strings.TrimPrefix(hex, "#")
	hex = strings.ToUpper(hex)

	var r, g, b int
	var a float64 = 1.0

	switch len(hex) {
	case 3:
		// 3位HEX，如 F00 -> FF0000
		rVal, _ := strconv.ParseInt(string(hex[0])+string(hex[0]), 16, 64)
		gVal, _ := strconv.ParseInt(string(hex[1])+string(hex[1]), 16, 64)
		bVal, _ := strconv.ParseInt(string(hex[2])+string(hex[2]), 16, 64)
		r = int(rVal)
		g = int(gVal)
		b = int(bVal)
	case 6:
		// 6位HEX，如 FF0000
		rVal, _ := strconv.ParseInt(hex[0:2], 16, 64)
		gVal, _ := strconv.ParseInt(hex[2:4], 16, 64)
		bVal, _ := strconv.ParseInt(hex[4:6], 16, 64)
		r = int(rVal)
		g = int(gVal)
		b = int(bVal)
	case 8:
		// 8位HEX（包含alpha），如 FF0000FF
		rVal, _ := strconv.ParseInt(hex[0:2], 16, 64)
		gVal, _ := strconv.ParseInt(hex[2:4], 16, 64)
		bVal, _ := strconv.ParseInt(hex[4:6], 16, 64)
		alphaInt, _ := strconv.ParseInt(hex[6:8], 16, 64)
		r = int(rVal)
		g = int(gVal)
		b = int(bVal)
		a = float64(alphaInt) / 255.0
		return fmt.Sprintf("RGBA(%d,%d,%d,%.2f)", r, g, b, a), nil
	default:
		return "", fmt.Errorf("无效的HEX长度: %d", len(hex))
	}

	return fmt.Sprintf("RGB(%d,%d,%d)", r, g, b), nil
}

// rgbToHex 将 RGB/RGBA 格式转换为 HEX 格式（可选功能）
func rgbToHex(color string) (string, error) {
	color = strings.ReplaceAll(color, " ", "")

	// 匹配 RGB
	rgbPattern := `^(?i)RGB\((\d{1,3}),(\d{1,3}),(\d{1,3})\)$`
	re := regexp.MustCompile(rgbPattern)
	matches := re.FindStringSubmatch(color)

	if matches != nil {
		r, _ := strconv.Atoi(matches[1])
		g, _ := strconv.Atoi(matches[2])
		b, _ := strconv.Atoi(matches[3])
		return fmt.Sprintf("#%02X%02X%02X", r, g, b), nil
	}

	// 匹配 RGBA
	rgbaPattern := `^(?i)RGBA\((\d{1,3}),(\d{1,3}),(\d{1,3}),([0-9]*\.?[0-9]+)\)$`
	re = regexp.MustCompile(rgbaPattern)
	matches = re.FindStringSubmatch(color)

	if matches != nil {
		r, _ := strconv.Atoi(matches[1])
		g, _ := strconv.Atoi(matches[2])
		b, _ := strconv.Atoi(matches[3])
		alpha, _ := strconv.ParseFloat(matches[4], 64)

		// 如果 alpha 在 0-1 之间，转换为 0-255
		if alpha <= 1 {
			alpha = alpha * 255
		}

		return fmt.Sprintf("#%02X%02X%02X%02X", r, g, b, int(alpha)), nil
	}

	return "", fmt.Errorf("无效的RGB/RGBA格式: %s", color)
}

// filterValidColors 过滤并返回颜色格式合法的 CMap
// filterValidColors 过滤并返回颜色格式合法的 CMap
func filterValidColors(colorMaps []CMap) ([]CMap, []string) {
	validMaps := make([]CMap, 0, len(colorMaps))
	invalidColors := make([]string, 0)

	for _, cm := range colorMaps {
		if isValidColor(cm.Color) { // 使用新的验证函数
			validMaps = append(validMaps, cm)
		} else {
			invalidColors = append(invalidColors,
				fmt.Sprintf("Property: %s, Color: %s", cm.Property, cm.Color))
		}
	}

	return validMaps, invalidColors
}

// generateRandomRGBColor 生成随机 RGB 颜色
func generateRandomRGBColor() string {
	r := rand.Intn(256)
	g := rand.Intn(256)
	b := rand.Intn(256)
	return fmt.Sprintf("RGB(%d,%d,%d)", r, g, b)
}

// generateDistinctColors 生成视觉上区分度较高的颜色
func generateDistinctColors(count int) []string {
	colors := make([]string, count)

	// 使用黄金角度分割来生成色相值，确保颜色分布均匀
	goldenRatioConjugate := 0.618033988749895
	hue := rand.Float64()

	for i := 0; i < count; i++ {
		hue += goldenRatioConjugate
		hue = math.Mod(hue, 1.0)

		// 转换 HSV 到 RGB (饱和度和明度固定以保证鲜艳度)
		r, g, b := hsvToRGB(hue, 0.7, 0.9)
		colors[i] = fmt.Sprintf("RGB(%d,%d,%d)", r, g, b)
	}

	return colors
}

// hsvToRGB 将 HSV 颜色转换为 RGB
func hsvToRGB(h, s, v float64) (int, int, int) {
	c := v * s
	x := c * (1 - math.Abs(math.Mod(h*6, 2)-1))
	m := v - c

	var r, g, b float64

	switch {
	case h < 1.0/6.0:
		r, g, b = c, x, 0
	case h < 2.0/6.0:
		r, g, b = x, c, 0
	case h < 3.0/6.0:
		r, g, b = 0, c, x
	case h < 4.0/6.0:
		r, g, b = 0, x, c
	case h < 5.0/6.0:
		r, g, b = x, 0, c
	default:
		r, g, b = c, 0, x
	}

	return int((r + m) * 255), int((g + m) * 255), int((b + m) * 255)
}

func (uc *UserController) AddUpdateColorSet(c *gin.Context) {
	var jsonData ColorData
	// 绑定并验证 JSON 数据
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "请求参数格式错误",
			"details": err.Error(),
		})
		return
	}

	DB := models.DB

	// 验证颜色格式，过滤出合法的颜色配置
	validColorMaps, invalidColors := filterValidColors(jsonData.ColorMap)

	// 如果存在不合法的颜色格式，记录日志
	if len(invalidColors) > 0 {
		log.Printf("Invalid color formats detected: %v", invalidColors)
	}
	if err := fixAttColorSequence(); err != nil {
		log.Printf("警告：修复序列失败: %v", err)
		// 不直接返回，继续执行（使用手动分配ID的方案）
	}
	// 使用事务处理数据库操作
	tx := DB.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	// 删除已存在的配置
	if err := tx.Where("layer_name = ?", jsonData.LayerName).Delete(&models.AttColor{}).Error; err != nil {
		tx.Rollback()
		log.Printf("Failed to delete existing AttColor records: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "删除旧配置失败",
		})
		return
	}

	// 判断是否为默认配置

	isDefaultConfig := len(validColorMaps) == 1 && validColorMaps[0].Property == "默认"

	var data []models.AttColor
	var autoAssignedCount int

	if isDefaultConfig {
		// 默认配置：直接添加
		data = buildAttColorRecords(validColorMaps, jsonData.LayerName, jsonData.AttName)
	} else {
		// 非默认配置：验证属性值是否存在
		existingProperties, err := getTablePropertyValues(DB, jsonData.LayerName, jsonData.AttName)
		if err != nil {
			tx.Rollback()
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("查询表属性值失败: %v", err),
			})
			return
		}

		if len(existingProperties) == 0 {
			tx.Rollback()
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "表中没有可用的属性值",
			})
			return
		}

		// 创建属性值到颜色的映射
		propertyColorMap := make(map[string]string, len(validColorMaps))
		for _, cm := range validColorMaps {
			propertyColorMap[cm.Property] = cm.Color
		}

		// 找出未匹配的属性值
		unmatchedProperties := make([]string, 0)
		matchedData := make([]CMap, 0, len(existingProperties))

		for _, prop := range existingProperties {
			if color, exists := propertyColorMap[prop]; exists {
				// 已有颜色配置的属性值
				matchedData = append(matchedData, CMap{
					Property: prop,
					Color:    color,
				})
			} else {
				// 未匹配的属性值
				unmatchedProperties = append(unmatchedProperties, prop)
			}
		}

		// 为未匹配的属性值生成颜色
		if len(unmatchedProperties) > 0 {
			// 生成区分度高的颜色
			generatedColors := generateDistinctColors(len(unmatchedProperties))

			for i, prop := range unmatchedProperties {
				matchedData = append(matchedData, CMap{
					Property: prop,
					Color:    generatedColors[i],
				})
			}

			autoAssignedCount = len(unmatchedProperties)
			log.Printf("自动为 %d 个未匹配属性值分配了颜色", autoAssignedCount)
		}

		data = buildAttColorRecords(matchedData, jsonData.LayerName, jsonData.AttName)
	}

	// 批量插入数据
	if err := tx.Create(&data).Error; err != nil {
		tx.Rollback()
		log.Printf("Failed to create AttColor records: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("保存颜色配置失败:%s", err.Error()),
		})
		return
	}

	// 提交事务
	if err := tx.Commit().Error; err != nil {
		log.Printf("Failed to commit transaction: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("提交事务失败:%s", err.Error()),
		})
		return
	}

	// 返回成功响应，包含实际保存的数据统计
	response := gin.H{
		"message":     "颜色配置保存成功",
		"saved_count": len(data),
	}

	if autoAssignedCount > 0 {
		response["auto_assigned_count"] = autoAssignedCount
		response["info"] = fmt.Sprintf("自动为 %d 个未匹配的属性值分配了颜色", autoAssignedCount)
	}

	if len(invalidColors) > 0 {
		response["warning"] = "部分颜色格式不合法已被忽略"
		response["invalid_colors"] = invalidColors
	}

	c.JSON(http.StatusOK, response)
}

// buildAttColorRecords 构建 AttColor 记录切片
func buildAttColorRecords(colorMaps []CMap, layerName, attName string) []models.AttColor {
	data := make([]models.AttColor, 0, len(colorMaps))
	for _, cm := range colorMaps {
		attColor := models.AttColor{
			Color:     cm.Color,
			Property:  cm.Property,
			LayerName: layerName,
			AttName:   attName,
		}
		data = append(data, attColor)
	}
	return data
}

// fixAttColorSequence 修复att_color表的自增序列
func fixAttColorSequence() error {
	DB := models.DB

	// 获取当前最大ID
	var maxID int64
	err := DB.Model(&models.AttColor{}).Select("COALESCE(MAX(id), 0)").Scan(&maxID).Error
	if err != nil {
		return fmt.Errorf("查询最大ID失败: %v", err)
	}

	// 更新序列的下一个值（maxID + 1）
	// 假设序列名称为 'att_color_id_seq'（PostgreSQL默认命名）
	query := fmt.Sprintf("SELECT setval('att_color_id_seq', %d, true)", maxID+1)

	err = DB.Exec(query).Error
	if err != nil {
		// 如果默认序列名不对，尝试其他可能的序列名
		query = fmt.Sprintf("SELECT setval('att_color_id_seq', %d, true)", maxID+1)
		err = DB.Exec(query).Error
		if err != nil {
			return fmt.Errorf("修复序列失败: %v", err)
		}
	}

	log.Printf("已修复att_color序列，下一个ID将从%d开始", maxID+1)
	return nil
}

// 检查表中是否存在指定的列
func checkTableColumnExists(db *gorm.DB, tableName, columnName string) bool {
	var count int64

	// 查询PostgreSQL的information_schema来检查列是否存在
	query := `
		SELECT COUNT(*) 
		FROM information_schema.columns 
		WHERE table_schema = 'public'
		AND table_name = $1
		AND column_name = $2
	`

	err := db.Raw(query, tableName, columnName).Count(&count).Error
	if err != nil {
		log.Printf("检查表列存在性失败: %v", err)
		return false
	}
	return count > 0
}

// 获取表中指定属性字段的所有唯一值
func getTablePropertyValues(db *gorm.DB, tableName, columnName string) ([]string, error) {
	var properties []string

	// 构建查询语句，获取指定列的所有唯一值
	// PostgreSQL使用双引号来引用标识符

	query := fmt.Sprintf(`SELECT DISTINCT "%s" FROM "%s" WHERE "%s" IS NOT NULL AND "%s" != ''`,
		columnName, tableName, columnName, columnName)

	rows, err := db.Raw(query).Rows()
	if err != nil {
		return nil, fmt.Errorf("查询表属性值失败: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var property string
		if err := rows.Scan(&property); err != nil {
			log.Printf("扫描属性值失败: %v", err)
			continue
		}
		properties = append(properties, property)
	}

	return properties, nil
}

type TPData struct {
	TableName  string `json:"table_name"`
	ColumnName string `json:"column_name"`
}

func (uc *UserController) GetTablePropertyValues(c *gin.Context) {
	DB := models.DB
	var tpData TPData
	if err := c.BindJSON(&tpData); err != nil {
		response.Error(c, 200, "参数错误")
		return
	}
	values, err := getTablePropertyValues(DB, tpData.TableName, tpData.ColumnName)
	if err != nil {
		response.Error(c, 200, "字段查询为空")
		return
	}
	response.Success(c, values)
}

// 获取配置表
func GetColor(LayerName string) []ColorData {
	DB := models.DB

	var searchData []models.AttColor
	DB.Where("layer_name = ?", LayerName).Find(&searchData)

	if len(searchData) == 0 {
		return []ColorData{}
	}

	// 使用 map 按 AttName 分组，同时对 Property 去重
	attColorMap := make(map[string]map[string]CMap) // attName -> property -> CMap

	for _, item := range searchData {
		if attColorMap[item.AttName] == nil {
			attColorMap[item.AttName] = make(map[string]CMap)
		}

		// 使用 Property 作为 key 实现去重
		// 如果相同 Property 已存在，会被覆盖（保留最后一个）
		attColorMap[item.AttName][item.Property] = CMap{
			Property: item.Property,
			Color:    item.Color,
		}
	}

	// 转换为 ColorData 切片
	result := make([]ColorData, 0, len(attColorMap))
	for attName, propertyMap := range attColorMap {
		colorMaps := make([]CMap, 0, len(propertyMap))
		for _, cmap := range propertyMap {
			colorMaps = append(colorMaps, cmap)
		}

		result = append(result, ColorData{
			LayerName: LayerName, // 直接使用参数，因为查询条件已经限定了 LayerName
			AttName:   attName,
			ColorMap:  colorMaps,
		})
	}

	return result
}

func validateAndCleanColorData(db *gorm.DB, layerName string, searchData []models.AttColor) []models.AttColor {
	// 按 AttName 分组
	attGroupMap := make(map[string][]models.AttColor)
	for _, item := range searchData {
		attGroupMap[item.AttName] = append(attGroupMap[item.AttName], item)
	}

	var validData []models.AttColor
	var invalidIDs []int64

	for attName, items := range attGroupMap {
		// 检查表中是否存在该属性字段
		if !checkTableColumnExists(db, layerName, attName) {
			log.Printf("属性字段 '%s' 在表 '%s' 中不存在，将删除相关配置", attName, layerName)
			// 收集无效记录的ID用于删除（但排除"默认"配置）
			for _, item := range items {
				if item.Property != "默认" {
					invalidIDs = append(invalidIDs, item.ID)
				} else {
					// 保留"默认"配置
					validData = append(validData, item)
					log.Printf("保留默认配置: AttName='%s', Property='默认'", attName)
				}
			}
			continue
		}

		// 获取表中该属性字段的所有唯一值
		existingProperties, err := getTablePropertyValues(db, layerName, attName)
		if err != nil {
			log.Printf("查询表 '%s' 属性字段 '%s' 的值失败: %v", layerName, attName, err)
			continue
		}

		// 创建属性值映射，用于快速查找
		propertyMap := make(map[string]bool)
		for _, prop := range existingProperties {
			propertyMap[prop] = true
		}

		// 验证每个属性值是否存在
		for _, item := range items {
			// 如果是"默认"配置，直接保留
			if item.Property == "默认" {
				validData = append(validData, item)
				log.Printf("保留默认配置: AttName='%s', Property='默认'", attName)
				continue
			}

			// 非"默认"配置需要验证属性值是否存在
			if propertyMap[item.Property] {
				validData = append(validData, item)
			} else {
				log.Printf("属性值 '%s' 在表 '%s' 的字段 '%s' 中不存在，将删除该配置",
					item.Property, layerName, attName)
				invalidIDs = append(invalidIDs, item.ID)
			}
		}
	}

	// 批量删除无效数据
	if len(invalidIDs) > 0 {
		if err := db.Where("id IN ?", invalidIDs).Delete(&models.AttColor{}).Error; err != nil {
			log.Printf("删除无效颜色配置失败: %v", err)
		} else {
			log.Printf("成功删除 %d 条无效颜色配置", len(invalidIDs))
		}
	}

	return validData
}

func CleanColorMapForTable(db *gorm.DB, layerName string) (*CleanResult, error) {
	// 查询该表的所有颜色配置
	var searchData []models.AttColor
	if err := db.Where("layer_name = ?", layerName).Find(&searchData).Error; err != nil {
		return nil, fmt.Errorf("查询颜色配置失败: %v", err)
	}

	// 如果没有配置数据，直接返回
	if len(searchData) == 0 {
		return &CleanResult{
			LayerName:    layerName,
			TotalCount:   0,
			ValidCount:   0,
			DeletedCount: 0,
			DefaultCount: 0,
			Message:      "该表没有颜色配置数据",
		}, nil
	}

	totalCount := len(searchData)

	// 统计"默认"配置数量
	defaultCount := 0
	for _, item := range searchData {
		if item.Property == "默认" {
			defaultCount++
		}
	}

	// 使用 validateAndCleanColorData 进行验证和清理
	validData := validateAndCleanColorData(db, layerName, searchData)

	validCount := len(validData)
	deletedCount := totalCount - validCount

	result := &CleanResult{
		LayerName:    layerName,
		TotalCount:   totalCount,
		ValidCount:   validCount,
		DeletedCount: deletedCount,
		DefaultCount: defaultCount,
	}

	if deletedCount > 0 {
		result.Message = fmt.Sprintf("成功清理 %d 条无效配置，保留 %d 条有效配置（包含 %d 条默认配置）",
			deletedCount, validCount, defaultCount)
	} else {
		result.Message = fmt.Sprintf("所有配置都是有效的，无需清理（包含 %d 条默认配置）", defaultCount)
	}

	return result, nil
}

// CleanResult 清理结果结构
type CleanResult struct {
	LayerName    string `json:"layer_name"`    // 图层/表名
	TotalCount   int    `json:"total_count"`   // 清理前总配置数
	ValidCount   int    `json:"valid_count"`   // 有效配置数
	DeletedCount int    `json:"deleted_count"` // 删除的配置数
	DefaultCount int    `json:"default_count"` // 默认配置数（被保护）
	Message      string `json:"message"`       // 结果消息
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
	err := fixChinesePropertySequence()
	if err != nil {
		fmt.Println(err)
	}
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
func fixChinesePropertySequence() error {
	DB := models.DB

	// 获取当前最大ID
	var maxID int64
	err := DB.Model(&models.ChineseProperty{}).Select("COALESCE(MAX(id), 0)").Scan(&maxID).Error
	if err != nil {
		return fmt.Errorf("查询最大ID失败: %v", err)
	}

	// 尝试修复序列
	// PostgreSQL序列通常命名为: 表名_id_seq
	sequenceName := "chinese_property_id_seq"

	// 检查序列是否存在
	var exists bool
	checkQuery := `
        SELECT EXISTS (
            SELECT 1 
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'S' 
            AND c.relname = $1
            AND n.nspname = 'public'
        )
    `

	err = DB.Raw(checkQuery, sequenceName).Scan(&exists).Error
	if err != nil {
		return fmt.Errorf("检查序列存在性失败: %v", err)
	}

	if exists {
		// 序列存在，修复它
		fixQuery := fmt.Sprintf("SELECT setval('%s', %d, true)", sequenceName, maxID+1)
		err = DB.Exec(fixQuery).Error
		if err != nil {
			return fmt.Errorf("修复序列失败: %v", err)
		}
		log.Printf("已修复%s序列，下一个ID将从%d开始", sequenceName, maxID+1)
	} else {
		// 序列不存在，记录警告
		log.Printf("警告：序列%s不存在，可能是表结构问题", sequenceName)
	}

	return nil
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
