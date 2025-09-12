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
	DB := *models.DB

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
