package views

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"github.com/GrainArc/SouceMap/pgmvt"
	"github.com/GrainArc/SouceMap/response"
	"github.com/gin-gonic/gin"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"net/http"
	"strconv"
	"strings"
)

// PublishWMTS 发布 WMTS 服务
func (uc *UserController) PublishWMTS(c *gin.Context) {
	// 获取参数
	layerName := strings.ToLower(c.Query("layername"))
	opacityStr := c.Query("opacity")

	if layerName == "" {
		response.Error(c, 500, "layername参数不能为空")
		return
	}

	// 解析透明度，默认为 1.0
	opacity := 1.0
	if opacityStr != "" {
		var err error
		opacity, err = strconv.ParseFloat(opacityStr, 64)
		if err != nil || opacity < 0 || opacity > 1 {
			response.Error(c, 500, "opacity参数格式错误，应为0-1之间的数值")
			return
		}
	}

	DB := models.DB

	// 1. 查询 MySchema 获取 TileSize
	var mySchema models.MySchema
	result := DB.Where("en = ?", layerName).First(&mySchema)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			response.Error(c, 500, "未找到对应的图层")
		} else {
			response.Error(c, 500, "数据库查询失败")
		}
		return
	}

	tileSize := mySchema.TileSize
	if tileSize == 0 {
		tileSize = 256 // 默认值
	}

	// 2. 获取颜色配置
	colorData := GetColor(layerName)
	colorConfigJSON, err := json.Marshal(colorData)
	if err != nil {
		response.Error(c, 500, "颜色配置序列化失败")
		return
	}

	// 3. 创建缓存表
	cacheTableName := layerName + "_wmts"
	if err := createWMTSCacheTable(DB, cacheTableName); err != nil {
		response.Error(c, 500, fmt.Sprintf("创建缓存表失败: %v", err))
		return
	}

	// 4. 保存或更新 WmtsSchema
	var wmtsSchema models.WmtsSchema
	result = DB.Where("layer_name = ?", layerName).First(&wmtsSchema)

	if result.Error != nil && !errors.Is(result.Error, gorm.ErrRecordNotFound) {
		response.Error(c, 500, "数据库查询失败")
		return
	}

	// 更新或创建记录
	wmtsSchema.LayerName = layerName
	wmtsSchema.Opacity = opacity
	wmtsSchema.TileSize = tileSize
	wmtsSchema.ColorConfig = datatypes.JSON(colorConfigJSON)

	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		// 创建新记录
		if err := DB.Create(&wmtsSchema).Error; err != nil {
			response.Error(c, 500, "创建WMTS配置失败")
			return
		}
	} else {
		// 更新现有记录 - 清空缓存
		if err := DB.Save(&wmtsSchema).Error; err != nil {
			response.Error(c, 500, "更新WMTS配置失败")
			return
		}
		// 清空缓存表
		if err := clearWMTSCache(DB, cacheTableName); err != nil {
			response.Error(c, 500, fmt.Sprintf("清空缓存失败: %v", err))
			return
		}
	}

	// 5. 返回服务接口
	serviceURL := fmt.Sprintf("/wmts/%s/{z}/{x}/{y}.png", layerName)
	response.SuccessWithMessage(c, "WMTS服务发布成功", gin.H{
		"service_url": serviceURL,
		"layer_name":  layerName,
		"tile_size":   tileSize,
		"opacity":     opacity,
		"cache_table": cacheTableName,
	})
}

// createWMTSCacheTable 创建WMTS缓存表
func createWMTSCacheTable(db *gorm.DB, tableName string) error {
	// 检查表是否已存在
	if db.Migrator().HasTable(tableName) {
		return nil
	}

	// 创建表结构
	sql := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS "%s" (
			id BIGSERIAL PRIMARY KEY,
			x BIGINT NOT NULL,
			y BIGINT NOT NULL,
			z BIGINT NOT NULL,
			byte BYTEA,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		-- 创建联合索引
		CREATE INDEX IF NOT EXISTS idx_%s_xyz ON "%s" (z, x, y);
		
		-- 创建唯一约束，防止重复缓存
		CREATE UNIQUE INDEX IF NOT EXISTS idx_%s_unique ON "%s" (z, x, y);
	`, tableName, tableName, tableName, tableName, tableName)

	return db.Exec(sql).Error
}

// clearWMTSCache 清空WMTS缓存
func clearWMTSCache(db *gorm.DB, tableName string) error {
	return db.Exec(fmt.Sprintf(`TRUNCATE TABLE "%s"`, tableName)).Error
}

// GetWMTSTile 获取 WMTS 瓦片
func (uc *UserController) GetWMTSTile(c *gin.Context) {
	layerName := strings.ToLower(c.Param("layername"))
	x, _ := strconv.Atoi(c.Param("x"))
	y, _ := strconv.Atoi(strings.TrimSuffix(c.Param("y"), ".png"))
	z, _ := strconv.Atoi(c.Param("z"))

	DB := models.DB

	// 1. 查询 WmtsSchema 获取配置
	var wmtsSchema models.WmtsSchema
	result := DB.Where("layer_name = ?", layerName).First(&wmtsSchema)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			c.String(http.StatusNotFound, "WMTS服务未发布")
		} else {
			c.String(http.StatusInternalServerError, "数据库查询失败")
		}
		return
	}

	// 2. 生成瓦片
	pngData := pgmvt.GenerateWMTSTile(x, y, z, layerName, wmtsSchema, DB)

	if pngData != nil {
		c.Data(http.StatusOK, "image/png", pngData)
	} else {
		// 返回空白透明瓦片
		c.Data(http.StatusOK, "image/png", pgmvt.GetEmptyTile())
	}
}

// UpdateWMTSStyle 更新 WMTS 样式
func (uc *UserController) UpdateWMTSStyle(c *gin.Context) {
	layerName := strings.ToLower(c.Query("layername"))
	opacityStr := c.Query("opacity")

	if layerName == "" {
		response.Error(c, 500, "layername参数不能为空")
		return
	}

	DB := models.DB

	// 查询现有配置
	var wmtsSchema models.WmtsSchema
	result := DB.Where("layer_name = ?", layerName).First(&wmtsSchema)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			response.Error(c, 500, "WMTS服务未发布")
		} else {
			response.Error(c, 500, "数据库查询失败")
		}
		return
	}

	// 更新透明度
	if opacityStr != "" {
		opacity, err := strconv.ParseFloat(opacityStr, 64)
		if err != nil || opacity < 0 || opacity > 1 {
			response.Error(c, 500, "opacity参数格式错误，应为0-1之间的数值")
			return
		}
		wmtsSchema.Opacity = opacity
	}

	// 重新获取颜色配置
	colorData := GetColor(layerName)
	colorConfigJSON, err := json.Marshal(colorData)
	if err != nil {
		response.Error(c, 500, "颜色配置序列化失败")
		return
	}
	wmtsSchema.ColorConfig = datatypes.JSON(colorConfigJSON)

	// 保存更新
	if err := DB.Save(&wmtsSchema).Error; err != nil {
		response.Error(c, 500, "更新WMTS样式失败")
		return
	}

	// 清空缓存
	cacheTableName := layerName + "_wmts"
	if err := clearWMTSCache(DB, cacheTableName); err != nil {
		response.Error(c, 500, fmt.Sprintf("清空缓存失败: %v", err))
		return
	}

	response.SuccessWithMessage(c, "WMTS样式更新成功，缓存已清空", gin.H{
		"layer_name": layerName,
		"opacity":    wmtsSchema.Opacity,
	})
}

// UnpublishWMTS 注销 WMTS 服务
func (uc *UserController) UnpublishWMTS(c *gin.Context) {
	layerName := strings.ToLower(c.Param("layername"))

	if layerName == "" {
		response.Error(c, 500, "layername参数不能为空")
		return
	}

	DB := models.DB

	// 删除配置
	result := DB.Where("layer_name = ?", layerName).Delete(&models.WmtsSchema{})
	if result.Error != nil {
		response.Error(c, 500, "删除WMTS配置失败")
		return
	}

	if result.RowsAffected == 0 {
		response.Error(c, 500, "WMTS服务不存在")
		return
	}

	// 删除缓存表
	cacheTableName := layerName + "_wmts"
	if err := dropWMTSCacheTable(DB, cacheTableName); err != nil {
		// 记录错误但不影响主流程
		fmt.Printf("删除缓存表失败: %v\n", err)
	}

	response.SuccessWithMessage(c, "WMTS服务注销成功", gin.H{
		"layer_name": layerName,
	})
}

// dropWMTSCacheTable 删除WMTS缓存表
func dropWMTSCacheTable(db *gorm.DB, tableName string) error {
	return db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, tableName)).Error
}

// ClearWMTSCache 清空指定图层的缓存
func (uc *UserController) ClearWMTSCache(c *gin.Context) {
	layerName := strings.ToLower(c.Param("layername"))

	if layerName == "" {
		response.Error(c, 500, "layername参数不能为空")
		return
	}

	DB := models.DB
	cacheTableName := layerName + "_wmts"

	if err := clearWMTSCache(DB, cacheTableName); err != nil {
		response.Error(c, 500, fmt.Sprintf("清空缓存失败: %v", err))
		return
	}

	response.SuccessWithMessage(c, "缓存清空成功", gin.H{
		"layer_name": layerName,
	})
}

// GetWMTSCacheStats 获取缓存统计信息
func (uc *UserController) GetWMTSCacheStats(c *gin.Context) {
	layerName := strings.ToLower(c.Param("layername"))

	if layerName == "" {
		response.Error(c, 500, "layername参数不能为空")
		return
	}

	DB := models.DB
	cacheTableName := layerName + "_wmts"

	var stats struct {
		Count     int64
		TotalSize int64
	}

	sql := fmt.Sprintf(`
		SELECT 
			COUNT(*) as count,
			SUM(LENGTH(byte)) as total_size
		FROM "%s"
	`, cacheTableName)

	DB.Raw(sql).Scan(&stats)

	response.Success(c, gin.H{
		"layer_name":       layerName,
		"cached_tiles":     stats.Count,
		"total_size_bytes": stats.TotalSize,
		"total_size_mb":    float64(stats.TotalSize) / 1024 / 1024,
	})
}
