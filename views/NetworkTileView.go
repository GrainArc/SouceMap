package views

import (
	"github.com/GrainArc/SouceMap/models"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
)

func (uc *UserController) CreateNetMap(c *gin.Context) {
	var netMap models.NetMap

	if err := c.ShouldBindJSON(&netMap); err != nil {
		c.JSON(http.StatusOK,
			gin.H{
				"error": err.Error(),
				"code":  500,
			})
		return
	}

	if err := models.DB.Create(&netMap).Error; err != nil {
		c.JSON(http.StatusOK, gin.H{"error": "创建失败: " + err.Error(), "code": 500})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": netMap,
	})
}

func (uc *UserController) GetNetMapByID(c *gin.Context) {
	id := c.Param("id")
	var netMap models.NetMap

	if err := models.DB.First(&netMap, id).Error; err != nil {
		c.JSON(http.StatusOK, gin.H{
			"error": "地图不存在",
			"code":  500,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": netMap,
	})
}

func (uc *UserController) ListNetMaps(c *gin.Context) {
	var netMaps []models.NetMap
	var total int64

	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("pageSize", "10"))

	query := models.DB

	// 条件筛选
	if mapName := c.Query("mapName"); mapName != "" {
		query = query.Where("map_name ILIKE ?", "%"+mapName+"%")
	}

	if groupName := c.Query("groupName"); groupName != "" {
		query = query.Where("group_name ILIKE ?", "%"+groupName+"%")
	}

	if status := c.Query("status"); status != "" {
		query = query.Where("status = ?", status)
	}

	// 统计总数
	query.Model(&models.NetMap{}).Count(&total)

	// 分页查询
	if err := query.Offset((page - 1) * pageSize).Limit(pageSize).Find(&netMaps).Error; err != nil {
		c.JSON(http.StatusOK, gin.H{
			"error": "查询失败",
			"code":  500,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": gin.H{
			"list":      netMaps,
			"total":     total,
			"page":      page,
			"pageSize":  pageSize,
			"totalPage": (total + int64(pageSize) - 1) / int64(pageSize),
		},
	})
}

func (uc *UserController) UpdateNetMap(c *gin.Context) {
	id := c.Param("id")
	var netMap models.NetMap

	// 检查记录是否存在
	if err := models.DB.First(&netMap, id).Error; err != nil {
		c.JSON(http.StatusOK, gin.H{
			"error": "地图不存在",
			"code":  500,
		})
		return
	}

	if err := c.ShouldBindJSON(&netMap); err != nil {
		c.JSON(http.StatusOK, gin.H{
			"error": err.Error(),
			"code":  500,
		})
		return
	}

	if err := models.DB.Model(&netMap).Updates(&netMap).Error; err != nil {
		c.JSON(http.StatusOK, gin.H{
			"error": "更新失败: " + err.Error(),
			"code":  500,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 200,
		"data": netMap,
	})
}

func (uc *UserController) DeleteNetMap(c *gin.Context) {
	id := c.Param("id")
	var netMap models.NetMap

	if err := models.DB.First(&netMap, id).Error; err != nil {
		c.JSON(http.StatusOK, gin.H{
			"error": "地图不存在",
			"code":  500,
		})
		return
	}

	if err := models.DB.Delete(&netMap).Error; err != nil {
		c.JSON(http.StatusOK, gin.H{
			"error": "删除失败: " + err.Error(),
			"code":  500,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "删除成功",
	})
}

func (uc *UserController) BatchDeleteNetMaps(c *gin.Context) {
	var req struct {
		IDs []uint `json:"ids" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusOK, gin.H{
			"error": err.Error(),
			"code":  500,
		})
		return
	}

	if err := models.DB.Delete(&models.NetMap{}, req.IDs).Error; err != nil {
		c.JSON(http.StatusOK, gin.H{
			"error": "批量删除失败: " + err.Error(),
			"code":  500,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    200,
		"message": "批量删除成功",
		"count":   len(req.IDs),
	})
}
