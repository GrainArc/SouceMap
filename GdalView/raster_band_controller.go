package GdalView

import (
	"github.com/GrainArc/SouceMap/services"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
)

// ==================== 波段信息查询 ====================

// GetBandInfo 获取单个波段信息
func (h *UserController) GetBandInfo(c *gin.Context) {
	sourcePath := c.Query("source_path")
	bandIndexStr := c.Query("band_index")

	if sourcePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source_path不能为空"})
		return
	}

	bandIndex, err := strconv.Atoi(bandIndexStr)
	if err != nil || bandIndex < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "band_index无效"})
		return
	}

	resp, err := h.rasterService.GetBandInfo(sourcePath, bandIndex)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": resp,
	})
}

// GetAllBandsInfo 获取所有波段信息
func (h *UserController) GetAllBandsInfo(c *gin.Context) {
	sourcePath := c.Query("source_path")

	if sourcePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source_path不能为空"})
		return
	}

	resp, err := h.rasterService.GetAllBandsInfo(sourcePath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": resp,
	})
}

// GetBandHistogram 获取波段直方图
func (h *UserController) GetBandHistogram(c *gin.Context) {
	var req services.GetBandHistogramRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.GetBandHistogram(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": resp,
	})
}

// GetPaletteInfo 获取调色板信息
func (h *UserController) GetPaletteInfo(c *gin.Context) {
	sourcePath := c.Query("source_path")
	bandIndexStr := c.Query("band_index")

	if sourcePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source_path不能为空"})
		return
	}

	bandIndex, err := strconv.Atoi(bandIndexStr)
	if err != nil || bandIndex < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "band_index无效"})
		return
	}

	resp, err := h.rasterService.GetPaletteInfo(sourcePath, bandIndex)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": resp,
	})
}

// ==================== 波段属性设置 ====================

// SetBandColorInterp 设置波段颜色解释
func (h *UserController) SetBandColorInterp(c *gin.Context) {
	var req services.SetColorInterpRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartSetColorInterpTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// SetBandNoData 设置波段NoData值
func (h *UserController) SetBandNoData(c *gin.Context) {
	var req services.SetNoDataRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartSetNoDataTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// ==================== 波段操作 ====================

// AddBand 添加波段
func (h *UserController) AddBand(c *gin.Context) {
	var req services.AddBandRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartAddBandTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// RemoveBand 删除波段
func (h *UserController) RemoveBand(c *gin.Context) {
	var req services.RemoveBandRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartRemoveBandTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// ReorderBands 重排波段
func (h *UserController) ReorderBands(c *gin.Context) {
	var req services.ReorderBandsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartReorderBandsTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// ConvertBandType 转换波段数据类型
func (h *UserController) ConvertBandType(c *gin.Context) {
	var req services.ConvertBandTypeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartConvertBandTypeTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// MergeBands 合并波段
func (h *UserController) MergeBands(c *gin.Context) {
	var req services.MergeBandsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartMergeBandsTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// ==================== 波段运算 ====================

// BandMath 波段数学运算
func (h *UserController) BandMath(c *gin.Context) {
	var req services.BandMathRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartBandMathTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// CalculateIndex 计算植被/水体指数
func (h *UserController) CalculateIndex(c *gin.Context) {
	var req services.CalculateIndexRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartCalculateIndexTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// NormalizeBand 归一化波段
func (h *UserController) NormalizeBand(c *gin.Context) {
	var req services.NormalizeBandRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartNormalizeBandTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// ==================== 滤波与重分类 ====================

// ApplyFilter 应用滤波器
func (h *UserController) ApplyFilter(c *gin.Context) {
	var req services.ApplyFilterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartApplyFilterTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// ReclassifyBand 重分类波段
func (h *UserController) ReclassifyBand(c *gin.Context) {
	var req services.ReclassifyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartReclassifyTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// ==================== 调色板操作 ====================

// SetPalette 设置调色板
func (h *UserController) SetPalette(c *gin.Context) {
	var req services.SetPaletteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartSetPaletteTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// PaletteToRGB 调色板转RGB
func (h *UserController) PaletteToRGB(c *gin.Context) {
	var req services.PaletteToRGBRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartPaletteToRGBTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// RGBToPalette RGB转调色板
func (h *UserController) RGBToPalette(c *gin.Context) {
	var req services.RGBToPaletteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartRGBToPaletteTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// ==================== 元数据操作 ====================

// SetBandMetadata 设置波段元数据
func (h *UserController) SetBandMetadata(c *gin.Context) {
	var req services.SetBandMetadataRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartSetBandMetadataTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// GetBandMetadata 获取波段元数据
func (h *UserController) GetBandMetadata(c *gin.Context) {
	sourcePath := c.Query("source_path")
	bandIndexStr := c.Query("band_index")
	key := c.Query("key")

	if sourcePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source_path不能为空"})
		return
	}

	bandIndex, err := strconv.Atoi(bandIndexStr)
	if err != nil || bandIndex < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "band_index无效"})
		return
	}

	if key != "" {
		// 获取单个元数据
		value, err := h.rasterService.GetBandMetadata(sourcePath, bandIndex, key)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"code": 0,
			"data": gin.H{
				"key":   key,
				"value": value,
			},
		})
	} else {
		// 获取所有元数据
		metadata, err := h.rasterService.GetAllBandMetadata(sourcePath, bandIndex)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"code": 0,
			"data": metadata,
		})
	}
}

// SetBandDescription 设置波段描述
func (h *UserController) SetBandDescription(c *gin.Context) {
	var req services.SetBandDescriptionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartSetBandDescriptionTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":    0,
		"message": "任务已提交",
		"data":    resp,
	})
}

// GetBandDescription 获取波段描述
func (h *UserController) GetBandDescription(c *gin.Context) {
	sourcePath := c.Query("source_path")
	bandIndexStr := c.Query("band_index")

	if sourcePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source_path不能为空"})
		return
	}

	bandIndex, err := strconv.Atoi(bandIndexStr)
	if err != nil || bandIndex < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "band_index无效"})
		return
	}

	desc, err := h.rasterService.GetBandDescription(sourcePath, bandIndex)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": gin.H{
			"band_index":  bandIndex,
			"description": desc,
		},
	})
}

// GetBandScaleOffset 获取波段缩放和偏移
func (h *UserController) GetBandScaleOffset(c *gin.Context) {
	sourcePath := c.Query("source_path")
	bandIndexStr := c.Query("band_index")

	if sourcePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source_path不能为空"})
		return
	}

	bandIndex, err := strconv.Atoi(bandIndexStr)
	if err != nil || bandIndex < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "band_index无效"})
		return
	}

	scale, offset, err := h.rasterService.GetBandScaleOffset(sourcePath, bandIndex)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": gin.H{
			"band_index": bandIndex,
			"scale":      scale,
			"offset":     offset,
		},
	})
}

// GetBandUnitType 获取波段单位类型
func (h *UserController) GetBandUnitType(c *gin.Context) {
	sourcePath := c.Query("source_path")
	bandIndexStr := c.Query("band_index")

	if sourcePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source_path不能为空"})
		return
	}

	bandIndex, err := strconv.Atoi(bandIndexStr)
	if err != nil || bandIndex < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "band_index无效"})
		return
	}

	unitType, err := h.rasterService.GetBandUnitType(sourcePath, bandIndex)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": gin.H{
			"band_index": bandIndex,
			"unit_type":  unitType,
		},
	})
}
