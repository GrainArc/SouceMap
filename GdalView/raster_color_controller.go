package GdalView

import (
	"github.com/GrainArc/SouceMap/services"
	"github.com/gin-gonic/gin"
	"net/http"
)

// ==================== 调色接口 ====================

// AdjustColors 综合调色
func (h *UserController) AdjustColors(c *gin.Context) {
	var req services.ColorAdjustRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartColorAdjustTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// AdjustBrightness 调整亮度
func (h *UserController) AdjustBrightness(c *gin.Context) {
	var req services.SingleColorAdjustRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartBrightnessTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// AdjustContrast 调整对比度
func (h *UserController) AdjustContrast(c *gin.Context) {
	var req services.SingleColorAdjustRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartContrastTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// AdjustSaturation 调整饱和度
func (h *UserController) AdjustSaturation(c *gin.Context) {
	var req services.SingleColorAdjustRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartSaturationTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// AdjustGamma Gamma校正
func (h *UserController) AdjustGamma(c *gin.Context) {
	var req services.SingleColorAdjustRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartGammaTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// AdjustHue 调整色相
func (h *UserController) AdjustHue(c *gin.Context) {
	var req services.SingleColorAdjustRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartHueTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// AdjustLevels 色阶调整
func (h *UserController) AdjustLevels(c *gin.Context) {
	var req services.LevelsAdjustRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartLevelsTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// AdjustCurves 曲线调整
func (h *UserController) AdjustCurves(c *gin.Context) {
	var req services.CurvesAdjustRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartCurvesTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// AutoLevels 自动色阶
func (h *UserController) AutoLevels(c *gin.Context) {
	var req services.AutoAdjustRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartAutoLevelsTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// AutoContrast 自动对比度
func (h *UserController) AutoContrast(c *gin.Context) {
	var req services.AutoAdjustRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartAutoContrastTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// AutoWhiteBalance 自动白平衡
func (h *UserController) AutoWhiteBalance(c *gin.Context) {
	var req services.AutoAdjustRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartAutoWhiteBalanceTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// HistogramEqualize 直方图均衡化
func (h *UserController) HistogramEqualize(c *gin.Context) {
	var req services.HistogramEqualizeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartHistogramEqualizeTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// CLAHEEqualize CLAHE均衡化
func (h *UserController) CLAHEEqualize(c *gin.Context) {
	var req services.CLAHERequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartCLAHETask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// PresetColor 预设调色
func (h *UserController) PresetColor(c *gin.Context) {
	var req services.PresetColorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartPresetColorTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// SCurveContrast S曲线对比度
func (h *UserController) SCurveContrast(c *gin.Context) {
	var req services.SCurveRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartSCurveTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// ==================== 匀色接口 ====================

// GetColorStatistics 获取颜色统计信息
func (h *UserController) GetColorStatistics(c *gin.Context) {
	var req services.GetColorStatisticsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.GetColorStatistics(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "data": resp})
}

// GetBandStatistics 获取波段统计信息
func (h *UserController) GetBandStatistics(c *gin.Context) {
	var req services.GetBandStatisticsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.GetBandStatistics(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "data": resp})
}

// HistogramMatch 直方图匹配
func (h *UserController) HistogramMatch(c *gin.Context) {
	var req services.HistogramMatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartHistogramMatchTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// MeanStdMatch 均值-标准差匹配
func (h *UserController) MeanStdMatch(c *gin.Context) {
	var req services.MeanStdMatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartMeanStdMatchTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// WallisFilter Wallis滤波
func (h *UserController) WallisFilter(c *gin.Context) {
	var req services.WallisFilterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartWallisFilterTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// MomentMatch 矩匹配
func (h *UserController) MomentMatch(c *gin.Context) {
	var req services.MomentMatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartMomentMatchTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// LinearRegressionBalance 线性回归匀色
func (h *UserController) LinearRegressionBalance(c *gin.Context) {
	var req services.LinearRegressionBalanceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartLinearRegressionBalanceTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// DodgingBalance Dodging匀光
func (h *UserController) DodgingBalance(c *gin.Context) {
	var req services.DodgingBalanceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartDodgingBalanceTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// GradientBlend 渐变融合
func (h *UserController) GradientBlend(c *gin.Context) {
	var req services.GradientBlendRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartGradientBlendTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// ColorBalance 通用匀色
func (h *UserController) ColorBalance(c *gin.Context) {
	var req services.ColorBalanceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartColorBalanceTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// AutoColorBalance 自动匀色
func (h *UserController) AutoColorBalance(c *gin.Context) {
	var req services.AutoColorBalanceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartAutoColorBalanceTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// BatchColorBalance 批量匀色
func (h *UserController) BatchColorBalance(c *gin.Context) {
	var req services.BatchColorBalanceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartBatchColorBalanceTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}

// ColorPipeline 调色管道
func (h *UserController) ColorPipeline(c *gin.Context) {
	var req services.ColorPipelineRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": -1, "error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartColorPipelineTask(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"code": -1, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "任务已提交", "data": resp})
}
