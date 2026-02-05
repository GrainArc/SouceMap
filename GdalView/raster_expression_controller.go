package GdalView

import (
	"net/http"

	"github.com/GrainArc/SouceMap/services"
	"github.com/gin-gonic/gin"
)

// ==================== 栅格计算器 ====================

// CalculateExpression 表达式计算
func (h *UserController) CalculateExpression(c *gin.Context) {
	var req services.ExpressionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartExpressionTask(&req)
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

// CalculateWithCondition 条件表达式计算
func (h *UserController) CalculateWithCondition(c *gin.Context) {
	var req services.ExpressionWithConditionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartExpressionWithConditionTask(&req)
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

// ConditionalReplace 条件替换
func (h *UserController) ConditionalReplace(c *gin.Context) {
	var req services.ConditionalReplaceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartConditionalReplaceTask(&req)
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

// CalculateBatch 批量表达式计算
func (h *UserController) CalculateBatch(c *gin.Context) {
	var req services.BatchExpressionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartBatchExpressionTask(&req)
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

// CalculateBlock 分块计算
func (h *UserController) CalculateBlock(c *gin.Context) {
	var req services.BlockCalculateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartBlockCalculateTask(&req)
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

// ValidateExpression 验证表达式
func (h *UserController) ValidateExpression(c *gin.Context) {
	var req services.ValidateExpressionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.ValidateExpression(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": resp,
	})
}

// ==================== 遥感指数计算 ====================

// CalculateNDVI 计算NDVI
func (h *UserController) CalculateNDVI(c *gin.Context) {
	var req services.IndexCalculateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.NIRBand < 1 || req.RedBand < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "nir_band和red_band必须大于0"})
		return
	}

	resp, err := h.rasterService.StartNDVITask(&req)
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

// CalculateNDWI 计算NDWI
func (h *UserController) CalculateNDWI(c *gin.Context) {
	var req services.IndexCalculateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.GreenBand < 1 || req.NIRBand < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "green_band和nir_band必须大于0"})
		return
	}

	resp, err := h.rasterService.StartNDWITask(&req)
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

// CalculateEVI 计算EVI
func (h *UserController) CalculateEVI(c *gin.Context) {
	var req services.IndexCalculateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.NIRBand < 1 || req.RedBand < 1 || req.BlueBand < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "nir_band、red_band和blue_band必须大于0"})
		return
	}

	resp, err := h.rasterService.StartEVITask(&req)
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

// CalculateSAVI 计算SAVI
func (h *UserController) CalculateSAVI(c *gin.Context) {
	var req services.IndexCalculateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.NIRBand < 1 || req.RedBand < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "nir_band和red_band必须大于0"})
		return
	}

	resp, err := h.rasterService.StartSAVITask(&req)
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

// CalculateMNDWI 计算MNDWI
func (h *UserController) CalculateMNDWI(c *gin.Context) {
	var req services.IndexCalculateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.GreenBand < 1 || req.SWIRBand < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "green_band和swir_band必须大于0"})
		return
	}

	resp, err := h.rasterService.StartMNDWITask(&req)
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

// CalculateNDBI 计算NDBI
func (h *UserController) CalculateNDBI(c *gin.Context) {
	var req services.IndexCalculateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.SWIRBand < 1 || req.NIRBand < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "swir_band和nir_band必须大于0"})
		return
	}

	resp, err := h.rasterService.StartNDBITask(&req)
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

// CalculateNDSI 计算NDSI
func (h *UserController) CalculateNDSI(c *gin.Context) {
	var req services.IndexCalculateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.GreenBand < 1 || req.SWIRBand < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "green_band和swir_band必须大于0"})
		return
	}

	resp, err := h.rasterService.StartNDSITask(&req)
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

// CalculateLAI 计算LAI
func (h *UserController) CalculateLAI(c *gin.Context) {
	var req services.IndexCalculateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.NIRBand < 1 || req.RedBand < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "nir_band和red_band必须大于0"})
		return
	}

	resp, err := h.rasterService.StartLAITask(&req)
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
