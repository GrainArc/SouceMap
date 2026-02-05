package GdalView

import (
	"github.com/GrainArc/SouceMap/services"
	"github.com/gin-gonic/gin"
	"net/http"
)

func (h *UserController) ClipRaster(c *gin.Context) {
	var req services.ClipRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartClipTask(&req)
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

// GetTaskStatus 查询任务状态

func (h *UserController) GetRasterTaskStatus(c *gin.Context) {
	taskID := c.Query("taskId")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "taskId不能为空"})
		return
	}

	record, err := h.rasterService.GetTaskStatus(taskID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "任务不存在"})
		return
	}

	statusText := map[int]string{0: "运行中", 1: "执行完成", 2: "执行失败"}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": gin.H{
			"task_id":     record.TaskID,
			"status":      record.Status,
			"status_text": statusText[record.Status],
			"output_path": record.OutputPath,
			"source_path": record.SourcePath,
		},
	})
}

// MosaicRaster 栅格镶嵌
func (h *UserController) MosaicRaster(c *gin.Context) {
	var req services.MosaicRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartMosaicTask(&req)
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

// GetMosaicPreview 获取镶嵌预览信息
func (h *UserController) GetMosaicPreview(c *gin.Context) {
	var req services.MosaicPreviewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.GetMosaicPreview(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": resp,
	})
}
