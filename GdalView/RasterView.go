package GdalView

import (
	"github.com/GrainArc/SouceMap/models"
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

// Controller 方法
func (h *UserController) GetRasterTaskList(c *gin.Context) {
	var req services.QueryRasterTasksRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误"})
		return
	}

	// 调用 service 层
	result, err := h.rasterService.GetTaskList(req.Page, req.PageSize, req.Status, req.TaskID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": result,
	})
}
func (h *UserController) DeleteRasterTask(c *gin.Context) {
	taskID := c.Query("taskId")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误: taskId必填"})
		return
	}

	result := models.DB.Where("task_id = ?", taskID).Delete(&models.RasterRecord{})
	if result.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "删除失败"})
		return
	}
	if result.RowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "任务不存在"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"msg":  "删除成功",
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

func (h *UserController) DefineProjection(c *gin.Context) {
	var req services.DefineProjectionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	resp, err := h.rasterService.StartDefineProjectionTask(&req)
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

// DefineProjectionWithGeoTransform 定义投影并设置地理变换
func (h *UserController) DefineProjectionWithGeoTransform(c *gin.Context) {
	var req services.DefineProjectionWithGeoTransformRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	resp, err := h.rasterService.StartDefineProjectionWithGeoTransformTask(&req)
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

// ReprojectRaster 栅格重投影
func (h *UserController) ReprojectRaster(c *gin.Context) {
	var req services.ReprojectRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	resp, err := h.rasterService.StartReprojectTask(&req)
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

// GetProjectionInfo 获取栅格投影信息
func (h *UserController) GetProjectionInfo(c *gin.Context) {
	sourcePath := c.Query("source_path")
	if sourcePath == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source_path不能为空"})
		return
	}
	info, err := h.rasterService.GetProjectionInfo(sourcePath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": info,
	})
}

// ResampleRaster 栅格重采样
func (h *UserController) ResampleRaster(c *gin.Context) {
	var req services.ResampleRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.StartResampleTask(&req)
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

// GetResamplePreview 获取重采样预览信息
func (h *UserController) GetResamplePreview(c *gin.Context) {
	var req services.ResamplePreviewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := h.rasterService.GetResamplePreview(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": resp,
	})
}
