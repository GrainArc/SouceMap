// webtile_handler.go
package tile_proxy

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// WebTileHandler 网络瓦片处理器
type WebTileHandler struct {
	downloader *WebTileDownloader
}

// NewWebTileHandler 创建处理器
func NewWebTileHandler(outputDir string) *WebTileHandler {
	return &WebTileHandler{
		downloader: NewWebTileDownloader(outputDir),
	}
}

// RegisterRoutes 注册路由
func (h *WebTileHandler) RegisterRoutes(r *gin.RouterGroup) {
	r.POST("/download_webtile/init", h.InitDownload)
	r.GET("/download_webtile/ws", h.ConnectWebSocket)
	r.GET("/download_webtile/status/:taskId", h.GetTaskStatus)
	r.GET("/download_webtile/download/:taskId", h.DownloadResult)
}

// InitDownload 初始化下载任务
func (h *WebTileHandler) InitDownload(c *gin.Context) {
	h.downloader.InitDownload(c)
}

// ConnectWebSocket WebSocket连接
func (h *WebTileHandler) ConnectWebSocket(c *gin.Context) {
	h.downloader.ConnectWebSocket(c)
}

// GetTaskStatus 获取任务状态（轮询备用）
func (h *WebTileHandler) GetTaskStatus(c *gin.Context) {
	taskID := c.Param("taskId")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "taskId is required"})
		return
	}

	task, ok := h.downloader.GetTask(taskID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"code": 404, "error": "task not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":            200,
		"status":          task.Status,
		"progress":        task.Progress,
		"message":         task.Message,
		"totalTiles":      task.TotalTiles,
		"downloadedTiles": task.DownloadedTiles,
		"failedTiles":     task.FailedTiles,
	})
}

// DownloadResult 下载结果文件
func (h *WebTileHandler) DownloadResult(c *gin.Context) {
	taskID := c.Param("taskId")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "taskId is required"})
		return
	}

	task, ok := h.downloader.GetTask(taskID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"code": 404, "error": "task not found"})
		return
	}

	if task.Status != "completed" {
		c.JSON(http.StatusBadRequest, gin.H{"code": 400, "error": "task not completed"})
		return
	}

	if task.OutputFile == "" {
		c.JSON(http.StatusNotFound, gin.H{"code": 404, "error": "output file not found"})
		return
	}

	c.File(task.OutputFile)
}

// Close 关闭处理器
func (h *WebTileHandler) Close() error {
	return h.downloader.Close()
}
