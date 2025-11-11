package GdalView

import (
	"fmt"
	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/config"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// RasterTileTask 栅格切片任务
type RasterTileTask struct {
	TaskID     string                `json:"task_id"`
	Status     string                `json:"status"` // pending, running, completed, failed, cancelled
	Progress   float64               `json:"progress"`
	Message    string                `json:"message"`
	InputPath  string                `json:"input_path"`
	OutputPath string                `json:"output_path"`
	Options    *Gogeo.MBTilesOptions `json:"options"`
	StartTime  time.Time             `json:"start_time"`
	EndTime    *time.Time            `json:"end_time,omitempty"`
	Error      string                `json:"error,omitempty"`

	// 内部使用
	generator   *Gogeo.MBTilesGenerator
	cancelFunc  func()
	mu          sync.RWMutex
	subscribers map[string]chan ProgressUpdate
}

// ProgressUpdate 进度更新消息
type ProgressUpdate struct {
	Progress float64 `json:"progress"`
	Message  string  `json:"message"`
	Status   string  `json:"status"`
}

// 全局任务管理器
var (
	rasterTaskManager = &RasterTaskManager{
		tasks: make(map[string]*RasterTileTask),
	}
)

// TaskManager 任务管理器
type RasterTaskManager struct {
	tasks map[string]*RasterTileTask
	mu    sync.RWMutex
}

// AddTask 添加任务
func (tm *RasterTaskManager) AddTask(task *RasterTileTask) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.tasks[task.TaskID] = task
}

// GetTask 获取任务
func (tm *RasterTaskManager) GetTask(taskID string) (*RasterTileTask, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	task, ok := tm.tasks[taskID]
	return task, ok
}

// RemoveTask 移除任务
func (tm *RasterTaskManager) RemoveTask(taskID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.tasks, taskID)
}

// StartRasterTileRequest 启动切片请求
type StartRasterTileRequest struct {
	InputPath   string            `json:"input_path" binding:"required"`
	TileSize    int               `json:"tile_size"`
	MinZoom     int               `json:"min_zoom"`
	MaxZoom     int               `json:"max_zoom"`
	Concurrency int               `json:"concurrency"`
	Metadata    map[string]string `json:"metadata"`
}

// StartRasterTile 启动栅格切片任务
func (uc *UserController) StartRasterTile(c *gin.Context) {
	var req StartRasterTileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "Invalid request parameters",
			"error":   err.Error(),
		})
		return
	}

	// 验证输入文件
	if !fileExists(req.InputPath) {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "Input file does not exist",
		})
		return
	}

	// 生成任务ID
	taskID := generateTaskID()

	// 创建选项
	options := &Gogeo.MBTilesOptions{
		TileSize:    req.TileSize,
		MinZoom:     req.MinZoom,
		MaxZoom:     req.MaxZoom,
		Concurrency: req.Concurrency,
		Metadata:    req.Metadata,
	}
	filename := filepath.Base(req.InputPath)
	ext := filepath.Ext(filename) // 获取扩展名（包含点）
	name := strings.TrimSuffix(filename, ext)
	// 创建任务
	task := &RasterTileTask{
		TaskID:      taskID,
		Status:      "pending",
		Progress:    0,
		Message:     "Task created",
		InputPath:   req.InputPath,
		OutputPath:  filepath.Join(config.Raster, name+".mbtiles"),
		Options:     options,
		StartTime:   time.Now(),
		subscribers: make(map[string]chan ProgressUpdate),
	}

	// 添加到任务管理器
	rasterTaskManager.AddTask(task)

	// 异步执行切片任务
	go executeRasterTileTask(task, req.Metadata)

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Task started successfully",
		"data": gin.H{
			"task_id": taskID,
		},
	})
}

// WebSocket升级器
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // 生产环境需要更严格的检查
	},
}

// RasterTileWebSocket WebSocket连接处理
func (uc *UserController) RasterTileWebSocket(c *gin.Context) {
	taskID := c.Param("taskId")

	// 获取任务
	task, ok := rasterTaskManager.GetTask(taskID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"message": "Task not found",
		})
		return
	}

	// 升级到WebSocket
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}
	defer conn.Close()

	// 生成订阅者ID
	subscriberID := generateTaskID()

	// 创建进度更新通道
	progressChan := make(chan ProgressUpdate, 100)

	// 注册订阅者
	task.mu.Lock()
	task.subscribers[subscriberID] = progressChan
	task.mu.Unlock()

	// 确保退出时清理订阅
	defer func() {
		task.mu.Lock()
		delete(task.subscribers, subscriberID)
		close(progressChan)
		task.mu.Unlock()
	}()

	// 发送当前状态
	currentStatus := ProgressUpdate{
		Progress: task.Progress,
		Message:  task.Message,
		Status:   task.Status,
	}
	if err := conn.WriteJSON(currentStatus); err != nil {
		log.Printf("Error sending initial status: %v", err)
		return
	}

	// 监听进度更新和客户端消息
	done := make(chan struct{})

	// 读取客户端消息的goroutine（用于检测连接断开）
	go func() {
		defer close(done)
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				log.Printf("WebSocket read error: %v", err)
				return
			}
		}
	}()

	// 发送进度更新
	for {
		select {
		case update, ok := <-progressChan:
			if !ok {
				return
			}

			if err := conn.WriteJSON(update); err != nil {
				log.Printf("Error sending progress update: %v", err)
				return
			}

			// 如果任务已完成或失败，发送后关闭连接
			if update.Status == "completed" || update.Status == "failed" || update.Status == "cancelled" {
				time.Sleep(time.Second) // 给客户端一点时间接收消息
				return
			}

		case <-done:
			return
		}
	}
}

// GetRasterTileTaskStatus 获取任务状态
func (uc *UserController) GetRasterTileTaskStatus(c *gin.Context) {
	taskID := c.Param("taskId")

	// 获取任务
	task, ok := rasterTaskManager.GetTask(taskID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{
			"success": false,
			"message": "Task not found",
		})
		return
	}

	task.mu.RLock()
	defer task.mu.RUnlock()

	// 构建响应
	response := gin.H{
		"task_id":     task.TaskID,
		"status":      task.Status,
		"progress":    task.Progress,
		"message":     task.Message,
		"input_path":  task.InputPath,
		"output_path": task.OutputPath,
		"start_time":  task.StartTime,
	}

	if task.EndTime != nil {
		response["end_time"] = task.EndTime
		duration := task.EndTime.Sub(task.StartTime)
		response["duration"] = duration.String()
	}

	if task.Error != "" {
		response["error"] = task.Error
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    response,
	})
}

// executeRasterTileTask 执行切片任务
func executeRasterTileTask(task *RasterTileTask, metadata map[string]string) {
	// 更新任务状态为运行中
	updateTaskStatus(task, "running", 0, "Starting tile generation")

	// 创建进度回调函数
	progressCallback := func(progress float64, message string) bool {
		updateTaskStatus(task, "running", progress, message)
		return true // 继续执行
	}

	// 更新选项中的回调
	task.Options.ProgressCallback = progressCallback

	// 创建生成器

	generator, err := Gogeo.NewMBTilesGenerator(task.InputPath, task.Options)
	if err != nil {
		endTime := time.Now()
		task.mu.Lock()
		task.Status = "failed"
		task.Error = fmt.Sprintf("Failed to create generator: %v", err)
		task.EndTime = &endTime
		task.mu.Unlock()

		broadcastUpdate(task, ProgressUpdate{
			Progress: task.Progress,
			Message:  task.Error,
			Status:   "failed",
		})
		return
	}
	defer generator.Close()

	task.mu.Lock()
	task.generator = generator
	task.mu.Unlock()

	// 执行生成
	var generateErr error
	if task.Options.Concurrency > 1 {
		generateErr = generator.GenerateWithConcurrency(task.OutputPath, metadata, task.Options.Concurrency)
	} else {
		generateErr = generator.Generate(task.OutputPath, metadata)
	}

	endTime := time.Now()
	task.mu.Lock()
	task.EndTime = &endTime
	task.mu.Unlock()

	if generateErr != nil {
		task.mu.Lock()
		task.Status = "failed"
		task.Error = generateErr.Error()
		task.mu.Unlock()

		broadcastUpdate(task, ProgressUpdate{
			Progress: task.Progress,
			Message:  fmt.Sprintf("Task failed: %v", generateErr),
			Status:   "failed",
		})
	} else {
		updateTaskStatus(task, "completed", 1.0, "Task completed successfully")
	}
}

// updateTaskStatus 更新任务状态
func updateTaskStatus(task *RasterTileTask, status string, progress float64, message string) {
	task.mu.Lock()
	task.Status = status
	task.Progress = progress
	task.Message = message
	task.mu.Unlock()

	// 广播更新
	broadcastUpdate(task, ProgressUpdate{
		Progress: progress,
		Message:  message,
		Status:   status,
	})
}

// broadcastUpdate 广播进度更新到所有订阅者
func broadcastUpdate(task *RasterTileTask, update ProgressUpdate) {
	task.mu.RLock()
	defer task.mu.RUnlock()

	for _, ch := range task.subscribers {
		select {
		case ch <- update:
		default:
			// 通道已满，跳过
		}
	}
}

// 辅助函数

func generateTaskID() string {
	return fmt.Sprintf("task_%d", time.Now().UnixNano())
}

func fileExists(path string) bool {
	_, err := filepath.Abs(path)
	return err == nil
}
