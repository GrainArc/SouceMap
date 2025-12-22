package GdalView

import (
	"fmt"
	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/config"
	"github.com/gin-gonic/gin"

	"log"
	"net/http"

	"sync"
	"time"
)

// TerrainTileTask 地形切片任务
type TerrainTileTask struct {
	TaskID         string                `json:"task_id"`
	Status         string                `json:"status"` // pending, running, completed, failed, cancelled
	Progress       float64               `json:"progress"`
	Message        string                `json:"message"`
	InputPath      string                `json:"input_path"`
	OutputPath     string                `json:"output_path"`
	Options        *Gogeo.MBTilesOptions `json:"options"`
	TerrainOptions *Gogeo.TerrainOptions `json:"terrain_options"`
	StartTime      time.Time             `json:"start_time"`
	EndTime        *time.Time            `json:"end_time,omitempty"`
	Error          string                `json:"error,omitempty"`

	// 内部使用
	generator   *Gogeo.MBTilesGenerator
	cancelFunc  func()
	mu          sync.RWMutex
	subscribers map[string]chan ProgressUpdate
}

// 全局地形任务管理器
var (
	terrainTaskManager = &TerrainTaskManager{
		tasks: make(map[string]*TerrainTileTask),
	}
)

// TerrainTaskManager 地形任务管理器
type TerrainTaskManager struct {
	tasks map[string]*TerrainTileTask
	mu    sync.RWMutex
}

// AddTask 添加任务
func (tm *TerrainTaskManager) AddTask(task *TerrainTileTask) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.tasks[task.TaskID] = task
}

// GetTask 获取任务
func (tm *TerrainTaskManager) GetTask(taskID string) (*TerrainTileTask, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	task, ok := tm.tasks[taskID]
	return task, ok
}

// RemoveTask 移除任务
func (tm *TerrainTaskManager) RemoveTask(taskID string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.tasks, taskID)
}

// StartTerrainTileRequest 启动地形切片请求
type StartTerrainTileRequest struct {
	InputPath   string `json:"input_path" binding:"required"`
	TileSize    int    `json:"tile_size"`
	MinZoom     int    `json:"min_zoom"`
	MaxZoom     int    `json:"max_zoom"`
	Concurrency int    `json:"concurrency"`
	Encoding    string `json:"encoding"` // "mapbox" 或 "terrarium"
	BatchSize   int    `json:"batch_size"`
}

// StartTerrainTile 启动地形切片任务
func (uc *UserController) StartTerrainTile(c *gin.Context) {
	var req StartTerrainTileRequest
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

	// 设置默认值
	if req.TileSize == 0 {
		req.TileSize = 256
	}
	if req.MaxZoom == 0 {
		req.MaxZoom = 14
	}
	if req.Concurrency == 0 {
		req.Concurrency = 8
	}
	if req.Encoding == "" {
		req.Encoding = "mapbox"
	}
	if req.BatchSize == 0 {
		req.BatchSize = 500
	}

	// 验证编码类型
	if req.Encoding != "mapbox" && req.Encoding != "terrarium" {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "Invalid encoding type. Must be 'mapbox' or 'terrarium'",
		})
		return
	}

	// 生成任务ID
	taskID := generateTaskID()

	// 创建MBTiles选项
	mbtilesOptions := &Gogeo.MBTilesOptions{
		TileSize:    req.TileSize,
		MinZoom:     req.MinZoom,
		MaxZoom:     req.MaxZoom,
		Concurrency: req.Concurrency,
	}

	// 创建地形选项
	terrainOptions := &Gogeo.TerrainOptions{
		TileSize:    req.TileSize,
		MinZoom:     req.MinZoom,
		MaxZoom:     req.MaxZoom,
		Encoding:    req.Encoding,
		Concurrency: req.Concurrency,
		BatchSize:   req.BatchSize,
	}

	// 创建任务
	task := &TerrainTileTask{
		TaskID:         taskID,
		Status:         "pending",
		Progress:       0,
		Message:        "Task created",
		InputPath:      req.InputPath,
		OutputPath:     config.MainConfig.Dem,
		Options:        mbtilesOptions,
		TerrainOptions: terrainOptions,
		StartTime:      time.Now(),
		subscribers:    make(map[string]chan ProgressUpdate),
	}

	// 添加到任务管理器
	terrainTaskManager.AddTask(task)

	// 异步执行地形切片任务
	go executeTerrainTileTask(task)

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": "Terrain tile task started successfully",
		"data": gin.H{
			"task_id": taskID,
		},
	})
}

// TerrainTileWebSocket WebSocket连接处理
func (uc *UserController) TerrainTileWebSocket(c *gin.Context) {
	taskID := c.Param("taskId")

	// 获取任务
	task, ok := terrainTaskManager.GetTask(taskID)
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

// GetTerrainTileTaskStatus 获取地形切片任务状态
func (uc *UserController) GetTerrainTileTaskStatus(c *gin.Context) {
	taskID := c.Param("taskId")

	// 获取任务
	task, ok := terrainTaskManager.GetTask(taskID)
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
		"task_id":         task.TaskID,
		"status":          task.Status,
		"progress":        task.Progress,
		"message":         task.Message,
		"input_path":      task.InputPath,
		"output_path":     task.OutputPath,
		"start_time":      task.StartTime,
		"terrain_options": task.TerrainOptions,
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

// executeTerrainTileTask 执行地形切片任务
func executeTerrainTileTask(task *TerrainTileTask) {
	// 更新任务状态为运行中
	updateTerrainTaskStatus(task, "running", 0, "Starting terrain tile generation")

	// 创建进度回调函数
	progressCallback := func(progress float64, message string) bool {
		updateTerrainTaskStatus(task, "running", progress, message)
		return true // 继续执行
	}

	// 更新选项中的回调
	task.Options.ProgressCallback = progressCallback
	task.TerrainOptions.ProgressCallback = progressCallback

	// 创建生成器
	generator, err := Gogeo.NewMBTilesGenerator(task.InputPath, task.Options)
	if err != nil {
		endTime := time.Now()
		task.mu.Lock()
		task.Status = "failed"
		task.Error = fmt.Sprintf("Failed to create generator: %v", err)
		task.EndTime = &endTime
		task.mu.Unlock()

		broadcastTerrainUpdate(task, ProgressUpdate{
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

	// 执行地形瓦片生成
	generateErr := generator.GenerateTerrainMBTiles(task.OutputPath, task.TerrainOptions)

	endTime := time.Now()
	task.mu.Lock()
	task.EndTime = &endTime
	task.mu.Unlock()

	if generateErr != nil {
		task.mu.Lock()
		task.Status = "failed"
		task.Error = generateErr.Error()
		task.mu.Unlock()

		broadcastTerrainUpdate(task, ProgressUpdate{
			Progress: task.Progress,
			Message:  fmt.Sprintf("Task failed: %v", generateErr),
			Status:   "failed",
		})
	} else {
		updateTerrainTaskStatus(task, "completed", 1.0, "Terrain tile generation completed successfully")
	}
}

// updateTerrainTaskStatus 更新地形任务状态
func updateTerrainTaskStatus(task *TerrainTileTask, status string, progress float64, message string) {
	task.mu.Lock()
	task.Status = status
	task.Progress = progress
	task.Message = message
	task.mu.Unlock()

	// 广播更新
	broadcastTerrainUpdate(task, ProgressUpdate{
		Progress: progress,
		Message:  message,
		Status:   status,
	})
}

// broadcastTerrainUpdate 广播进度更新到所有订阅者
func broadcastTerrainUpdate(task *TerrainTileTask, update ProgressUpdate) {
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
