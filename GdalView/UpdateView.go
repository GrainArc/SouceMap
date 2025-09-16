package GdalView

import (
	"context"
	"fmt"
	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/OSGEO"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"net/http"
	"sync"
	"time"
)

// 空间更新分析请求参数结构体
type UpdateRequest struct {
	InputTable  string  `json:"table1" binding:"required"`
	UpdateTable string  `json:"table2" binding:"required"`
	OutTable    string  `json:"out_table" binding:"required"`
	IsMergeTile bool    `json:"isMergeTile"` // 是否合并瓦片
	MaxWorkers  int     `json:"maxWorkers"`
	GridSize    float64 `json:"gridSize"`
	TileCount   int     `json:"tileCount"`
}

type UpdateTaskInfo struct {
	ID            string                    `json:"id"`
	Status        TaskStatus                `json:"status"`
	UpdateRequest UpdateRequest             `json:"update_request"`
	CreatedAt     time.Time                 `json:"created_at"`
	StartedAt     *time.Time                `json:"started_at,omitempty"`
	EndedAt       *time.Time                `json:"ended_at,omitempty"`
	Error         string                    `json:"error,omitempty"`
	Result        *Gogeo.GeosAnalysisResult `json:"-"` // 不序列化到JSON
	Context       context.Context           `json:"-"`
	Cancel        context.CancelFunc        `json:"-"`
	mutex         sync.RWMutex              `json:"-"`
}

// 参数验证函数
func validateUpdateParams(req *UpdateRequest) error {
	if req.InputTable == "" {
		return fmt.Errorf("input_table不能为空")
	}
	if req.UpdateTable == "" {
		return fmt.Errorf("update_table不能为空")
	}
	if req.OutTable == "" {
		return fmt.Errorf("out_table不能为空")
	}
	if req.MaxWorkers <= 0 {
		req.MaxWorkers = 8 // 默认值
	}
	if req.GridSize < 0 {
		req.GridSize = 0.000000001 // 默认值
	}
	if req.TileCount <= 0 {
		req.TileCount = 10 // 默认值
	}

	return nil
}

// 更新任务管理器
type UpdateTaskManager struct {
	tasks map[string]*UpdateTaskInfo
	mutex sync.RWMutex
}

var updateTaskManager = &UpdateTaskManager{
	tasks: make(map[string]*UpdateTaskInfo),
}

// 添加更新任务
func (utm *UpdateTaskManager) AddTask(task *UpdateTaskInfo) {
	utm.mutex.Lock()
	defer utm.mutex.Unlock()
	utm.tasks[task.ID] = task
}

// 获取更新任务
func (utm *UpdateTaskManager) GetTask(taskID string) (*UpdateTaskInfo, bool) {
	utm.mutex.RLock()
	defer utm.mutex.RUnlock()
	task, exists := utm.tasks[taskID]
	return task, exists
}

// 删除更新任务
func (utm *UpdateTaskManager) RemoveTask(taskID string) {
	utm.mutex.Lock()
	defer utm.mutex.Unlock()
	if task, exists := utm.tasks[taskID]; exists {
		if task.Cancel != nil {
			task.Cancel()
		}
		delete(utm.tasks, taskID)
	}
}

// 更新任务状态
func (task *UpdateTaskInfo) UpdateStatus(status TaskStatus) {
	task.mutex.Lock()
	defer task.mutex.Unlock()
	task.Status = status
	now := time.Now()

	switch status {
	case TaskStatusRunning:
		task.StartedAt = &now
	case TaskStatusCompleted, TaskStatusFailed, TaskStatusCancelled:
		task.EndedAt = &now
	}
}

// StartUpdate 创建并初始化空间更新分析任务
func (uc *UserController) StartUpdate(c *gin.Context) {
	// 解析请求参数
	var req UpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "参数错误: " + err.Error()})
		return
	}

	if err := validateUpdateParams(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	// 创建任务
	taskID := uuid.New().String()
	ctx, cancel := context.WithCancel(context.Background())

	task := &UpdateTaskInfo{
		ID:            taskID,
		Status:        TaskStatusPending,
		UpdateRequest: req,
		CreatedAt:     time.Now(),
		Context:       ctx,
		Cancel:        cancel,
	}

	// 添加到任务管理器
	updateTaskManager.AddTask(task)

	// 返回任务ID
	c.JSON(200, gin.H{
		"task_id": taskID,
		"status":  task.Status,
		"message": "空间更新分析任务已创建，请使用WebSocket连接开始执行",
		"ws_url":  fmt.Sprintf("/gdal/Update/ws/%s", taskID),
	})
}

// UpdateWebSocket 处理WebSocket连接并执行空间更新分析任务
func (uc *UserController) UpdateWebSocket(c *gin.Context) {
	taskID := c.Param("taskId")

	// 获取任务信息
	task, exists := updateTaskManager.GetTask(taskID)
	if !exists {
		c.JSON(404, gin.H{"error": "任务不存在"})
		return
	}

	// 检查任务状态
	task.mutex.RLock()
	if task.Status != TaskStatusPending {
		task.mutex.RUnlock()
		c.JSON(400, gin.H{"error": "任务已经开始或已完成"})
		return
	}
	task.mutex.RUnlock()

	// 升级到WebSocket连接
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		c.JSON(500, gin.H{"error": "WebSocket升级失败"})
		return
	}
	defer ws.Close()

	// 更新任务状态为运行中
	task.UpdateStatus(TaskStatusRunning)

	// 用于协调取消操作的通道
	cancelChan := make(chan bool, 1)

	// 启动goroutine监听客户端取消消息
	go func() {
		for {
			var msg ClientMessage
			err := ws.ReadJSON(&msg)
			if err != nil {
				// WebSocket连接断开或读取错误
				fmt.Printf("WebSocket读取错误: %v\n", err)
				cancelChan <- true
				return
			}

			if msg.Action == "cancel" {
				fmt.Printf("收到任务 %s 的取消请求\n", taskID)
				cancelChan <- true
				task.Cancel() // 取消context
				return
			}
		}
	}()

	// 进度回调函数
	progressCallback := func(complete float64, message string) bool {
		// 检查context是否被取消
		select {
		case <-task.Context.Done():
			return false
		default:
		}

		percentage := int(complete * 100)

		// 通过WebSocket发送进度消息
		progressMsg := ProgressMessage{
			Type:       "progress",
			Percentage: percentage,
			Message:    message,
			Timestamp:  time.Now().UnixMilli(),
		}

		if err := ws.WriteJSON(progressMsg); err != nil {
			fmt.Printf("发送进度消息失败: %v\n", err)
			return false // 返回false可终止操作
		}

		return true
	}

	// 添加计时器
	startTime := time.Now()

	// 使用任务中的参数配置
	req := task.UpdateRequest
	config := &Gogeo.ParallelGeosConfig{
		TileCount:        req.TileCount,
		MaxWorkers:       req.MaxWorkers,
		IsMergeTile:      req.IsMergeTile,
		ProgressCallback: progressCallback,
		PrecisionConfig: &Gogeo.GeometryPrecisionConfig{
			GridSize:      req.GridSize,
			PreserveTopo:  true,
			KeepCollapsed: false,
			Enabled:       true,
		},
	}

	// 在goroutine中执行空间分析，以便能够响应取消操作
	resultChan := make(chan *Gogeo.GeosAnalysisResult, 1)
	errorChan := make(chan error, 1)

	go func() {
		result, err := OSGEO.SpatialUpdateAnalysisParallel(
			req.InputTable,
			req.UpdateTable,
			config,
		)

		if err != nil {
			errorChan <- err
		} else {
			resultChan <- result
		}
	}()

	// 等待结果或取消信号
	select {
	case <-cancelChan:
		// 操作被取消
		task.UpdateStatus(TaskStatusCancelled)
		cancelMsg := ProgressMessage{
			Type:      "cancelled",
			Message:   fmt.Sprintf("任务 %s 已被用户取消", taskID),
			Timestamp: time.Now().UnixMilli(),
		}
		ws.WriteJSON(cancelMsg)
		fmt.Printf("任务 %s 已被取消\n", taskID)
		return

	case <-task.Context.Done():
		// Context被取消
		task.UpdateStatus(TaskStatusCancelled)
		cancelMsg := ProgressMessage{
			Type:      "cancelled",
			Message:   fmt.Sprintf("任务 %s 已被取消", taskID),
			Timestamp: time.Now().UnixMilli(),
		}
		ws.WriteJSON(cancelMsg)
		return

	case err := <-errorChan:
		// 分析过程中出错
		task.UpdateStatus(TaskStatusFailed)
		task.mutex.Lock()
		task.Error = err.Error()
		task.mutex.Unlock()

		errorMsg := ProgressMessage{
			Type:      "error",
			Message:   "空间更新分析失败: " + err.Error(),
			Timestamp: time.Now().UnixMilli(),
		}
		ws.WriteJSON(errorMsg)
		return

	case result := <-resultChan:
		// 分析成功完成
		// 检查是否在最后时刻被取消
		select {
		case <-task.Context.Done():
			task.UpdateStatus(TaskStatusCancelled)
			cancelMsg := ProgressMessage{
				Type:      "cancelled",
				Message:   fmt.Sprintf("任务 %s 已被用户取消", taskID),
				Timestamp: time.Now().UnixMilli(),
			}
			ws.WriteJSON(cancelMsg)
			return
		default:
		}

		// 保存结果到数据库
		DB := models.DB
		OutTable := methods.ConvertToInitials(req.OutTable)

		var count int64
		DB.Model(&models.MySchema{}).Where("en = ? AND cn != ?", OutTable, req.OutTable).Count(&count)
		if count > 0 {
			OutTable = OutTable + "_1"
		}
		err := Gogeo.SaveGDALLayerToPGBatch(DB, result.OutputLayer, OutTable, "", 4326, 1000)
		if err != nil {
			task.UpdateStatus(TaskStatusFailed)
			task.mutex.Lock()
			task.Error = err.Error()
			task.mutex.Unlock()

			errorMsg := ProgressMessage{
				Type:      "error",
				Message:   "保存结果失败: " + err.Error(),
				Timestamp: time.Now().UnixMilli(),
			}
			ws.WriteJSON(errorMsg)
			return
		}
		addLayerSchema(DB, req.InputTable, req.OutTable, OutTable)
		// 保存结果到任务中
		task.mutex.Lock()
		task.Result = result
		task.mutex.Unlock()
		task.UpdateStatus(TaskStatusCompleted)

		// 计算并发送完成消息
		elapsedTime := time.Since(startTime)
		completionMsg := ProgressMessage{
			Type:       "complete",
			Percentage: 100,
			Message:    fmt.Sprintf("空间更新分析完成，耗时: %v，结果已保存到表: %s", elapsedTime, req.OutTable),
			Timestamp:  time.Now().UnixMilli(),
		}
		ws.WriteJSON(completionMsg)
	}
}

// GetUpdateTaskStatus 获取更新任务状态
func (uc *UserController) GetUpdateTaskStatus(c *gin.Context) {
	taskID := c.Param("taskId")

	task, exists := updateTaskManager.GetTask(taskID)
	if !exists {
		c.JSON(404, gin.H{"error": "任务不存在"})
		return
	}

	task.mutex.RLock()
	defer task.mutex.RUnlock()

	response := gin.H{
		"id":         task.ID,
		"status":     task.Status,
		"created_at": task.CreatedAt,
	}

	if task.StartedAt != nil {
		response["started_at"] = *task.StartedAt
	}

	if task.EndedAt != nil {
		response["ended_at"] = *task.EndedAt
		if task.StartedAt != nil {
			duration := task.EndedAt.Sub(*task.StartedAt)
			response["duration"] = duration.String()
		}
	}

	if task.Error != "" {
		response["error"] = task.Error
	}

	if task.Result != nil {
		response["result_count"] = task.Result.ResultCount
	}

	c.JSON(200, response)
}
