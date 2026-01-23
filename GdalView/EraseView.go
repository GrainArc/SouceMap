package GdalView

import (
	"context"
	"fmt"
	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"net/http"
	"sync"
	"time"
)

// 擦除分析请求参数结构体
type EraseRequest struct {
	InputTable  string  `json:"table1" binding:"required"`
	EraseTable  string  `json:"table2" binding:"required"`
	OutTable    string  `json:"out_table" binding:"required"`
	IsMergeTile bool    `json:"isMergeTile"` // 是否合并瓦片
	MaxWorkers  int     `json:"maxWorkers"`
	GridSize    float64 `json:"gridSize"`
	TileCount   int     `json:"tileCount"`
}

type EraseTaskInfo struct {
	ID           string                    `json:"id"`
	Status       TaskStatus                `json:"status"`
	EraseRequest EraseRequest              `json:"erase_request"`
	CreatedAt    time.Time                 `json:"created_at"`
	StartedAt    *time.Time                `json:"started_at,omitempty"`
	EndedAt      *time.Time                `json:"ended_at,omitempty"`
	Error        string                    `json:"error,omitempty"`
	Result       *Gogeo.GeosAnalysisResult `json:"-"` // 不序列化到JSON
	Context      context.Context           `json:"-"`
	Cancel       context.CancelFunc        `json:"-"`
	mutex        sync.RWMutex              `json:"-"`
}

// 参数验证函数
func validateEraseParams(req *EraseRequest) error {
	if req.InputTable == "" {
		return fmt.Errorf("input_table不能为空")
	}
	if req.EraseTable == "" {
		return fmt.Errorf("erase_table不能为空")
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

// 擦除任务管理器
type EraseTaskManager struct {
	tasks map[string]*EraseTaskInfo
	mutex sync.RWMutex
}

var eraseTaskManager = &EraseTaskManager{
	tasks: make(map[string]*EraseTaskInfo),
}

// 添加擦除任务
func (etm *EraseTaskManager) AddTask(task *EraseTaskInfo) {
	etm.mutex.Lock()
	defer etm.mutex.Unlock()
	etm.tasks[task.ID] = task
}

// 获取擦除任务
func (etm *EraseTaskManager) GetTask(taskID string) (*EraseTaskInfo, bool) {
	etm.mutex.RLock()
	defer etm.mutex.RUnlock()
	task, exists := etm.tasks[taskID]
	return task, exists
}

// 删除擦除任务
func (etm *EraseTaskManager) RemoveTask(taskID string) {
	etm.mutex.Lock()
	defer etm.mutex.Unlock()
	if task, exists := etm.tasks[taskID]; exists {
		if task.Cancel != nil {
			task.Cancel()
		}
		delete(etm.tasks, taskID)
	}
}

// 更新擦除任务状态
func (task *EraseTaskInfo) UpdateStatus(status TaskStatus) {
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

// StartErase 创建并初始化擦除分析任务
func (uc *UserController) StartErase(c *gin.Context) {
	// 解析请求参数
	var req EraseRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "参数错误: " + err.Error()})
		return
	}

	if err := validateEraseParams(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	// 创建任务
	taskID := uuid.New().String()
	ctx, cancel := context.WithCancel(context.Background())

	task := &EraseTaskInfo{
		ID:           taskID,
		Status:       TaskStatusPending,
		EraseRequest: req,
		CreatedAt:    time.Now(),
		Context:      ctx,
		Cancel:       cancel,
	}

	// 添加到任务管理器
	eraseTaskManager.AddTask(task)

	// 返回任务ID
	c.JSON(200, gin.H{
		"task_id": taskID,
		"status":  task.Status,
		"message": "擦除分析任务已创建，请使用WebSocket连接开始执行",
		"ws_url":  fmt.Sprintf("/gdal/Erase/ws/%s", taskID),
	})
}

// EraseWebSocket 处理WebSocket连接并执行擦除分析任务
func (uc *UserController) EraseWebSocket(c *gin.Context) {
	taskID := c.Param("taskId")

	// 获取任务信息
	task, exists := eraseTaskManager.GetTask(taskID)
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
	req := task.EraseRequest
	config := &Gogeo.ParallelGeosConfig{
		TileCount:        req.TileCount,
		MaxWorkers:       req.MaxWorkers,
		ProgressCallback: progressCallback,
		IsMergeTile:      req.IsMergeTile,
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
		result, err := Gogeo.SpatialEraseAnalysisParallelPG(models.DB,
			req.InputTable,
			req.EraseTable,
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
			Message:   "擦除分析失败: " + err.Error(),
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
		// 检查重名
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
			Message:    fmt.Sprintf("擦除分析完成，耗时: %v，结果已保存到表: %s", elapsedTime, req.OutTable),
			Timestamp:  time.Now().UnixMilli(),
		}
		ws.WriteJSON(completionMsg)
	}
}

// GetEraseTaskStatus 获取擦除任务状态
func (uc *UserController) GetEraseTaskStatus(c *gin.Context) {
	taskID := c.Param("taskId")

	task, exists := eraseTaskManager.GetTask(taskID)
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
		response["started_at"] = task.StartedAt
	}
	if task.EndedAt != nil {
		response["ended_at"] = task.EndedAt
	}
	if task.Error != "" {
		response["error"] = task.Error
	}

	c.JSON(200, response)
}
