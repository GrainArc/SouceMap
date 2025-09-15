package GdalView

import (
	"context"
	"fmt"
	"github.com/fmecool/Gogeo"
	"github.com/fmecool/SouceMap/OSGEO"
	"github.com/fmecool/SouceMap/methods"
	"github.com/fmecool/SouceMap/models"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"net/http"
	"sync"
	"time"
)

// Union请求参数结构体
type UnionRequest struct {
	TableName        string   `json:"table_name" binding:"required"`
	GroupFields      []string `json:"group_fields" binding:"required"`
	OutTable         string   `json:"out_table" binding:"required"`
	GridSize         float64  `json:"gridSize"`
	PreserveTopo     bool     `json:"preserveTopo"`
	KeepCollapsed    bool     `json:"keepCollapsed"`
	PrecisionEnabled bool     `json:"precisionEnabled"`
}

// Union任务信息结构体
type UnionTaskInfo struct {
	ID        string                    `json:"id"`
	Status    TaskStatus                `json:"status"`
	Request   UnionRequest              `json:"union_request"`
	CreatedAt time.Time                 `json:"created_at"`
	StartedAt *time.Time                `json:"started_at,omitempty"`
	EndedAt   *time.Time                `json:"ended_at,omitempty"`
	Error     string                    `json:"error,omitempty"`
	Result    *Gogeo.GeosAnalysisResult `json:"-"` // 不序列化到JSON
	Context   context.Context           `json:"-"`
	Cancel    context.CancelFunc        `json:"-"`
	mutex     sync.RWMutex              `json:"-"`
}

// Union任务管理器
type UnionTaskManager struct {
	tasks map[string]*UnionTaskInfo
	mutex sync.RWMutex
}

var unionTaskManager = &UnionTaskManager{
	tasks: make(map[string]*UnionTaskInfo),
}

// 添加Union任务
func (utm *UnionTaskManager) AddTask(task *UnionTaskInfo) {
	utm.mutex.Lock()
	defer utm.mutex.Unlock()
	utm.tasks[task.ID] = task
}

// 获取Union任务
func (utm *UnionTaskManager) GetTask(taskID string) (*UnionTaskInfo, bool) {
	utm.mutex.RLock()
	defer utm.mutex.RUnlock()
	task, exists := utm.tasks[taskID]
	return task, exists
}

// 删除Union任务
func (utm *UnionTaskManager) RemoveTask(taskID string) {
	utm.mutex.Lock()
	defer utm.mutex.Unlock()
	if task, exists := utm.tasks[taskID]; exists {
		if task.Cancel != nil {
			task.Cancel()
		}
		delete(utm.tasks, taskID)
	}
}

// 更新Union任务状态
func (task *UnionTaskInfo) UpdateStatus(status TaskStatus) {
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

// Union参数验证函数
func validateUnionParams(req *UnionRequest) error {
	if req.TableName == "" {
		return fmt.Errorf("table_name不能为空")
	}
	if len(req.GroupFields) == 0 {
		return fmt.Errorf("group_fields不能为空")
	}
	if req.OutTable == "" {
		return fmt.Errorf("out_table不能为空")
	}
	if req.GridSize < 0 {
		req.GridSize = 0.00000001 // 默认值：浮点精度
	}
	// 设置默认值
	if !req.PrecisionEnabled {
		req.PreserveTopo = true
		req.KeepCollapsed = false
	}
	return nil
}

// StartUnion 创建并初始化Union分析任务
func (uc *UserController) StartUnion(c *gin.Context) {
	// 解析请求参数
	var req UnionRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "参数错误: " + err.Error()})
		return
	}

	if err := validateUnionParams(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	// 创建任务
	taskID := uuid.New().String()
	ctx, cancel := context.WithCancel(context.Background())

	task := &UnionTaskInfo{
		ID:        taskID,
		Status:    TaskStatusPending,
		Request:   req,
		CreatedAt: time.Now(),
		Context:   ctx,
		Cancel:    cancel,
	}

	// 添加到任务管理器
	unionTaskManager.AddTask(task)

	// 返回任务ID
	c.JSON(200, gin.H{
		"task_id": taskID,
		"status":  task.Status,
		"message": "Union任务已创建，请使用WebSocket连接开始执行",
		"ws_url":  fmt.Sprintf("/gdal/union/ws/%s", taskID),
	})
}

// UnionWebSocket 处理WebSocket连接并执行Union分析任务
func (uc *UserController) UnionWebSocket(c *gin.Context) {
	taskID := c.Param("taskId")

	// 获取任务信息
	task, exists := unionTaskManager.GetTask(taskID)
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
				fmt.Printf("收到Union任务 %s 的取消请求\n", taskID)
				cancelChan <- true
				task.Cancel() // 取消context
				return
			}
		}
	}()
	progressCallback := func(complete float64, message string) bool {

		// 检查context是否被取消
		select {
		case <-task.Context.Done():
			fmt.Printf("Union任务被取消，停止进度报告\n")
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
			return false
		}

		fmt.Printf("进度消息已发送: %d%% - %s\n", percentage, message)
		return true
	}

	// 添加计时器
	startTime := time.Now()

	// 使用任务中的参数配置
	req := task.Request
	precisionConfig := &Gogeo.GeometryPrecisionConfig{
		GridSize:      req.GridSize,
		PreserveTopo:  req.PreserveTopo,
		KeepCollapsed: req.KeepCollapsed,
		Enabled:       req.PrecisionEnabled,
	}

	// 在goroutine中执行Union分析，以便能够响应取消操作
	resultChan := make(chan *Gogeo.GeosAnalysisResult, 1)
	errorChan := make(chan error, 1)

	go func() {
		result, err := OSGEO.SpatialUnionAnalysis(
			req.TableName,
			req.GroupFields,
			req.OutTable,
			precisionConfig,
			progressCallback,
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
			Message:   fmt.Sprintf("Union任务 %s 已被用户取消", taskID),
			Timestamp: time.Now().UnixMilli(),
		}
		ws.WriteJSON(cancelMsg)
		fmt.Printf("Union任务 %s 已被取消\n", taskID)
		return

	case <-task.Context.Done():
		// Context被取消
		task.UpdateStatus(TaskStatusCancelled)
		cancelMsg := ProgressMessage{
			Type:      "cancelled",
			Message:   fmt.Sprintf("Union任务 %s 已被取消", taskID),
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
			Message:   "Union分析失败: " + err.Error(),
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
				Message:   fmt.Sprintf("Union任务 %s 已被用户取消", taskID),
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
				Message:   "保存Union结果失败: " + err.Error(),
				Timestamp: time.Now().UnixMilli(),
			}
			ws.WriteJSON(errorMsg)
			return
		}
		addLayerSchema(DB, req.TableName, req.OutTable, OutTable)
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
			Message:    fmt.Sprintf("Union分析完成，耗时: %v，结果已保存到表: %s", elapsedTime, req.OutTable),
			Timestamp:  time.Now().UnixMilli(),
		}
		ws.WriteJSON(completionMsg)
	}
}

// GetUnionTaskStatus 查询Union任务状态
func (uc *UserController) GetUnionTaskStatus(c *gin.Context) {
	taskID := c.Param("taskId")

	task, exists := unionTaskManager.GetTask(taskID)
	if !exists {
		c.JSON(404, gin.H{"error": "任务不存在"})
		return
	}

	task.mutex.RLock()
	defer task.mutex.RUnlock()

	response := gin.H{
		"task_id":    task.ID,
		"status":     task.Status,
		"created_at": task.CreatedAt,
		"started_at": task.StartedAt,
		"ended_at":   task.EndedAt,
	}

	if task.Error != "" {
		response["error"] = task.Error
	}

	c.JSON(200, response)
}
