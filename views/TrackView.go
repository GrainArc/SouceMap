package views

import (
	"context"

	"fmt"
	"github.com/GrainArc/SouceMap/models"
	"github.com/GrainArc/SouceMap/services"
	"github.com/paulmach/orb/geojson"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// 图层要素追踪

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // 生产环境需要严格检查
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func NewTrackHandler() *TrackHandler {
	return &TrackHandler{
		trackService: services.NewTrackService(),
	}
}

// TrackSession 追踪会话
type TrackSession struct {
	conn         *websocket.Conn
	linesGeoJSON *geojson.FeatureCollection // 打断后的线段
	startPoint   []float64
	mu           sync.RWMutex
	ctx          context.Context
	cancel       context.CancelFunc
	completePath *geojson.FeatureCollection // 完整路径
}

// InitTrack 初始化追踪并升级到 WebSocket
func (h *TrackHandler) InitTrack(c *gin.Context) {
	var trackData models.TrackData
	if err := c.ShouldBindJSON(&trackData); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	if len(trackData.LayerNames) == 0 {
		c.JSON(400, gin.H{"error": "layer names required"})
		return
	}

	ctx := c.Request.Context()

	// 获取并打断几何为线段
	log.Printf("Getting geometries from layers: %v", trackData.LayerNames)
	linesGeoJSON, err := h.trackService.GetAndBreakGeometries(
		ctx,
		trackData.LayerNames,
		trackData.Box,
	)
	if err != nil {
		c.JSON(500, gin.H{"error": fmt.Sprintf("failed to get geometries: %v", err)})
		return
	}

	if linesGeoJSON == nil || len(linesGeoJSON.Features) == 0 {
		c.JSON(404, gin.H{"error": "no geometries found in the specified area"})
		return
	}

	log.Printf("Found %d line segments", len(linesGeoJSON.Features))

	// 生成会话ID
	sessionID := fmt.Sprintf("%d", time.Now().UnixNano())

	// 存储会话数据(可以用 Redis 或内存缓存)
	h.storePendingSession(sessionID, &PendingSession{
		LinesGeoJSON: linesGeoJSON,
		StartPoint:   trackData.StartPoint,
		CreatedAt:    time.Now(),
	})

	// 返回会话ID和线段数据
	c.JSON(200, gin.H{
		"session_id": sessionID,
		"message":    fmt.Sprintf("Tracking initialized with %d line segments", len(linesGeoJSON.Features)),
	})
}

// ConnectWebSocket 连接 WebSocket(GET 请求)
func (h *TrackHandler) ConnectWebSocket(c *gin.Context) {
	sessionID := c.Query("session_id")
	if sessionID == "" {
		c.JSON(400, gin.H{"error": "session_id required"})
		return
	}

	// 获取会话数据
	pending := h.getPendingSession(sessionID)
	if pending == nil {
		c.JSON(404, gin.H{"error": "session not found or expired"})
		return
	}

	// 升级到 WebSocket
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("Failed to upgrade to websocket: %v", err)
		return
	}

	// 创建会话
	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &TrackSession{
		conn:         conn,
		linesGeoJSON: pending.LinesGeoJSON,
		startPoint:   pending.StartPoint,
		ctx:          sessionCtx,
		cancel:       cancel,
		completePath: geojson.NewFeatureCollection(),
	}

	// 清理待处理会话
	h.removePendingSession(sessionID)

	// 发送连接成功消息
	initResponse := models.TrackResponse{
		Type:    "connected",
		Message: "WebSocket connected successfully",
	}
	if err := conn.WriteJSON(initResponse); err != nil {
		log.Printf("Failed to send init response: %v", err)
		conn.Close()
		return
	}

	// 处理 WebSocket 会话
	h.handleSession(session)
}

// 添加会话管理
type PendingSession struct {
	LinesGeoJSON *geojson.FeatureCollection
	StartPoint   []float64
	CreatedAt    time.Time
}
type TrackHandler struct {
	trackService    *services.TrackService
	pendingSessions sync.Map // sessionID -> *PendingSession
}

func (h *TrackHandler) storePendingSession(id string, session *PendingSession) {
	h.pendingSessions.Store(id, session)

	// 设置过期清理(5分钟)
	go func() {
		time.Sleep(5 * time.Minute)
		h.pendingSessions.Delete(id)
	}()
}

func (h *TrackHandler) getPendingSession(id string) *PendingSession {
	if val, ok := h.pendingSessions.Load(id); ok {
		return val.(*PendingSession)
	}
	return nil
}

func (h *TrackHandler) removePendingSession(id string) {
	h.pendingSessions.Delete(id)
}
func (h *TrackHandler) handleSession(session *TrackSession) {
	defer func() {
		session.cancel()
		session.conn.Close()
		log.Println("WebSocket session closed")
	}()

	// 设置心跳
	pingTicker := time.NewTicker(30 * time.Second)
	defer pingTicker.Stop()

	// 心跳 goroutine
	go func() {
		for {
			select {
			case <-session.ctx.Done():
				return
			case <-pingTicker.C:
				session.mu.Lock()
				err := session.conn.WriteMessage(websocket.PingMessage, nil)
				session.mu.Unlock()
				if err != nil {
					log.Printf("Ping failed: %v", err)
					session.cancel()
					return
				}
			}
		}
	}()

	// 处理消息
	for {
		select {
		case <-session.ctx.Done():
			return
		default:
		}

		var msg map[string]interface{}
		err := session.conn.ReadJSON(&msg)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			return
		}

		// 获取 action 和 point
		action, hasAction := msg["action"].(string)
		pointData, hasPoint := msg["point"].([]interface{})

		// 验证 point 数据
		var point []float64
		if hasPoint && len(pointData) == 2 {
			x, xOk := pointData[0].(float64)
			y, yOk := pointData[1].(float64)
			if xOk && yOk {
				point = []float64{x, y}
			}
		}

		// 处理不同的 action
		if hasAction {
			switch action {
			case "complete":
				// 处理完成命令
				h.handleComplete(session)
				return

			case "snap":
				// 处理捕捉点请求
				if len(point) == 2 {
					// 获取 max_distance 参数
					var maxDistance float64 = 0
					if maxDist, ok := msg["max_distance"].(float64); ok {
						maxDistance = maxDist
					}
					h.handleSnapPointWithDistance(session, point, maxDistance)
				} else {
					log.Printf("Invalid point for snap action")
				}

			case "track":
				// 处理追踪路径请求
				if len(point) == 2 {
					h.handleMouseMove(session, point)
				} else {
					log.Printf("Invalid point for track action")
				}

			default:
				log.Printf("Unknown action: %s", action)
			}
		} else {
			// 兼容旧版本：没有 action 字段时，默认为追踪
			if len(point) == 2 {
				h.handleMouseMove(session, point)
			}
		}
	}
}

// 添加新的处理函数
func (h *TrackHandler) handleSnapPoint(session *TrackSession, point []float64) {
	// 从消息中获取 max_distance，如果没有则默认为 0(不限制)
	var maxDistance float64 = 2
	snappedPoint, distance, lineID, err := h.trackService.FindNearestPointOnLines(
		session.ctx,
		session.linesGeoJSON,
		point,
		maxDistance, // 传入阈值参数
	)

	var response models.SnapPointResponse
	if err != nil {
		response = models.SnapPointResponse{
			Type:    "snap_point",
			Message: err.Error(),
		}
		log.Printf("Snap point error: %v", err)
	} else {
		response = models.SnapPointResponse{
			Type:         "snap_point",
			SnappedPoint: snappedPoint,
			Distance:     distance,
			LineID:       lineID,
		}
	}

	// 发送捕捉结果
	session.mu.Lock()
	err = session.conn.WriteJSON(response)
	session.mu.Unlock()

	if err != nil {
		log.Printf("Failed to send snap point response: %v", err)
		session.cancel()
	}
}

func (h *TrackHandler) handleMouseMove(session *TrackSession, currentPoint []float64) {
	// 计算最短路径，返回 GeoJSON

	pathFC, err := h.trackService.CalculateShortestPath(
		session.ctx,
		session.linesGeoJSON,
		session.startPoint,
		currentPoint,
	)
	if err != nil {
		log.Printf("Path calculation error: %v", err)
		return
	}

	response := models.TrackResponse{
		Type: "path",
		Path: pathFC,
	}

	// 发送路径更新
	session.mu.Lock()
	err = session.conn.WriteJSON(response)
	if err != nil {
		session.mu.Unlock()
		log.Printf("Failed to send path update: %v", err)
		session.cancel()
		return
	}

	// 更新起始点为当前结束点
	session.startPoint = currentPoint
	session.mu.Unlock()
}

func (h *TrackHandler) handleComplete(session *TrackSession) {
	// 返回完整路径
	session.mu.RLock()
	finalPath := session.completePath
	session.mu.RUnlock()

	response := models.TrackResponse{
		Type:    "complete",
		Path:    finalPath,
		Message: fmt.Sprintf("Tracking completed with %d features", len(finalPath.Features)),
	}

	session.mu.Lock()
	session.conn.WriteJSON(response)
	session.mu.Unlock()

	log.Printf("Tracking completed")
}
func (h *TrackHandler) handleSnapPointWithDistance(session *TrackSession, point []float64, maxDistance float64) {
	snappedPoint, distance, lineID, err := h.trackService.FindNearestPointOnLines(
		session.ctx,
		session.linesGeoJSON,
		point,
		maxDistance,
	)

	var response models.SnapPointResponse
	if err != nil {
		response = models.SnapPointResponse{
			Type:    "snap_point",
			Message: err.Error(),
		}
		log.Printf("Snap point error: %v", err)
	} else {
		response = models.SnapPointResponse{
			Type:         "snap_point",
			SnappedPoint: snappedPoint,
			Distance:     distance,
			LineID:       lineID,
		}
	}

	// 发送捕捉结果
	session.mu.Lock()
	err = session.conn.WriteJSON(response)
	session.mu.Unlock()

	if err != nil {
		log.Printf("Failed to send snap point response: %v", err)
		session.cancel()
	}
}
