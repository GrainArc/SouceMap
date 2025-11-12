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

type TrackHandler struct {
	trackService *services.TrackService
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

	// 验证数据
	if len(trackData.StartPoint) != 2 {
		c.JSON(400, gin.H{"error": "invalid start point"})
		return
	}
	if len(trackData.LayerNames) == 0 {
		c.JSON(400, gin.H{"error": "layer names required"})
		return
	}

	ctx := c.Request.Context()

	// 获取并打断几何为线段，返回 GeoJSON
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
		linesGeoJSON: linesGeoJSON,
		startPoint:   trackData.StartPoint,
		ctx:          sessionCtx,
		cancel:       cancel,
		completePath: geojson.NewFeatureCollection(),
	}

	// 发送初始化成功消息，包含打断后的线段
	initResponse := models.TrackResponse{
		Type:    "init",
		Lines:   linesGeoJSON,
		Message: fmt.Sprintf("Tracking initialized with %d line segments", len(linesGeoJSON.Features)),
	}
	if err := conn.WriteJSON(initResponse); err != nil {
		log.Printf("Failed to send init response: %v", err)
		conn.Close()
		return
	}

	// 处理 WebSocket 会话
	h.handleSession(session)
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

		// 处理完成命令
		if action, ok := msg["action"].(string); ok && action == "complete" {
			h.handleComplete(session)
			return
		}

		// 处理鼠标点
		if pointData, ok := msg["point"].([]interface{}); ok && len(pointData) == 2 {
			x, xOk := pointData[0].(float64)
			y, yOk := pointData[1].(float64)
			if !xOk || !yOk {
				continue
			}
			currentPoint := []float64{x, y}
			h.handleMouseMove(session, currentPoint)
		}
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
	session.mu.Unlock()

	if err != nil {
		log.Printf("Failed to send path update: %v", err)
		session.cancel()
	}
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
