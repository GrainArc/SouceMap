// webtile_downloader.go
package tile_proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GrainArc/Gogeo"
	"github.com/GrainArc/SouceMap/models"
	"github.com/GrainArc/SouceMap/pgmvt"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"gorm.io/gorm"
)

// ========== 请求/响应结构体 ==========

// DownloadRequest 下载请求参数
type DownloadRequest struct {
	MapName   string          `json:"mapName" binding:"required"`
	ZoomLevel int             `json:"zoomLevel" binding:"required"`
	GeoJSON   json.RawMessage `json:"geoJson" binding:"required"`
}

// DownloadTask 下载任务
type DownloadTask struct {
	ID              string     `json:"id"`
	MapName         string     `json:"mapName"`
	ZoomLevel       int        `json:"zoomLevel"`
	Status          string     `json:"status"` // pending, running, completed, failed
	Progress        float64    `json:"progress"`
	TotalTiles      int        `json:"totalTiles"`
	DownloadedTiles int        `json:"downloadedTiles"`
	FailedTiles     int        `json:"failedTiles"`
	Message         string     `json:"message"`
	OutputFile      string     `json:"outputFile"`
	CreatedAt       time.Time  `json:"createdAt"`
	StartedAt       *time.Time `json:"startedAt"`
	CompletedAt     *time.Time `json:"completedAt"`
}

// ProgressMessage WebSocket进度消息
type ProgressMessage struct {
	Type     string      `json:"type"` // connected, progress, completed, error
	TaskID   string      `json:"taskId,omitempty"`
	Progress float64     `json:"progress,omitempty"`
	Message  string      `json:"message,omitempty"`
	Data     interface{} `json:"data,omitempty"`
}

// PendingTask 待处理任务（等待WebSocket连接）
type PendingTask struct {
	Task      *DownloadTask
	NetMap    *models.NetMap
	Tiles     []TileIndex
	BBox      *BoundingBox
	ZoomLevel int
	CreatedAt time.Time
}

// DownloadSession 下载会话
type DownloadSession struct {
	conn      *websocket.Conn
	task      *DownloadTask
	netMap    *models.NetMap
	tiles     []TileIndex
	bbox      *BoundingBox
	zoomLevel int
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
}

// ========== GeoJSON 结构体 ==========

type GeoJSONGeometry struct {
	Type        string          `json:"type"`
	Coordinates json.RawMessage `json:"coordinates"`
}

type GeoJSONFeature struct {
	Type       string          `json:"type"`
	Geometry   GeoJSONGeometry `json:"geometry"`
	Properties interface{}     `json:"properties"`
}

type GeoJSONFeatureCollection struct {
	Type     string           `json:"type"`
	Features []GeoJSONFeature `json:"features"`
}

// ========== 辅助结构体 ==========

type BoundingBox struct {
	MinLon float64
	MinLat float64
	MaxLon float64
	MaxLat float64
}

type TileIndex struct {
	Z int
	X int
	Y int
}

// ========== WebTileDownloader ==========

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // 生产环境需要严格检查
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// WebTileDownloader 网络瓦片下载器
type WebTileDownloader struct {
	db             *gorm.DB
	httpClient     *http.Client
	coordConv      *pgmvt.ChangeCoord
	cache          *TileCache
	safeProcessor  *SafeTileProcessor
	pendingTasks   sync.Map // taskID -> *PendingTask
	completedTasks sync.Map // taskID -> *DownloadTask (保存已完成的任务)
	outputDir      string
}

// NewWebTileDownloader 创建下载器
func NewWebTileDownloader(outputDir string) *WebTileDownloader {
	if outputDir == "" {
		outputDir = "./downloads"
	}
	os.MkdirAll(outputDir, 0755)

	return &WebTileDownloader{
		db: models.DB,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		coordConv:     pgmvt.NewChangeCoord(),
		cache:         NewTileCache(20000, 300*time.Hour),
		safeProcessor: NewSafeTileProcessor(4),
		outputDir:     outputDir,
	}
}

// InitDownload 初始化下载任务
func (d *WebTileDownloader) InitDownload(c *gin.Context) {
	var req DownloadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":  400,
			"error": fmt.Sprintf("invalid request: %v", err),
		})
		return
	}

	// 验证层级
	if req.ZoomLevel < 0 || req.ZoomLevel > 20 {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":  400,
			"error": "zoom level must be between 0 and 20",
		})
		return
	}

	// 查找地图配置
	var netMap models.NetMap
	if err := d.db.Where("map_name = ? AND status = 1", req.MapName).First(&netMap).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{
			"code":  404,
			"error": "map not found or disabled",
		})
		return
	}

	// 解析GeoJSON获取边界框
	bbox, err := d.parseGeoJSONToBBox(req.GeoJSON)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":  400,
			"error": fmt.Sprintf("invalid geojson: %v", err),
		})
		return
	}

	// 计算需要下载的瓦片
	tiles := d.calculateTilesInBBox(bbox, req.ZoomLevel)
	if len(tiles) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":  400,
			"error": "no tiles in the specified area",
		})
		return
	}

	// 限制最大瓦片数量
	maxTiles := 10000
	if len(tiles) > maxTiles {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":  400,
			"error": fmt.Sprintf("too many tiles: %d, max allowed: %d", len(tiles), maxTiles),
		})
		return
	}

	// 创建任务
	taskID := fmt.Sprintf("task_%d_%d", time.Now().UnixNano(), netMap.ID)
	task := &DownloadTask{
		ID:         taskID,
		MapName:    req.MapName,
		ZoomLevel:  req.ZoomLevel,
		Status:     "pending",
		TotalTiles: len(tiles),
		Message:    "waiting for websocket connection",
		CreatedAt:  time.Now(),
	}

	// 存储待处理任务
	d.storePendingTask(taskID, &PendingTask{
		Task:      task,
		NetMap:    &netMap,
		Tiles:     tiles,
		BBox:      bbox,
		ZoomLevel: req.ZoomLevel,
		CreatedAt: time.Now(),
	})

	log.Printf("Download task created: %s with %d tiles", taskID, len(tiles))

	// 返回任务ID
	c.JSON(http.StatusOK, gin.H{
		"code":       200,
		"taskId":     taskID,
		"totalTiles": len(tiles),
		"message":    "download task created, connect to websocket for progress",
	})
}

// ConnectWebSocket WebSocket连接处理
func (d *WebTileDownloader) ConnectWebSocket(c *gin.Context) {
	taskID := c.Query("taskId")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":  400,
			"error": "taskId is required",
		})
		return
	}

	// 获取待处理任务
	pending := d.getPendingTask(taskID)
	if pending == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"code":  404,
			"error": "task not found or expired",
		})
		return
	}

	// 升级到 WebSocket
	conn, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("Failed to upgrade to websocket: %v", err)
		return
	}

	// 创建会话
	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &DownloadSession{
		conn:      conn,
		task:      pending.Task,
		netMap:    pending.NetMap,
		tiles:     pending.Tiles,
		bbox:      pending.BBox,
		zoomLevel: pending.ZoomLevel,
		ctx:       sessionCtx,
		cancel:    cancel,
	}

	// 清理待处理任务
	d.removePendingTask(taskID)

	// 发送连接成功消息
	initResponse := ProgressMessage{
		Type:    "connected",
		TaskID:  taskID,
		Message: "WebSocket connected successfully, starting download...",
		Data: map[string]interface{}{
			"totalTiles": pending.Task.TotalTiles,
			"status":     "connected",
		},
	}
	if err := conn.WriteJSON(initResponse); err != nil {
		log.Printf("Failed to send init response: %v", err)
		conn.Close()
		return
	}

	log.Printf("WebSocket connected for task: %s", taskID)

	// 处理下载会话
	d.handleSession(session)
}

// ========== 会话管理 ==========

func (d *WebTileDownloader) storePendingTask(id string, task *PendingTask) {
	d.pendingTasks.Store(id, task)

	// 设置过期清理(5分钟)
	go func() {
		time.Sleep(5 * time.Minute)
		d.pendingTasks.Delete(id)
		log.Printf("Pending task expired: %s", id)
	}()
}

func (d *WebTileDownloader) getPendingTask(id string) *PendingTask {
	if val, ok := d.pendingTasks.Load(id); ok {
		return val.(*PendingTask)
	}
	return nil
}

func (d *WebTileDownloader) removePendingTask(id string) {
	d.pendingTasks.Delete(id)
}

func (d *WebTileDownloader) storeCompletedTask(task *DownloadTask) {
	d.completedTasks.Store(task.ID, task)

	// 设置过期清理(1小时)
	go func() {
		time.Sleep(1 * time.Hour)
		d.completedTasks.Delete(task.ID)
		log.Printf("Completed task expired: %s", task.ID)
	}()
}

// GetTask 获取任务状态
func (d *WebTileDownloader) GetTask(taskID string) (*DownloadTask, bool) {
	// 先查找已完成的任务
	if val, ok := d.completedTasks.Load(taskID); ok {
		return val.(*DownloadTask), true
	}
	// 再查找待处理的任务
	if val, ok := d.pendingTasks.Load(taskID); ok {
		return val.(*PendingTask).Task, true
	}
	return nil, false
}

// ========== 会话处理 ==========

func (d *WebTileDownloader) handleSession(session *DownloadSession) {
	defer func() {
		session.cancel()
		session.conn.Close()
		log.Printf("Download session closed for task: %s", session.task.ID)
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
					log.Printf("Ping failed for task %s: %v", session.task.ID, err)
					session.cancel()
					return
				}
			}
		}
	}()

	// 启动下载任务
	go d.executeDownload(session)

	// 处理客户端消息（主要是接收取消命令）
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
				log.Printf("WebSocket error for task %s: %v", session.task.ID, err)
			}
			return
		}

		// 处理客户端命令
		if action, ok := msg["action"].(string); ok {
			switch action {
			case "cancel":
				log.Printf("Download cancelled by client for task: %s", session.task.ID)
				session.cancel()
				d.sendMessage(session, ProgressMessage{
					Type:    "cancelled",
					TaskID:  session.task.ID,
					Message: "Download cancelled by user",
				})
				return
			}
		}
	}
}

type WebMercatorBounds struct {
	MinX float64
	MinY float64
	MaxX float64
	MaxY float64
}

// GetTileBoundsWebMercator 获取瓦片的 Web Mercator 边界
func GetTileBoundsWebMercator(z, x, y int) WebMercatorBounds {
	n := math.Pow(2, float64(z))

	// Web Mercator 范围
	const earthRadius = 6378137.0
	const maxExtent = math.Pi * earthRadius // 20037508.342789244

	tileSize := (2 * maxExtent) / n

	minX := -maxExtent + float64(x)*tileSize
	maxX := minX + tileSize

	// Y轴在Web Mercator中是反向的（TMS标准）
	maxY := maxExtent - float64(y)*tileSize
	minY := maxY - tileSize

	return WebMercatorBounds{
		MinX: minX,
		MinY: minY,
		MaxX: maxX,
		MaxY: maxY,
	}
}

// executeDownload 执行下载任务
func (d *WebTileDownloader) executeDownload(session *DownloadSession) {
	task := session.task
	netMap := session.netMap
	tiles := session.tiles
	bbox := session.bbox
	zoom := session.zoomLevel

	// 更新任务状态
	now := time.Now()
	task.Status = "running"
	task.StartedAt = &now
	task.Message = "downloading tiles..."

	d.sendMessage(session, ProgressMessage{
		Type:     "progress",
		TaskID:   task.ID,
		Progress: 0,
		Message:  "starting download...",
		Data: map[string]interface{}{
			"status":          task.Status,
			"totalTiles":      task.TotalTiles,
			"downloadedTiles": 0,
			"failedTiles":     0,
		},
	})

	// 获取瓦片大小
	tileSize := 256
	if netMap.TileSize > 0 {
		tileSize = netMap.TileSize
	}

	// 计算输出图像尺寸
	minTile := LonLatToTileCoord(bbox.MinLon, bbox.MaxLat, zoom)
	maxTile := LonLatToTileCoord(bbox.MaxLon, bbox.MinLat, zoom)

	outputWidth := (maxTile.X - minTile.X + 1) * tileSize
	outputHeight := (maxTile.Y - minTile.Y + 1) * tileSize

	// 计算GeoTransform参数
	topLeftBounds := GetTileBoundsWGS84(zoom, minTile.X, minTile.Y)
	bottomRightBounds := GetTileBoundsWGS84(zoom, maxTile.X, maxTile.Y)

	pixelWidth := (bottomRightBounds.MaxLon - topLeftBounds.MinLon) / float64(outputWidth)
	pixelHeight := (topLeftBounds.MaxLat - bottomRightBounds.MinLat) / float64(outputHeight)

	geoTransform := [6]float64{
		topLeftBounds.MinLon,
		pixelWidth,
		0,
		topLeftBounds.MaxLat,
		0,
		-pixelHeight,
	}

	// 创建GeoTiff写入器
	writer, err := Gogeo.NewGeoTiffWriter(outputWidth, outputHeight, 4, tileSize, geoTransform)
	if err != nil {
		d.failSession(session, fmt.Sprintf("failed to create GeoTiff writer: %v", err))
		return
	}
	defer writer.Close()

	// 判断是否需要坐标转换
	needTransform := netMap.Projection == CoordGCJ02 || netMap.Projection == CoordBD09

	// 并发下载瓦片
	var downloadedCount int64
	var failedCount int64
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 8)

	tileMutex := sync.Mutex{}
	downloadedTiles := make(map[string][]byte)

	for _, tile := range tiles {
		// 检查是否取消
		select {
		case <-session.ctx.Done():
			log.Printf("Download cancelled for task: %s", task.ID)
			return
		default:
		}

		wg.Add(1)
		go func(t TileIndex) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// 再次检查取消
			select {
			case <-session.ctx.Done():
				return
			default:
			}

			var tileData []byte
			var err error

			if needTransform {
				var transformFunc CoordTransformFunc
				if netMap.Projection == CoordGCJ02 {
					transformFunc = d.wgs84ToGCJ02
				} else {
					transformFunc = d.wgs84ToBD09
				}
				tileData, err = d.downloadTileWithTransform(session.ctx, netMap, t, tileSize, transformFunc)
			} else {
				tileData, err = d.downloadTileDirect(session.ctx, netMap, t)
			}

			if err != nil {
				atomic.AddInt64(&failedCount, 1)
				log.Printf("Failed to download tile %d/%d/%d: %v", t.Z, t.X, t.Y, err)
			} else {
				tileMutex.Lock()
				downloadedTiles[fmt.Sprintf("%d_%d", t.X, t.Y)] = tileData
				tileMutex.Unlock()
			}

			// 更新进度
			downloaded := atomic.AddInt64(&downloadedCount, 1)
			failed := atomic.LoadInt64(&failedCount)
			progress := float64(downloaded) / float64(len(tiles)) * 100

			task.DownloadedTiles = int(downloaded) - int(failed)
			task.FailedTiles = int(failed)
			task.Progress = progress
			task.Message = fmt.Sprintf("downloading: %d/%d tiles", downloaded, len(tiles))

			// 每10个瓦片广播一次进度
			if downloaded%10 == 0 || downloaded == int64(len(tiles)) {
				d.sendMessage(session, ProgressMessage{
					Type:     "progress",
					TaskID:   task.ID,
					Progress: progress,
					Message:  task.Message,
					Data: map[string]interface{}{
						"status":          task.Status,
						"totalTiles":      task.TotalTiles,
						"downloadedTiles": task.DownloadedTiles,
						"failedTiles":     task.FailedTiles,
					},
				})
			}
		}(tile)
	}

	wg.Wait()

	// 检查是否取消
	select {
	case <-session.ctx.Done():
		return
	default:
	}

	// 检查是否有足够的瓦片
	if len(downloadedTiles) == 0 {
		d.failSession(session, "no tiles downloaded successfully")
		return
	}

	// 拼接瓦片到GeoTiff
	task.Message = "merging tiles..."
	d.sendMessage(session, ProgressMessage{
		Type:     "progress",
		TaskID:   task.ID,
		Progress: 95,
		Message:  "merging tiles to GeoTiff...",
		Data: map[string]interface{}{
			"status":          task.Status,
			"totalTiles":      task.TotalTiles,
			"downloadedTiles": task.DownloadedTiles,
			"failedTiles":     task.FailedTiles,
		},
	})

	format := "png"
	if netMap.ImageFormat != "" {
		format = netMap.ImageFormat
	}

	for _, tile := range tiles {
		key := fmt.Sprintf("%d_%d", tile.X, tile.Y)
		tileData, ok := downloadedTiles[key]
		if !ok || len(tileData) == 0 {
			continue
		}

		dstX := (tile.X - minTile.X) * tileSize
		dstY := (tile.Y - minTile.Y) * tileSize

		if err := writer.WriteTile(tileData, format, dstX, dstY); err != nil {
			log.Printf("Failed to write tile %d/%d: %v", tile.X, tile.Y, err)
		}
	}

	// 导出GeoTiff文件
	outputFile := filepath.Join(d.outputDir, fmt.Sprintf("%s_z%d_%d.tif", netMap.MapName, zoom, time.Now().Unix()))
	if err := writer.ExportToFile(outputFile); err != nil {
		d.failSession(session, fmt.Sprintf("failed to export GeoTiff: %v", err))
		return
	}

	// 完成任务
	completedAt := time.Now()
	task.Status = "completed"
	task.Progress = 100
	task.Message = "download completed"
	task.OutputFile = outputFile
	task.CompletedAt = &completedAt

	// 保存已完成的任务
	d.storeCompletedTask(task)

	log.Printf("Task %s completed, output: %s", task.ID, outputFile)

	d.sendMessage(session, ProgressMessage{
		Type:     "completed",
		TaskID:   task.ID,
		Progress: 100,
		Message:  "download completed",
		Data: map[string]interface{}{
			"status":          task.Status,
			"totalTiles":      task.TotalTiles,
			"downloadedTiles": task.DownloadedTiles,
			"failedTiles":     task.FailedTiles,
			"outputFile":      outputFile,
		},
	})
}

// sendMessage 发送消息给客户端
func (d *WebTileDownloader) sendMessage(session *DownloadSession, msg ProgressMessage) {
	session.mu.Lock()
	defer session.mu.Unlock()

	session.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := session.conn.WriteJSON(msg); err != nil {
		log.Printf("Failed to send message for task %s: %v", session.task.ID, err)
	}
}

// failSession 标记会话失败
func (d *WebTileDownloader) failSession(session *DownloadSession, message string) {
	log.Printf("Task %s failed: %s", session.task.ID, message)

	session.task.Status = "failed"
	session.task.Message = message

	// 保存失败的任务
	d.storeCompletedTask(session.task)

	d.sendMessage(session, ProgressMessage{
		Type:     "error",
		TaskID:   session.task.ID,
		Progress: session.task.Progress,
		Message:  message,
		Data: map[string]interface{}{
			"status":          session.task.Status,
			"totalTiles":      session.task.TotalTiles,
			"downloadedTiles": session.task.DownloadedTiles,
			"failedTiles":     session.task.FailedTiles,
		},
	})
}

// ========== GeoJSON 解析 ==========

func (d *WebTileDownloader) parseGeoJSONToBBox(geoJSON json.RawMessage) (*BoundingBox, error) {
	var fc GeoJSONFeatureCollection
	if err := json.Unmarshal(geoJSON, &fc); err == nil && fc.Type == "FeatureCollection" {
		return d.extractBBoxFromFeatures(fc.Features)
	}

	var feature GeoJSONFeature
	if err := json.Unmarshal(geoJSON, &feature); err == nil && feature.Type == "Feature" {
		return d.extractBBoxFromGeometry(feature.Geometry)
	}

	var geometry GeoJSONGeometry
	if err := json.Unmarshal(geoJSON, &geometry); err == nil {
		return d.extractBBoxFromGeometry(geometry)
	}

	return nil, fmt.Errorf("unsupported GeoJSON format")
}

// webtile_downloader.go (续)

func (d *WebTileDownloader) extractBBoxFromFeatures(features []GeoJSONFeature) (*BoundingBox, error) {
	if len(features) == 0 {
		return nil, fmt.Errorf("no features in collection")
	}

	bbox := &BoundingBox{
		MinLon: 180,
		MinLat: 90,
		MaxLon: -180,
		MaxLat: -90,
	}

	for _, feature := range features {
		featureBBox, err := d.extractBBoxFromGeometry(feature.Geometry)
		if err != nil {
			continue
		}
		bbox.MinLon = math.Min(bbox.MinLon, featureBBox.MinLon)
		bbox.MinLat = math.Min(bbox.MinLat, featureBBox.MinLat)
		bbox.MaxLon = math.Max(bbox.MaxLon, featureBBox.MaxLon)
		bbox.MaxLat = math.Max(bbox.MaxLat, featureBBox.MaxLat)
	}

	return bbox, nil
}

func (d *WebTileDownloader) extractBBoxFromGeometry(geometry GeoJSONGeometry) (*BoundingBox, error) {
	bbox := &BoundingBox{
		MinLon: 180,
		MinLat: 90,
		MaxLon: -180,
		MaxLat: -90,
	}

	switch geometry.Type {
	case "Point":
		var coords [2]float64
		if err := json.Unmarshal(geometry.Coordinates, &coords); err != nil {
			return nil, err
		}
		bbox.MinLon, bbox.MaxLon = coords[0], coords[0]
		bbox.MinLat, bbox.MaxLat = coords[1], coords[1]

	case "LineString", "MultiPoint":
		var coords [][2]float64
		if err := json.Unmarshal(geometry.Coordinates, &coords); err != nil {
			return nil, err
		}
		for _, coord := range coords {
			bbox.MinLon = math.Min(bbox.MinLon, coord[0])
			bbox.MaxLon = math.Max(bbox.MaxLon, coord[0])
			bbox.MinLat = math.Min(bbox.MinLat, coord[1])
			bbox.MaxLat = math.Max(bbox.MaxLat, coord[1])
		}

	case "Polygon", "MultiLineString":
		var coords [][][2]float64
		if err := json.Unmarshal(geometry.Coordinates, &coords); err != nil {
			return nil, err
		}
		for _, ring := range coords {
			for _, coord := range ring {
				bbox.MinLon = math.Min(bbox.MinLon, coord[0])
				bbox.MaxLon = math.Max(bbox.MaxLon, coord[0])
				bbox.MinLat = math.Min(bbox.MinLat, coord[1])
				bbox.MaxLat = math.Max(bbox.MaxLat, coord[1])
			}
		}

	case "MultiPolygon":
		var coords [][][][2]float64
		if err := json.Unmarshal(geometry.Coordinates, &coords); err != nil {
			return nil, err
		}
		for _, polygon := range coords {
			for _, ring := range polygon {
				for _, coord := range ring {
					bbox.MinLon = math.Min(bbox.MinLon, coord[0])
					bbox.MaxLon = math.Max(bbox.MaxLon, coord[0])
					bbox.MinLat = math.Min(bbox.MinLat, coord[1])
					bbox.MaxLat = math.Max(bbox.MaxLat, coord[1])
				}
			}
		}

	default:
		return nil, fmt.Errorf("unsupported geometry type: %s", geometry.Type)
	}

	return bbox, nil
}

// ========== 瓦片计算 ==========

func (d *WebTileDownloader) calculateTilesInBBox(bbox *BoundingBox, zoom int) []TileIndex {
	minTile := LonLatToTileCoord(bbox.MinLon, bbox.MaxLat, zoom)
	maxTile := LonLatToTileCoord(bbox.MaxLon, bbox.MinLat, zoom)

	var tiles []TileIndex
	for x := minTile.X; x <= maxTile.X; x++ {
		for y := minTile.Y; y <= maxTile.Y; y++ {
			tiles = append(tiles, TileIndex{Z: zoom, X: x, Y: y})
		}
	}

	return tiles
}

// ========== 瓦片下载 ==========

func (d *WebTileDownloader) downloadTileDirect(ctx context.Context, netMap *models.NetMap, tile TileIndex) ([]byte, error) {
	url := d.buildTileURL(netMap, tile.Z, tile.X, tile.Y)
	return d.fetchTileWithRetry(ctx, url, MaxRetries)
}

func (d *WebTileDownloader) downloadTileWithTransform(
	ctx context.Context,
	netMap *models.NetMap,
	tile TileIndex,
	tileSize int,
	transform CoordTransformFunc,
) ([]byte, error) {
	bounds := GetTileBoundsWGS84(tile.Z, tile.X, tile.Y)
	transformedBounds := d.transformBoundsWithSampling(bounds, transform)

	topLeftPixel := LonLatToGlobalPixel(
		transformedBounds.MinLon,
		transformedBounds.MaxLat,
		tile.Z,
		tileSize,
	)
	bottomRightPixel := LonLatToGlobalPixel(
		transformedBounds.MaxLon,
		transformedBounds.MinLat,
		tile.Z,
		tileSize,
	)

	tileRange, cropInfo := CalculateRequiredTilesWithCrop(topLeftPixel, bottomRightPixel, tileSize)
	maxTileIndex := (1 << tile.Z) - 1
	tileRange = d.clampTileRange(tileRange, maxTileIndex)

	tileCount := (tileRange.MaxX - tileRange.MinX + 1) * (tileRange.MaxY - tileRange.MinY + 1)
	if tileCount > 16 {
		return nil, fmt.Errorf("too many source tiles required: %d", tileCount)
	}

	tiles, err := d.fetchTilesParallel(ctx, netMap, tile.Z, tileRange)
	if err != nil {
		return nil, err
	}

	cropX := int(math.Round(cropInfo.CropX))
	cropY := int(math.Round(cropInfo.CropY))
	canvasWidth := (tileRange.MaxX - tileRange.MinX + 1) * tileSize
	canvasHeight := (tileRange.MaxY - tileRange.MinY + 1) * tileSize

	if cropX < 0 {
		cropX = 0
	}
	if cropY < 0 {
		cropY = 0
	}
	if cropX+tileSize > canvasWidth {
		cropX = canvasWidth - tileSize
		if cropX < 0 {
			cropX = 0
		}
	}
	if cropY+tileSize > canvasHeight {
		cropY = canvasHeight - tileSize
		if cropY < 0 {
			cropY = 0
		}
	}

	outputFormat := "PNG"
	if netMap.ImageFormat == "jpg" || netMap.ImageFormat == "jpeg" {
		outputFormat = "JPEG"
	}

	return SafeImageProcess(ctx, tiles, tileRange, tileSize, cropX, cropY, outputFormat)
}

func (d *WebTileDownloader) fetchTilesParallel(ctx context.Context, netMap *models.NetMap, z int, tileRange TileRange) ([]FetchedTile, error) {
	tileCount := (tileRange.MaxX - tileRange.MinX + 1) * (tileRange.MaxY - tileRange.MinY + 1)
	if tileCount <= 0 {
		return nil, fmt.Errorf("invalid tile range")
	}

	var wg sync.WaitGroup
	tileChan := make(chan FetchedTile, tileCount)
	semaphore := make(chan struct{}, 4)

	for x := tileRange.MinX; x <= tileRange.MaxX; x++ {
		for y := tileRange.MinY; y <= tileRange.MaxY; y++ {
			wg.Add(1)
			go func(tileX, tileY int) {
				defer wg.Done()

				semaphore <- struct{}{}
				defer func() { <-semaphore }()

				url := d.buildTileURL(netMap, z, tileX, tileY)
				data, err := d.fetchTileWithRetry(ctx, url, MaxRetries)

				format := "png"
				if netMap.ImageFormat != "" {
					format = netMap.ImageFormat
				}

				tileChan <- FetchedTile{
					Data:   data,
					X:      tileX,
					Y:      tileY,
					Format: format,
					Err:    err,
				}
			}(x, y)
		}
	}

	go func() {
		wg.Wait()
		close(tileChan)
	}()

	var tiles []FetchedTile
	for tile := range tileChan {
		if tile.Err == nil && len(tile.Data) > 0 {
			tiles = append(tiles, tile)
		}
	}

	return tiles, nil
}

func (d *WebTileDownloader) transformBoundsWithSampling(bounds TileBounds, transform CoordTransformFunc) TileBounds {
	const samples = 10

	var minLon, maxLon, minLat, maxLat float64
	first := true

	for i := 0; i <= samples; i++ {
		t := float64(i) / float64(samples)

		edges := [][2]float64{
			{bounds.MinLon + (bounds.MaxLon-bounds.MinLon)*t, bounds.MaxLat},
			{bounds.MinLon + (bounds.MaxLon-bounds.MinLon)*t, bounds.MinLat},
			{bounds.MinLon, bounds.MinLat + (bounds.MaxLat-bounds.MinLat)*t},
			{bounds.MaxLon, bounds.MinLat + (bounds.MaxLat-bounds.MinLat)*t},
		}

		for _, edge := range edges {
			lon, lat := transform(edge[0], edge[1])
			if first {
				minLon, maxLon, minLat, maxLat = lon, lon, lat, lat
				first = false
			} else {
				minLon = math.Min(minLon, lon)
				maxLon = math.Max(maxLon, lon)
				minLat = math.Min(minLat, lat)
				maxLat = math.Max(maxLat, lat)
			}
		}
	}

	return TileBounds{
		MinLon: minLon,
		MaxLon: maxLon,
		MinLat: minLat,
		MaxLat: maxLat,
	}
}

func (d *WebTileDownloader) clampTileRange(tr TileRange, maxIndex int) TileRange {
	if tr.MinX < 0 {
		tr.MinX = 0
	}
	if tr.MinY < 0 {
		tr.MinY = 0
	}
	if tr.MaxX > maxIndex {
		tr.MaxX = maxIndex
	}
	if tr.MaxY > maxIndex {
		tr.MaxY = maxIndex
	}
	return tr
}

// ========== 坐标转换 ==========

func (d *WebTileDownloader) wgs84ToGCJ02(lon, lat float64) (float64, float64) {
	return d.coordConv.WGS84ToGCJ02(lon, lat)
}

func (d *WebTileDownloader) wgs84ToBD09(lon, lat float64) (float64, float64) {
	return d.coordConv.WGS84ToBD09(lon, lat)
}

// ========== URL 构建 ==========

func (d *WebTileDownloader) buildTileURL(netMap *models.NetMap, z, x, y int) string {
	if netMap.TileUrlTemplate != "" {
		url := netMap.TileUrlTemplate
		url = strings.ReplaceAll(url, "{z}", strconv.Itoa(z))
		url = strings.ReplaceAll(url, "{x}", strconv.Itoa(x))
		url = strings.ReplaceAll(url, "{y}", strconv.Itoa(y))
		url = strings.ReplaceAll(url, "{-y}", strconv.Itoa(int(1<<uint(z))-1-y))
		return url
	}

	protocol := netMap.Protocol
	if protocol == "" {
		protocol = "https"
	}

	port := ""
	if netMap.Port != 0 && netMap.Port != 80 && netMap.Port != 443 {
		port = fmt.Sprintf(":%d", netMap.Port)
	}

	path := netMap.UrlPath
	path = strings.ReplaceAll(path, "{z}", strconv.Itoa(z))
	path = strings.ReplaceAll(path, "{x}", strconv.Itoa(x))
	path = strings.ReplaceAll(path, "{y}", strconv.Itoa(y))

	return fmt.Sprintf("%s://%s%s%s", protocol, netMap.Hostname, port, path)
}

// ========== HTTP 请求 ==========

func (d *WebTileDownloader) fetchTileWithRetry(ctx context.Context, url string, maxRetries int) ([]byte, error) {
	var lastErr error
	delay := RetryDelay

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				delay = delay * time.Duration(RetryBackoff)
			}
		}

		data, err := d.fetchTile(ctx, url)
		if err == nil && d.isValidTileData(data) {
			return data, nil
		}

		lastErr = err
		if err == nil && !d.isValidTileData(data) {
			lastErr = fmt.Errorf("invalid tile data")
		}
	}

	return nil, fmt.Errorf("all %d attempts failed: %v", maxRetries+1, lastErr)
}

func (d *WebTileDownloader) fetchTile(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request failed: %v", err)
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	req.Header.Set("Accept", "image/webp,image/apng,image/*,*/*;q=0.8")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch tile failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tile server returned status: %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response failed: %v", err)
	}

	return data, nil
}

func (d *WebTileDownloader) isValidTileData(data []byte) bool {
	if len(data) < 100 {
		return false
	}

	// PNG
	if len(data) >= 8 {
		pngSignature := []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}
		isPNG := true
		for i := 0; i < 8; i++ {
			if data[i] != pngSignature[i] {
				isPNG = false
				break
			}
		}
		if isPNG && len(data) > 500 {
			return true
		}
	}

	// JPEG
	if len(data) >= 3 && data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF {
		return len(data) > 1000
	}

	// WebP
	if len(data) >= 12 && string(data[0:4]) == "RIFF" && string(data[8:12]) == "WEBP" {
		return true
	}

	return true
}

// ========== 清理 ==========

func (d *WebTileDownloader) Close() error {
	if d.cache != nil {
		return d.cache.Close()
	}
	return nil
}
