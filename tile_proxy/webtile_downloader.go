// webtile_downloader.go
package tile_proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/GrainArc/SouceMap/pgmvt"
	"io"
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
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"gorm.io/gorm"
)

// DownloadRequest 下载请求参数
type DownloadRequest struct {
	MapName   string          `json:"mapName" binding:"required"`   // 地图名称
	ZoomLevel int             `json:"zoomLevel" binding:"required"` // 下载层级
	GeoJSON   json.RawMessage `json:"geoJson" binding:"required"`   // 范围GeoJSON
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
	Type     string      `json:"type"` // progress, completed, error
	TaskID   string      `json:"taskId"`
	Progress float64     `json:"progress"`
	Message  string      `json:"message"`
	Data     interface{} `json:"data,omitempty"`
}

// GeoJSONGeometry GeoJSON几何体
type GeoJSONGeometry struct {
	Type        string          `json:"type"`
	Coordinates json.RawMessage `json:"coordinates"`
}

// GeoJSONFeature GeoJSON要素
type GeoJSONFeature struct {
	Type       string          `json:"type"`
	Geometry   GeoJSONGeometry `json:"geometry"`
	Properties interface{}     `json:"properties"`
}

// GeoJSONFeatureCollection GeoJSON要素集合
type GeoJSONFeatureCollection struct {
	Type     string           `json:"type"`
	Features []GeoJSONFeature `json:"features"`
}

// BoundingBox 边界框
type BoundingBox struct {
	MinLon float64
	MinLat float64
	MaxLon float64
	MaxLat float64
}

// TileIndex 瓦片索引
type TileIndex struct {
	Z int
	X int
	Y int
}

// WebTileDownloader 网络瓦片下载器
type WebTileDownloader struct {
	db            *gorm.DB
	httpClient    *http.Client
	coordConv     *pgmvt.ChangeCoord
	cache         *TileCache
	safeProcessor *SafeTileProcessor
	tasks         sync.Map // taskID -> *DownloadTask
	wsClients     sync.Map // taskID -> []*websocket.Conn
	outputDir     string
	upgrader      websocket.Upgrader
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
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
	}
}

// InitWebtileDownloader 初始化下载任务
func (d *WebTileDownloader) InitWebtileDownloader(c *gin.Context) {
	var req DownloadRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid request: %v", err)})
		return
	}

	// 验证层级
	if req.ZoomLevel < 0 || req.ZoomLevel > 20 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "zoom level must be between 0 and 20"})
		return
	}

	// 查找地图配置
	var netMap models.NetMap
	if err := d.db.Where("map_name = ? AND status = 1", req.MapName).First(&netMap).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "map not found or disabled"})
		return
	}

	// 解析GeoJSON获取边界框
	bbox, err := d.parseGeoJSONToBBox(req.GeoJSON)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid geojson: %v", err)})
		return
	}

	// 计算需要下载的瓦片
	tiles := d.calculateTilesInBBox(bbox, req.ZoomLevel)
	if len(tiles) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no tiles in the specified area"})
		return
	}

	// 限制最大瓦片数量
	maxTiles := 10000
	if len(tiles) > maxTiles {
		c.JSON(http.StatusBadRequest, gin.H{
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
		CreatedAt:  time.Now(),
	}

	d.tasks.Store(taskID, task)

	// 启动后台下载任务
	go d.executeDownloadTask(taskID, &netMap, tiles, bbox, req.ZoomLevel)

	c.JSON(http.StatusOK, gin.H{
		"taskId":     taskID,
		"totalTiles": len(tiles),
		"message":    "download task created, connect to websocket for progress",
	})
}

// ConnectWebSocket WebSocket连接处理
func (d *WebTileDownloader) ConnectWebSocket(c *gin.Context) {
	taskID := c.Query("taskId")
	if taskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "taskId is required"})
		return
	}

	// 检查任务是否存在
	if _, ok := d.tasks.Load(taskID); !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
		return
	}

	// 升级为WebSocket连接
	conn, err := d.upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("websocket upgrade failed: %v", err)})
		return
	}

	// 注册客户端
	d.registerWSClient(taskID, conn)

	// 发送当前状态
	if taskVal, ok := d.tasks.Load(taskID); ok {
		task := taskVal.(*DownloadTask)
		d.sendProgressToClient(conn, task)
	}

	// 保持连接并处理消息
	go d.handleWSConnection(taskID, conn)
}

// registerWSClient 注册WebSocket客户端
func (d *WebTileDownloader) registerWSClient(taskID string, conn *websocket.Conn) {
	clientsVal, _ := d.wsClients.LoadOrStore(taskID, &sync.Map{})
	clients := clientsVal.(*sync.Map)
	clients.Store(conn, true)
}

// unregisterWSClient 注销WebSocket客户端
func (d *WebTileDownloader) unregisterWSClient(taskID string, conn *websocket.Conn) {
	if clientsVal, ok := d.wsClients.Load(taskID); ok {
		clients := clientsVal.(*sync.Map)
		clients.Delete(conn)
	}
	conn.Close()
}

// handleWSConnection 处理WebSocket连接
func (d *WebTileDownloader) handleWSConnection(taskID string, conn *websocket.Conn) {
	defer d.unregisterWSClient(taskID, conn)

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

// broadcastProgress 广播进度
func (d *WebTileDownloader) broadcastProgress(taskID string, msg ProgressMessage) {
	if clientsVal, ok := d.wsClients.Load(taskID); ok {
		clients := clientsVal.(*sync.Map)
		clients.Range(func(key, value interface{}) bool {
			conn := key.(*websocket.Conn)
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteJSON(msg); err != nil {
				d.unregisterWSClient(taskID, conn)
			}
			return true
		})
	}
}

// sendProgressToClient 发送进度给单个客户端
func (d *WebTileDownloader) sendProgressToClient(conn *websocket.Conn, task *DownloadTask) {
	msg := ProgressMessage{
		Type:     "progress",
		TaskID:   task.ID,
		Progress: task.Progress,
		Message:  task.Message,
		Data:     task,
	}
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	conn.WriteJSON(msg)
}

// parseGeoJSONToBBox 解析GeoJSON为边界框
func (d *WebTileDownloader) parseGeoJSONToBBox(geoJSON json.RawMessage) (*BoundingBox, error) {
	// 尝试解析为FeatureCollection
	var fc GeoJSONFeatureCollection
	if err := json.Unmarshal(geoJSON, &fc); err == nil && fc.Type == "FeatureCollection" {
		return d.extractBBoxFromFeatures(fc.Features)
	}

	// 尝试解析为单个Feature
	var feature GeoJSONFeature
	if err := json.Unmarshal(geoJSON, &feature); err == nil && feature.Type == "Feature" {
		return d.extractBBoxFromGeometry(feature.Geometry)
	}

	// 尝试解析为Geometry
	var geometry GeoJSONGeometry
	if err := json.Unmarshal(geoJSON, &geometry); err == nil {
		return d.extractBBoxFromGeometry(geometry)
	}

	return nil, fmt.Errorf("unsupported GeoJSON format")
}

// extractBBoxFromFeatures 从要素集合提取边界框
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

// extractBBoxFromGeometry 从几何体提取边界框 (续)
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

// calculateTilesInBBox 计算边界框内的所有瓦片
func (d *WebTileDownloader) calculateTilesInBBox(bbox *BoundingBox, zoom int) []TileIndex {
	// 计算瓦片范围
	minTile := LonLatToTileCoord(bbox.MinLon, bbox.MaxLat, zoom) // 左上角
	maxTile := LonLatToTileCoord(bbox.MaxLon, bbox.MinLat, zoom) // 右下角

	var tiles []TileIndex
	for x := minTile.X; x <= maxTile.X; x++ {
		for y := minTile.Y; y <= maxTile.Y; y++ {
			tiles = append(tiles, TileIndex{Z: zoom, X: x, Y: y})
		}
	}

	return tiles
}

// executeDownloadTask 执行下载任务
func (d *WebTileDownloader) executeDownloadTask(taskID string, netMap *models.NetMap, tiles []TileIndex, bbox *BoundingBox, zoom int) {
	taskVal, _ := d.tasks.Load(taskID)
	task := taskVal.(*DownloadTask)

	// 更新任务状态
	now := time.Now()
	task.Status = "running"
	task.StartedAt = &now
	task.Message = "downloading tiles..."

	d.broadcastProgress(taskID, ProgressMessage{
		Type:     "progress",
		TaskID:   taskID,
		Progress: 0,
		Message:  "starting download...",
		Data:     task,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

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
	// GeoTransform[0]: 左上角X坐标（经度）
	// GeoTransform[1]: 像素宽度（经度方向）
	// GeoTransform[2]: 旋转（通常为0）
	// GeoTransform[3]: 左上角Y坐标（纬度）
	// GeoTransform[4]: 旋转（通常为0）
	// GeoTransform[5]: 像素高度（纬度方向，通常为负值）
	topLeftBounds := GetTileBoundsWGS84(zoom, minTile.X, minTile.Y)
	bottomRightBounds := GetTileBoundsWGS84(zoom, maxTile.X, maxTile.Y)

	pixelWidth := (bottomRightBounds.MaxLon - topLeftBounds.MinLon) / float64(outputWidth)
	pixelHeight := (topLeftBounds.MaxLat - bottomRightBounds.MinLat) / float64(outputHeight)

	geoTransform := [6]float64{
		topLeftBounds.MinLon, // 左上角经度
		pixelWidth,           // 像素宽度
		0,                    // 旋转
		topLeftBounds.MaxLat, // 左上角纬度
		0,                    // 旋转
		-pixelHeight,         // 像素高度（负值）
	}

	// 创建GeoTiff写入器
	writer, err := Gogeo.NewGeoTiffWriter(outputWidth, outputHeight, 4, tileSize, geoTransform)
	if err != nil {
		d.failTask(taskID, fmt.Sprintf("failed to create GeoTiff writer: %v", err))
		return
	}
	defer writer.Close()

	// 判断是否需要坐标转换
	needTransform := netMap.Projection == CoordGCJ02 || netMap.Projection == CoordBD09

	// 并发下载瓦片
	var downloadedCount int64
	var failedCount int64
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 8) // 限制并发数

	tileMutex := sync.Mutex{}
	downloadedTiles := make(map[string][]byte)

	for _, tile := range tiles {
		wg.Add(1)
		go func(t TileIndex) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			var tileData []byte
			var err error

			if needTransform {
				// 需要坐标转换
				var transformFunc CoordTransformFunc
				if netMap.Projection == CoordGCJ02 {
					transformFunc = d.wgs84ToGCJ02
				} else {
					transformFunc = d.wgs84ToBD09
				}
				tileData, err = d.downloadTileWithTransform(ctx, netMap, t, tileSize, transformFunc)
			} else {
				// 直接下载
				tileData, err = d.downloadTileDirect(ctx, netMap, t)
			}

			if err != nil {
				atomic.AddInt64(&failedCount, 1)
				fmt.Printf("Failed to download tile %d/%d/%d: %v\n", t.Z, t.X, t.Y, err)
			} else {
				tileMutex.Lock()
				downloadedTiles[fmt.Sprintf("%d_%d", t.X, t.Y)] = tileData
				tileMutex.Unlock()
			}

			// 更新进度
			downloaded := atomic.AddInt64(&downloadedCount, 1)
			progress := float64(downloaded) / float64(len(tiles)) * 100

			task.DownloadedTiles = int(downloaded)
			task.FailedTiles = int(atomic.LoadInt64(&failedCount))
			task.Progress = progress
			task.Message = fmt.Sprintf("downloading: %d/%d tiles", downloaded, len(tiles))

			// 每10个瓦片广播一次进度
			if downloaded%10 == 0 || downloaded == int64(len(tiles)) {
				d.broadcastProgress(taskID, ProgressMessage{
					Type:     "progress",
					TaskID:   taskID,
					Progress: progress,
					Message:  task.Message,
					Data:     task,
				})
			}
		}(tile)
	}

	wg.Wait()

	// 检查是否有足够的瓦片
	if len(downloadedTiles) == 0 {
		d.failTask(taskID, "no tiles downloaded successfully")
		return
	}

	// 拼接瓦片到GeoTiff
	task.Message = "merging tiles..."
	d.broadcastProgress(taskID, ProgressMessage{
		Type:     "progress",
		TaskID:   taskID,
		Progress: 95,
		Message:  "merging tiles to GeoTiff...",
		Data:     task,
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

		// 计算瓦片在输出图像中的位置
		dstX := (tile.X - minTile.X) * tileSize
		dstY := (tile.Y - minTile.Y) * tileSize

		if err := writer.WriteTile(tileData, format, dstX, dstY); err != nil {
			fmt.Printf("Failed to write tile %d/%d: %v\n", tile.X, tile.Y, err)
		}
	}

	// 导出GeoTiff文件
	outputFile := filepath.Join(d.outputDir, fmt.Sprintf("%s_z%d_%d.tif", netMap.MapName, zoom, time.Now().Unix()))
	if err := writer.ExportToFile(outputFile); err != nil {
		d.failTask(taskID, fmt.Sprintf("failed to export GeoTiff: %v", err))
		return
	}

	// 完成任务
	completedAt := time.Now()
	task.Status = "completed"
	task.Progress = 100
	task.Message = "download completed"
	task.OutputFile = outputFile
	task.CompletedAt = &completedAt

	d.broadcastProgress(taskID, ProgressMessage{
		Type:     "completed",
		TaskID:   taskID,
		Progress: 100,
		Message:  "download completed",
		Data:     task,
	})
}

// downloadTileDirect 直接下载瓦片
func (d *WebTileDownloader) downloadTileDirect(ctx context.Context, netMap *models.NetMap, tile TileIndex) ([]byte, error) {
	url := d.buildTileURL(netMap, tile.Z, tile.X, tile.Y)
	return d.fetchTileWithRetry(ctx, url, MaxRetries)
}

// downloadTileWithTransform 下载并转换瓦片
func (d *WebTileDownloader) downloadTileWithTransform(
	ctx context.Context,
	netMap *models.NetMap,
	tile TileIndex,
	tileSize int,
	transform CoordTransformFunc,
) ([]byte, error) {
	// 计算WGS84瓦片边界
	bounds := GetTileBoundsWGS84(tile.Z, tile.X, tile.Y)

	// 转换边界到目标坐标系
	transformedBounds := d.transformBoundsWithSampling(bounds, transform)

	// 计算像素坐标
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

	// 计算瓦片范围
	tileRange, cropInfo := CalculateRequiredTilesWithCrop(topLeftPixel, bottomRightPixel, tileSize)

	// 限制瓦片范围
	maxTileIndex := (1 << tile.Z) - 1
	tileRange = d.clampTileRange(tileRange, maxTileIndex)

	// 限制最大瓦片数量
	tileCount := (tileRange.MaxX - tileRange.MinX + 1) * (tileRange.MaxY - tileRange.MinY + 1)
	if tileCount > 16 {
		return nil, fmt.Errorf("too many source tiles required: %d", tileCount)
	}

	// 获取源瓦片
	tiles, err := d.fetchTilesParallel(ctx, netMap, tile.Z, tileRange)
	if err != nil {
		return nil, err
	}

	// 计算裁剪参数
	cropX := int(math.Round(cropInfo.CropX))
	cropY := int(math.Round(cropInfo.CropY))
	canvasWidth := (tileRange.MaxX - tileRange.MinX + 1) * tileSize
	canvasHeight := (tileRange.MaxY - tileRange.MinY + 1) * tileSize

	// 边界检查
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

	// 确定输出格式
	outputFormat := "PNG"
	if netMap.ImageFormat == "jpg" || netMap.ImageFormat == "jpeg" {
		outputFormat = "JPEG"
	}

	// 使用安全处理函数
	return SafeImageProcess(ctx, tiles, tileRange, tileSize, cropX, cropY, outputFormat)
}

// fetchTilesParallel 并发获取瓦片
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

// transformBoundsWithSampling 使用采样点转换边界
func (d *WebTileDownloader) transformBoundsWithSampling(bounds TileBounds, transform CoordTransformFunc) TileBounds {
	const samples = 10

	var minLon, maxLon, minLat, maxLat float64
	first := true

	for i := 0; i <= samples; i++ {
		t := float64(i) / float64(samples)

		// 四条边采样
		edges := [][2]float64{
			{bounds.MinLon + (bounds.MaxLon-bounds.MinLon)*t, bounds.MaxLat}, // 上边
			{bounds.MinLon + (bounds.MaxLon-bounds.MinLon)*t, bounds.MinLat}, // 下边
			{bounds.MinLon, bounds.MinLat + (bounds.MaxLat-bounds.MinLat)*t}, // 左边
			{bounds.MaxLon, bounds.MinLat + (bounds.MaxLat-bounds.MinLat)*t}, // 右边
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

// clampTileRange 限制瓦片范围
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

// wgs84ToGCJ02 WGS84转GCJ02
func (d *WebTileDownloader) wgs84ToGCJ02(lon, lat float64) (float64, float64) {
	return d.coordConv.WGS84ToGCJ02(lon, lat)
}

// wgs84ToBD09 WGS84转BD09
func (d *WebTileDownloader) wgs84ToBD09(lon, lat float64) (float64, float64) {
	return d.coordConv.WGS84ToBD09(lon, lat)
}

// buildTileURL 构建瓦片URL
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

// fetchTileWithRetry 带重试的瓦片获取
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

// fetchTile 获取单个瓦片
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

// isValidTileData 检查瓦片数据是否有效
func (d *WebTileDownloader) isValidTileData(data []byte) bool {
	if len(data) < 100 {
		return false
	}

	// 检查PNG签名
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

	// 检查JPEG签名
	if len(data) >= 3 && data[0] == 0xFF && data[1] == 0xD8 && data[2] == 0xFF {
		return len(data) > 1000
	}

	// 检查WebP签名
	if len(data) >= 12 && string(data[0:4]) == "RIFF" && string(data[8:12]) == "WEBP" {
		return true
	}

	return true
}

// failTask 标记任务失败
func (d *WebTileDownloader) failTask(taskID string, message string) {
	if taskVal, ok := d.tasks.Load(taskID); ok {
		task := taskVal.(*DownloadTask)
		task.Status = "failed"
		task.Message = message

		d.broadcastProgress(taskID, ProgressMessage{
			Type:     "error",
			TaskID:   taskID,
			Progress: task.Progress,
			Message:  message,
			Data:     task,
		})
	}
}

// GetTask 获取任务状态
func (d *WebTileDownloader) GetTask(taskID string) (*DownloadTask, bool) {
	if taskVal, ok := d.tasks.Load(taskID); ok {
		return taskVal.(*DownloadTask), true
	}
	return nil, false
}

// Close 关闭下载器
func (d *WebTileDownloader) Close() error {
	if d.cache != nil {
		return d.cache.Close()
	}
	return nil
}
