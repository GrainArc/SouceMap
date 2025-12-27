package tile_proxy

import (
	"context"
	"fmt"
	"github.com/GrainArc/Gogeo"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GrainArc/SouceMap/models"
	"github.com/GrainArc/SouceMap/pgmvt"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// 坐标系类型
const (
	CoordWGS84 = "0" // WGS84
	CoordGCJ02 = "1" // 火星坐标系
	CoordBD09  = "2" // 百度坐标系
)

// TileProxyService 瓦片代理服务
type TileProxyService struct {
	db         *gorm.DB
	httpClient *http.Client
	coordConv  *pgmvt.ChangeCoord
	cache      *TileCache
}

// NewTileProxyService 创建瓦片代理服务
func NewTileProxyService() *TileProxyService {
	return &TileProxyService{
		db: models.DB,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		coordConv: pgmvt.NewChangeCoord(),
		cache:     NewTileCache(1000, 10*time.Minute), // 1000个瓦片，10分钟过期
	}
}

// TileRequest 瓦片请求参数
type TileRequest struct {
	MapID uint
	Z     int
	X     int
	Y     int
}

// FetchedTile 获取的瓦片数据
type FetchedTile struct {
	Data   []byte
	X      int
	Y      int
	Format string
	Err    error
}

// RegisterRoutes 注册路由
func (s *TileProxyService) RegisterRoutes(r *gin.RouterGroup) {
	r.GET("/tile/:mapId/:z/:x/:y", s.HandleTileRequest)
}

// HandleTileRequest 处理瓦片请求
func (s *TileProxyService) HandleTileRequest(c *gin.Context) {
	// 解析参数
	mapID, err := strconv.ParseUint(c.Param("mapId"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid map id"})
		return
	}

	z, err := strconv.Atoi(c.Param("z"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid z"})
		return
	}

	x, err := strconv.Atoi(c.Param("x"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid x"})
		return
	}

	// 处理y参数（可能带扩展名）
	yStr := c.Param("y")
	yStr = strings.TrimSuffix(yStr, ".png")
	yStr = strings.TrimSuffix(yStr, ".jpg")
	yStr = strings.TrimSuffix(yStr, ".jpeg")
	yStr = strings.TrimSuffix(yStr, ".webp")

	y, err := strconv.Atoi(yStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid y"})
		return
	}

	req := TileRequest{
		MapID: uint(mapID),
		Z:     z,
		X:     x,
		Y:     y,
	}

	// 获取地图配置
	var netMap models.NetMap
	if err := s.db.First(&netMap, req.MapID).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "map not found"})
		return
	}

	if netMap.Status != 1 {
		c.JSON(http.StatusForbidden, gin.H{"error": "map is disabled"})
		return
	}

	// 检查缓存
	cacheKey := fmt.Sprintf("%d_%d_%d_%d", req.MapID, req.Z, req.X, req.Y)
	if cachedData, ok := s.cache.Get(cacheKey); ok {
		s.sendTileResponse(c, cachedData, netMap.ImageFormat)
		return
	}

	// 根据坐标系处理
	var tileData []byte
	switch netMap.Projection {
	case CoordWGS84:
		// WGS84坐标系，直接代理
		tileData, err = s.proxyDirectTile(c.Request.Context(), &netMap, req)
	case CoordGCJ02:
		// GCJ02坐标系，需要转换
		tileData, err = s.proxyWithCoordTransform(c.Request.Context(), &netMap, req, s.wgs84ToGCJ02)
	case CoordBD09:
		// BD09坐标系，需要转换
		tileData, err = s.proxyWithCoordTransform(c.Request.Context(), &netMap, req, s.wgs84ToBD09)
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported projection"})
		return
	}

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 缓存结果
	s.cache.Set(cacheKey, tileData)

	// 返回瓦片
	s.sendTileResponse(c, tileData, netMap.ImageFormat)
}

// proxyDirectTile 直接代理瓦片（无坐标转换）
func (s *TileProxyService) proxyDirectTile(ctx context.Context, netMap *models.NetMap, req TileRequest) ([]byte, error) {
	url := s.buildTileURL(netMap, req.Z, req.X, req.Y)
	return s.fetchTile(ctx, url)
}

// CoordTransformFunc 坐标转换函数类型
type CoordTransformFunc func(lon, lat float64) (float64, float64)

// wgs84ToGCJ02 WGS84转GCJ02
func (s *TileProxyService) wgs84ToGCJ02(lon, lat float64) (float64, float64) {
	return s.coordConv.WGS84ToGCJ02(lon, lat)
}

// wgs84ToBD09 WGS84转BD09
func (s *TileProxyService) wgs84ToBD09(lon, lat float64) (float64, float64) {
	return s.coordConv.WGS84ToBD09(lon, lat)
}

// proxyWithCoordTransform 带坐标转换的瓦片代理
func (s *TileProxyService) proxyWithCoordTransform(ctx context.Context, netMap *models.NetMap, req TileRequest, transform CoordTransformFunc) ([]byte, error) {
	// 1. 计算请求瓦片的WGS84边界
	bounds := GetTileBoundsWGS84(req.Z, req.X, req.Y)

	// 2. 转换四个角点到目标坐标系
	topLeftLon, topLeftLat := transform(bounds.MinLon, bounds.MaxLat)
	topRightLon, topRightLat := transform(bounds.MaxLon, bounds.MaxLat)
	bottomLeftLon, bottomLeftLat := transform(bounds.MinLon, bounds.MinLat)
	bottomRightLon, bottomRightLat := transform(bounds.MaxLon, bounds.MinLat)

	// 3. 计算转换后的边界范围
	minLon := min4(topLeftLon, topRightLon, bottomLeftLon, bottomRightLon)
	maxLon := max4(topLeftLon, topRightLon, bottomLeftLon, bottomRightLon)
	minLat := min4(topLeftLat, topRightLat, bottomLeftLat, bottomRightLat)
	maxLat := max4(topLeftLat, topRightLat, bottomLeftLat, bottomRightLat)

	// 4. 计算全局像素坐标
	topLeftPixel := LonLatToGlobalPixel(minLon, maxLat, req.Z)
	bottomRightPixel := LonLatToGlobalPixel(maxLon, minLat, req.Z)

	// 5. 计算需要请求的瓦片范围
	tileRange, offset := CalculateRequiredTiles(topLeftPixel, bottomRightPixel)

	// 6. 并发请求所有需要的瓦片
	tiles := s.fetchTilesParallel(ctx, netMap, req.Z, tileRange)

	// 7. 计算画布尺寸
	canvasWidth := (tileRange.MaxX - tileRange.MinX + 1) * TileSize
	canvasHeight := (tileRange.MaxY - tileRange.MinY + 1) * TileSize

	// 8. 使用GDAL处理图像
	processor, err := Gogeo.NewImageProcessor(canvasWidth, canvasHeight, 4) // RGBA
	if err != nil {
		return nil, fmt.Errorf("failed to create image processor: %v", err)
	}
	defer processor.Close()

	// 9. 将瓦片拼接到画布
	for _, tile := range tiles {
		if tile.Err != nil {
			continue // 跳过失败的瓦片
		}

		dstX := (tile.X - tileRange.MinX) * TileSize
		dstY := (tile.Y - tileRange.MinY) * TileSize

		if err := processor.AddTileFromBuffer(tile.Data, tile.Format, dstX, dstY); err != nil {
			// 记录错误但继续处理
			fmt.Printf("Warning: failed to add tile %d/%d: %v\n", tile.X, tile.Y, err)
		}
	}

	// 10. 裁剪并导出
	cropX := int(offset.X)
	cropY := int(offset.Y)

	// 确保裁剪区域不超出画布
	if cropX+TileSize > canvasWidth {
		cropX = canvasWidth - TileSize
	}
	if cropY+TileSize > canvasHeight {
		cropY = canvasHeight - TileSize
	}
	if cropX < 0 {
		cropX = 0
	}
	if cropY < 0 {
		cropY = 0
	}

	outputFormat := "PNG"
	if netMap.ImageFormat == "jpg" || netMap.ImageFormat == "jpeg" {
		outputFormat = "JPEG"
	}

	return processor.CropAndExport(cropX, cropY, TileSize, TileSize, outputFormat)
}

// fetchTilesParallel 并发获取瓦片
func (s *TileProxyService) fetchTilesParallel(ctx context.Context, netMap *models.NetMap, z int, tileRange TileRange) []FetchedTile {
	var wg sync.WaitGroup
	tileChan := make(chan FetchedTile, (tileRange.MaxX-tileRange.MinX+1)*(tileRange.MaxY-tileRange.MinY+1))

	for x := tileRange.MinX; x <= tileRange.MaxX; x++ {
		for y := tileRange.MinY; y <= tileRange.MaxY; y++ {
			wg.Add(1)
			go func(tileX, tileY int) {
				defer wg.Done()

				url := s.buildTileURL(netMap, z, tileX, tileY)
				data, err := s.fetchTile(ctx, url)

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

	// 等待所有请求完成
	go func() {
		wg.Wait()
		close(tileChan)
	}()

	// 收集结果
	var tiles []FetchedTile
	for tile := range tileChan {
		tiles = append(tiles, tile)
	}

	return tiles
}

// buildTileURL 构建瓦片URL
func (s *TileProxyService) buildTileURL(netMap *models.NetMap, z, x, y int) string {
	// 如果有完整的URL模板，使用模板
	if netMap.TileUrlTemplate != "" {
		url := netMap.TileUrlTemplate
		url = strings.ReplaceAll(url, "{z}", strconv.Itoa(z))
		url = strings.ReplaceAll(url, "{x}", strconv.Itoa(x))
		url = strings.ReplaceAll(url, "{y}", strconv.Itoa(y))
		url = strings.ReplaceAll(url, "{-y}", strconv.Itoa(int(1<<uint(z))-1-y)) // TMS格式
		return url
	}

	// 否则根据各字段拼接
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

// fetchTile 获取单个瓦片
func (s *TileProxyService) fetchTile(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request failed: %v", err)
	}

	// 设置请求头，模拟浏览器
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	req.Header.Set("Accept", "image/webp,image/apng,image/*,*/*;q=0.8")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	req.Header.Set("Referer", url)

	resp, err := s.httpClient.Do(req)
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

// sendTileResponse 发送瓦片响应
func (s *TileProxyService) sendTileResponse(c *gin.Context, data []byte, format string) {
	contentType := "image/png"
	switch strings.ToLower(format) {
	case "jpg", "jpeg":
		contentType = "image/jpeg"
	case "webp":
		contentType = "image/webp"
	case "png":
		contentType = "image/png"
	}

	c.Header("Content-Type", contentType)
	c.Header("Cache-Control", "public, max-age=86400") // 缓存1天
	c.Header("Access-Control-Allow-Origin", "*")
	c.Data(http.StatusOK, contentType, data)
}

// 辅助函数
func min4(a, b, c, d float64) float64 {
	return min(min(a, b), min(c, d))
}

func max4(a, b, c, d float64) float64 {
	return max(max(a, b), max(c, d))
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
