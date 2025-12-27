// tile_proxy.go
package tile_proxy

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GrainArc/Gogeo"
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
		cache:     NewTileCache(1000, 10*time.Minute),
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

	// 获取瓦片大小配置，默认256
	tileSize := 256
	if netMap.TileSize > 0 {
		tileSize = netMap.TileSize
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
		tileData, err = s.proxyWithCoordTransform(c.Request.Context(), &netMap, req, tileSize, s.wgs84ToGCJ02)
	case CoordBD09:
		// BD09坐标系，需要转换
		tileData, err = s.proxyWithCoordTransform(c.Request.Context(), &netMap, req, tileSize, s.wgs84ToBD09)
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
func (s *TileProxyService) proxyWithCoordTransform(
	ctx context.Context,
	netMap *models.NetMap,
	req TileRequest,
	tileSize int,
	transform CoordTransformFunc,
) ([]byte, error) {
	// 1. 计算请求瓦片的WGS84边界
	bounds := GetTileBoundsWGS84(req.Z, req.X, req.Y)

	// 2. 转换四个角点到目标坐标系
	// 注意：需要更密集的采样点来处理非线性变换
	transformedBounds := s.transformBoundsWithSampling(bounds, transform)

	// 3. 计算转换后边界的全局像素坐标
	topLeftPixel := LonLatToGlobalPixel(
		transformedBounds.MinLon,
		transformedBounds.MaxLat,
		req.Z,
		tileSize,
	)
	bottomRightPixel := LonLatToGlobalPixel(
		transformedBounds.MaxLon,
		transformedBounds.MinLat,
		req.Z,
		tileSize,
	)

	// 4. 计算需要请求的瓦片范围和裁剪信息
	tileRange, cropInfo := CalculateRequiredTilesWithCrop(topLeftPixel, bottomRightPixel, tileSize)

	// 5. 验证瓦片范围的有效性
	maxTileIndex := (1 << req.Z) - 1
	tileRange = s.clampTileRange(tileRange, maxTileIndex)

	// 6. 并发请求所有需要的瓦片
	tiles := s.fetchTilesParallel(ctx, netMap, req.Z, tileRange)

	// 7. 计算画布尺寸
	canvasWidth := (tileRange.MaxX - tileRange.MinX + 1) * tileSize
	canvasHeight := (tileRange.MaxY - tileRange.MinY + 1) * tileSize

	// 8. 使用GDAL处理图像
	processor, err := Gogeo.NewImageProcessor(canvasWidth, canvasHeight, 4) // RGBA
	if err != nil {
		return nil, fmt.Errorf("failed to create image processor: %v", err)
	}
	defer processor.Close()

	// 9. 将瓦片拼接到画布
	successCount := 0
	for _, tile := range tiles {
		if tile.Err != nil {
			fmt.Printf("Warning: failed to fetch tile %d/%d/%d: %v\n", req.Z, tile.X, tile.Y, tile.Err)
			continue
		}

		if len(tile.Data) == 0 {
			continue
		}

		dstX := (tile.X - tileRange.MinX) * tileSize
		dstY := (tile.Y - tileRange.MinY) * tileSize

		if err := processor.AddTileFromBuffer(tile.Data, tile.Format, dstX, dstY); err != nil {
			fmt.Printf("Warning: failed to add tile %d/%d/%d: %v\n", req.Z, tile.X, tile.Y, err)
			continue
		}
		successCount++
	}

	if successCount == 0 {
		return nil, fmt.Errorf("no tiles were successfully fetched")
	}

	// 10. 计算精确的裁剪参数
	cropX := int(math.Round(cropInfo.CropX))
	cropY := int(math.Round(cropInfo.CropY))

	// 确保裁剪区域不超出画布边界
	if cropX < 0 {
		cropX = 0
	}
	if cropY < 0 {
		cropY = 0
	}
	if cropX+tileSize > canvasWidth {
		cropX = canvasWidth - tileSize
	}
	if cropY+tileSize > canvasHeight {
		cropY = canvasHeight - tileSize
	}

	// 确保裁剪尺寸正确
	outputWidth := tileSize
	outputHeight := tileSize

	// 如果画布本身就小于目标尺寸，调整输出尺寸
	if canvasWidth < tileSize {
		outputWidth = canvasWidth
		cropX = 0
	}
	if canvasHeight < tileSize {
		outputHeight = canvasHeight
		cropY = 0
	}

	// 11. 裁剪并导出
	outputFormat := "PNG"
	if netMap.ImageFormat == "jpg" || netMap.ImageFormat == "jpeg" {
		outputFormat = "JPEG"
	}

	// 如果输出尺寸与目标尺寸不同，需要缩放
	if outputWidth != tileSize || outputHeight != tileSize {
		// 先裁剪可用区域，然后缩放到目标尺寸
		return processor.CropScaleAndExport(cropX, cropY, outputWidth, outputHeight, tileSize, tileSize, outputFormat)
	}

	return processor.CropAndExport(cropX, cropY, tileSize, tileSize, outputFormat)
}

// transformBoundsWithSampling 使用采样点转换边界（处理非线性变换）
func (s *TileProxyService) transformBoundsWithSampling(bounds TileBounds, transform CoordTransformFunc) TileBounds {
	// 在边界上采样多个点，以更准确地计算转换后的边界
	const samples = 10

	var minLon, maxLon, minLat, maxLat float64
	first := true

	// 采样边界上的点
	for i := 0; i <= samples; i++ {
		t := float64(i) / float64(samples)

		// 上边
		lon, lat := transform(
			bounds.MinLon+(bounds.MaxLon-bounds.MinLon)*t,
			bounds.MaxLat,
		)
		if first {
			minLon, maxLon, minLat, maxLat = lon, lon, lat, lat
			first = false
		} else {
			minLon, maxLon = minFloat(minLon, lon), maxFloat(maxLon, lon)
			minLat, maxLat = minFloat(minLat, lat), maxFloat(maxLat, lat)
		}

		// 下边
		lon, lat = transform(
			bounds.MinLon+(bounds.MaxLon-bounds.MinLon)*t,
			bounds.MinLat,
		)
		minLon, maxLon = minFloat(minLon, lon), maxFloat(maxLon, lon)
		minLat, maxLat = minFloat(minLat, lat), maxFloat(maxLat, lat)

		// 左边
		lon, lat = transform(
			bounds.MinLon,
			bounds.MinLat+(bounds.MaxLat-bounds.MinLat)*t,
		)
		minLon, maxLon = minFloat(minLon, lon), maxFloat(maxLon, lon)
		minLat, maxLat = minFloat(minLat, lat), maxFloat(maxLat, lat)

		// 右边
		lon, lat = transform(
			bounds.MaxLon,
			bounds.MinLat+(bounds.MaxLat-bounds.MinLat)*t,
		)
		minLon, maxLon = minFloat(minLon, lon), maxFloat(maxLon, lon)
		minLat, maxLat = minFloat(minLat, lat), maxFloat(maxLat, lat)
	}

	return TileBounds{
		MinLon: minLon,
		MaxLon: maxLon,
		MinLat: minLat,
		MaxLat: maxLat,
	}
}

// clampTileRange 限制瓦片范围在有效范围内
func (s *TileProxyService) clampTileRange(tr TileRange, maxIndex int) TileRange {
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

// fetchTilesParallel 并发获取瓦片
func (s *TileProxyService) fetchTilesParallel(ctx context.Context, netMap *models.NetMap, z int, tileRange TileRange) []FetchedTile {
	tileCount := (tileRange.MaxX - tileRange.MinX + 1) * (tileRange.MaxY - tileRange.MinY + 1)
	if tileCount <= 0 {
		return nil
	}

	var wg sync.WaitGroup
	tileChan := make(chan FetchedTile, tileCount)

	// 限制并发数
	semaphore := make(chan struct{}, 10)

	for x := tileRange.MinX; x <= tileRange.MaxX; x++ {
		for y := tileRange.MinY; y <= tileRange.MaxY; y++ {
			wg.Add(1)
			go func(tileX, tileY int) {
				defer wg.Done()

				semaphore <- struct{}{}
				defer func() { <-semaphore }()

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

	go func() {
		wg.Wait()
		close(tileChan)
	}()

	var tiles []FetchedTile
	for tile := range tileChan {
		tiles = append(tiles, tile)
	}

	return tiles
}

// buildTileURL 构建瓦片URL
func (s *TileProxyService) buildTileURL(netMap *models.NetMap, z, x, y int) string {
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

// fetchTile 获取单个瓦片
func (s *TileProxyService) fetchTile(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request failed: %v", err)
	}

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
	c.Header("Cache-Control", "public, max-age=86400")
	c.Header("Access-Control-Allow-Origin", "*")
	c.Data(http.StatusOK, contentType, data)
}

// 辅助函数
func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
