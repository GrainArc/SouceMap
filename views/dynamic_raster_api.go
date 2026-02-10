// views/dynamic_raster_api.go
package views

import (
	"fmt"
	"github.com/GrainArc/SouceMap/services"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/GrainArc/SouceMap/models"
	"github.com/gin-gonic/gin"
)

// ============================================
// 请求/响应结构体
// ============================================

// CreateServiceRequest 创建服务请求
type CreateServiceRequest struct {
	Name        string `json:"name" binding:"required"`       // 服务名称
	ImagePath   string `json:"image_path" binding:"required"` // 影像路径
	Description string `json:"description"`                   // 描述
	TileSize    int    `json:"tile_size"`                     // 瓦片大小
	PoolSize    int    `json:"pool_size"`                     // 连接池大小
	MinZoom     int    `json:"min_zoom"`                      // 最小缩放
	MaxZoom     int    `json:"max_zoom"`                      // 最大缩放
	ServiceType string `json:"service_type"`                  // 服务类型: raster/terrain
	Encoding    string `json:"encoding"`                      // 地形编码: mapbox/terrarium
	AutoStart   bool   `json:"auto_start"`                    // 是否自动启动
}

// UpdateServiceRequest 更新服务请求
type UpdateServiceRequest struct {
	ImagePath   string `json:"image_path"`
	Description string `json:"description"`
	TileSize    int    `json:"tile_size"`
	PoolSize    int    `json:"pool_size"`
	MinZoom     int    `json:"min_zoom"`
	MaxZoom     int    `json:"max_zoom"`
	ServiceType string `json:"service_type"`
	Encoding    string `json:"encoding"`
}

// ServiceInfoResponse 服务信息响应
type ServiceInfoResponse struct {
	ID          uint      `json:"id"`
	Name        string    `json:"name"`
	ImagePath   string    `json:"image_path"`
	Description string    `json:"description"`
	TileSize    int       `json:"tile_size"`
	PoolSize    int       `json:"pool_size"`
	MinZoom     int       `json:"min_zoom"`
	MaxZoom     int       `json:"max_zoom"`
	Bounds      []float64 `json:"bounds"`
	Center      []float64 `json:"center"`
	ServiceType string    `json:"service_type"`
	Encoding    string    `json:"encoding"`
	Status      int       `json:"status"`
	StatusText  string    `json:"status_text"`
	ErrorMsg    string    `json:"error_msg,omitempty"`
	TileURL     string    `json:"tile_url"`
	CreatedAt   string    `json:"created_at"`
	UpdatedAt   string    `json:"updated_at"`
}

// TileJSON TileJSON规范响应
type TileJSON struct {
	TileJSON    string    `json:"tilejson"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Version     string    `json:"version"`
	Attribution string    `json:"attribution"`
	Scheme      string    `json:"scheme"`
	Tiles       []string  `json:"tiles"`
	MinZoom     int       `json:"minzoom"`
	MaxZoom     int       `json:"maxzoom"`
	Bounds      []float64 `json:"bounds"`
	Center      []float64 `json:"center"`
}

// ============================================
// 服务管理接口
// ============================================

// CreateDynamicRasterService 创建动态栅格服务
func (uc *UserController) CreateDynamicRasterService(c *gin.Context) {
	var req CreateServiceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request: " + err.Error()})
		return
	}

	// 检查文件是否存在
	if _, err := os.Stat(req.ImagePath); os.IsNotExist(err) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "image file not found: " + req.ImagePath})
		return
	}

	// 创建服务配置
	raster := &models.DynamicRaster{
		Name:        req.Name,
		ImagePath:   req.ImagePath,
		Description: req.Description,
		TileSize:    req.TileSize,
		PoolSize:    req.PoolSize,
		MinZoom:     req.MinZoom,
		MaxZoom:     req.MaxZoom,
		ServiceType: req.ServiceType,
		Encoding:    req.Encoding,
		Status:      0,
	}

	// 设置默认值
	if raster.TileSize <= 0 {
		raster.TileSize = 256
	}
	if raster.PoolSize <= 0 {
		raster.PoolSize = 4
	}
	if raster.MaxZoom <= 0 {
		raster.MaxZoom = 18
	}
	if raster.ServiceType == "" {
		raster.ServiceType = "raster"
	}
	if raster.Encoding == "" {
		raster.Encoding = "mapbox"
	}

	manager := services.GetTileServerManager()
	if err := manager.AddService(raster); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 如果需要自动启动
	if req.AutoStart {
		if err := manager.StartServer(req.Name); err != nil {
			c.JSON(http.StatusOK, gin.H{
				"message": "service created but failed to start",
				"error":   err.Error(),
				"name":    req.Name,
			})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "service created successfully",
		"name":    req.Name,
	})
}

// ListDynamicRasterServices 列出所有动态栅格服务
func (uc *UserController) ListDynamicRasterServices(c *gin.Context) {
	manager := services.GetTileServerManager()
	services, err := manager.ListServices()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// 获取请求的host用于构建URL
	scheme := "http"
	if c.Request.TLS != nil {
		scheme = "https"
	}
	host := c.Request.Host

	var response []ServiceInfoResponse
	for _, svc := range services {
		resp := buildServiceInfoResponse(&svc, scheme, host)
		response = append(response, resp)
	}

	c.JSON(http.StatusOK, response)
}

// GetDynamicRasterService 获取单个服务信息
func (uc *UserController) GetDynamicRasterService(c *gin.Context) {
	name := c.Param("name")

	manager := services.GetTileServerManager()
	svc, err := manager.GetServiceConfig(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	scheme := "http"
	if c.Request.TLS != nil {
		scheme = "https"
	}
	host := c.Request.Host

	response := buildServiceInfoResponse(svc, scheme, host)
	c.JSON(http.StatusOK, response)
}

// UpdateDynamicRasterService 更新服务配置
func (uc *UserController) UpdateDynamicRasterService(c *gin.Context) {
	name := c.Param("name")

	var req UpdateServiceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request: " + err.Error()})
		return
	}

	// 构建更新字段
	updates := make(map[string]interface{})
	if req.ImagePath != "" {
		// 检查新文件是否存在
		if _, err := os.Stat(req.ImagePath); os.IsNotExist(err) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "image file not found: " + req.ImagePath})
			return
		}
		updates["image_path"] = req.ImagePath
	}
	if req.Description != "" {
		updates["description"] = req.Description
	}
	if req.TileSize > 0 {
		updates["tile_size"] = req.TileSize
	}
	if req.PoolSize > 0 {
		updates["pool_size"] = req.PoolSize
	}
	if req.MinZoom >= 0 {
		updates["min_zoom"] = req.MinZoom
	}
	if req.MaxZoom > 0 {
		updates["max_zoom"] = req.MaxZoom
	}
	if req.ServiceType != "" {
		updates["service_type"] = req.ServiceType
	}
	if req.Encoding != "" {
		updates["encoding"] = req.Encoding
	}

	manager := services.GetTileServerManager()
	if err := manager.UpdateService(name, updates); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "service updated successfully"})
}

// DeleteDynamicRasterService 删除服务
func (uc *UserController) DeleteDynamicRasterService(c *gin.Context) {
	name := c.Param("name")

	manager := services.GetTileServerManager()
	if err := manager.DeleteService(name); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "service deleted successfully"})
}

// StartDynamicRasterService 启动服务
func (uc *UserController) StartDynamicRasterService(c *gin.Context) {
	name := c.Param("name")

	manager := services.GetTileServerManager()
	if err := manager.StartServer(name); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "service started successfully"})
}

// StopDynamicRasterService 停止服务
func (uc *UserController) StopDynamicRasterService(c *gin.Context) {
	name := c.Param("name")

	manager := services.GetTileServerManager()
	manager.StopServer(name)

	c.JSON(http.StatusOK, gin.H{"message": "service stopped successfully"})
}

// RefreshDynamicRasterService 刷新服务
func (uc *UserController) RefreshDynamicRasterService(c *gin.Context) {
	name := c.Param("name")

	manager := services.GetTileServerManager()
	if err := manager.RefreshService(name); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "service refreshed successfully"})
}

// GetDynamicRasterTileJSON 获取TileJSON
func (uc *UserController) GetDynamicRasterTileJSON(c *gin.Context) {
	name := c.Param("name")

	manager := services.GetTileServerManager()
	svc, err := manager.GetServiceConfig(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	scheme := "http"
	if c.Request.TLS != nil {
		scheme = "https"
	}
	host := c.Request.Host

	// 解析边界和中心点
	var bounds []float64
	var center []float64

	if svc.Bounds != "" {
		minLon, minLat, maxLon, maxLat, _ := services.ParseBounds(svc.Bounds)
		bounds = []float64{minLon, minLat, maxLon, maxLat}
	} else {
		bounds = []float64{-180, -85, 180, 85}
	}

	if svc.Center != "" {
		lon, lat, _ := services.ParseCenter(svc.Center)
		center = []float64{lon, lat, float64((svc.MinZoom + svc.MaxZoom) / 2)}
	} else {
		center = []float64{0, 0, 10}
	}

	// 构建瓦片URL
	tileURL := fmt.Sprintf("%s://%s/raster/dynamic/tile/%s/{z}/{x}/{y}.png", scheme, host, name)

	tileJSON := TileJSON{
		TileJSON:    "2.2.0",
		Name:        svc.Name,
		Description: svc.Description,
		Version:     "1.0.0",
		Attribution: "",
		Scheme:      "xyz",
		Tiles:       []string{tileURL},
		MinZoom:     svc.MinZoom,
		MaxZoom:     svc.MaxZoom,
		Bounds:      bounds,
		Center:      center,
	}

	c.JSON(http.StatusOK, tileJSON)
}

// ============================================
// 瓦片获取接口
// ============================================

// GetDynamicRasterTile 获取动态栅格瓦片（修改）
func (uc *UserController) GetDynamicRasterTile(c *gin.Context) {
	name := c.Param("name")
	z, err := strconv.Atoi(c.Param("z"))
	if err != nil {
		c.String(http.StatusBadRequest, "invalid z parameter")
		return
	}
	x, err := strconv.Atoi(c.Param("x"))
	if err != nil {
		c.String(http.StatusBadRequest, "invalid x parameter")
		return
	}

	y, err := strconv.Atoi(strings.TrimSuffix(c.Param("y.png"), ".png"))
	if err != nil {
		c.String(http.StatusBadRequest, "invalid y parameter")
		return
	}

	manager := services.GetTileServerManager()
	server, config, err := manager.GetServer(name)
	if err != nil {
		c.String(http.StatusNotFound, "service not found: %s", err.Error())
		return
	}

	// 检查缩放级别
	if z < config.MinZoom || z > config.MaxZoom {
		c.String(http.StatusBadRequest, "zoom level out of range")
		return
	}

	// === 缓存查询 ===
	tileType := "raster"
	encoding := ""
	if config.ServiceType == "terrain" {
		tileType = "terrain"
		encoding = config.Encoding
	}

	cacheService := services.GetTileCacheService()
	if cacheService != nil {
		if cachedData, found, err := cacheService.GetCachedTile(name, z, x, y, tileType, encoding); err == nil && found {
			c.Header("Content-Type", "image/png")
			c.Header("Cache-Control", "public, max-age=86400")
			c.Header("Access-Control-Allow-Origin", "*")
			c.Header("X-Tile-Cache", "HIT")
			c.Data(http.StatusOK, "image/png", cachedData)
			return
		}
	}

	// === 动态切片 ===
	var tileData []byte
	if config.ServiceType == "terrain" {
		tileData, err = server.GetTerrainTile(z, x, y, config.Encoding)
	} else {
		tileData, err = server.GetTile(z, x, y)
	}

	if err != nil {
		c.String(http.StatusInternalServerError, "failed to get tile: %s", err.Error())
		return
	}

	// === 异步写入缓存 ===
	if cacheService != nil {
		go func(svcName string, tz, tx, ty int, tt, te string, data []byte) {
			if err := cacheService.SetCachedTile(svcName, tz, tx, ty, tt, te, data); err != nil {
				fmt.Printf("[WARN] failed to cache tile %s/%d/%d/%d: %v\n", svcName, tz, tx, ty, err)
			}
		}(name, z, x, y, tileType, encoding, tileData)
	}

	// 设置响应头
	c.Header("Content-Type", "image/png")
	c.Header("Cache-Control", "public, max-age=86400")
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("X-Tile-Cache", "MISS")
	c.Data(http.StatusOK, "image/png", tileData)
}

// GetDynamicTerrainTile 获取地形瓦片（修改）
func (uc *UserController) GetDynamicTerrainTile(c *gin.Context) {
	name := c.Param("name")
	z, _ := strconv.Atoi(c.Param("z"))
	x, _ := strconv.Atoi(c.Param("x"))
	yStr := strings.TrimSuffix(c.Param("y"), ".png")
	y, _ := strconv.Atoi(yStr)
	encoding := c.DefaultQuery("encoding", "mapbox")

	manager := services.GetTileServerManager()
	server, config, err := manager.GetServer(name)
	if err != nil {
		c.String(http.StatusNotFound, "service not found: %s", err.Error())
		return
	}

	// 检查缩放级别
	if z < config.MinZoom || z > config.MaxZoom {
		c.String(http.StatusBadRequest, "zoom level out of range")
		return
	}

	// === 缓存查询 ===
	tileType := "terrain"
	cacheService := services.GetTileCacheService()
	if cacheService != nil {
		if cachedData, found, err := cacheService.GetCachedTile(name, z, x, y, tileType, encoding); err == nil && found {
			c.Header("Content-Type", "image/png")
			c.Header("Cache-Control", "public, max-age=86400")
			c.Header("Access-Control-Allow-Origin", "*")
			c.Header("X-Tile-Cache", "HIT")
			c.Data(http.StatusOK, "image/png", cachedData)
			return
		}
	}

	// === 动态切片 ===
	tileData, err := server.GetTerrainTile(z, x, y, encoding)
	if err != nil {
		c.String(http.StatusInternalServerError, "failed to get terrain tile: %s", err.Error())
		return
	}

	// === 异步写入缓存 ===
	if cacheService != nil {
		go func(svcName string, tz, tx, ty int, tt, te string, data []byte) {
			if err := cacheService.SetCachedTile(svcName, tz, tx, ty, tt, te, data); err != nil {
				fmt.Printf("[WARN] failed to cache terrain tile %s/%d/%d/%d: %v\n", svcName, tz, tx, ty, err)
			}
		}(name, z, x, y, tileType, encoding, tileData)
	}

	c.Header("Content-Type", "image/png")
	c.Header("Cache-Control", "public, max-age=86400")
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("X-Tile-Cache", "MISS")
	c.Data(http.StatusOK, "image/png", tileData)
}

// ============================================
// 辅助函数
// ============================================

// buildServiceInfoResponse 构建服务信息响应
func buildServiceInfoResponse(svc *models.DynamicRaster, scheme, host string) ServiceInfoResponse {
	var bounds []float64
	var center []float64

	if svc.Bounds != "" {
		minLon, minLat, maxLon, maxLat, _ := services.ParseBounds(svc.Bounds)
		bounds = []float64{minLon, minLat, maxLon, maxLat}
	}

	if svc.Center != "" {
		lon, lat, _ := services.ParseCenter(svc.Center)
		center = []float64{lon, lat}
	}

	statusText := "stopped"
	switch svc.Status {
	case 1:
		statusText = "running"
	case 2:
		statusText = "error"
	}

	tileURL := fmt.Sprintf("%s://%s/raster/dynamic/tile/%s/{z}/{x}/{y}.png", scheme, host, svc.Name)

	return ServiceInfoResponse{
		ID:          svc.ID,
		Name:        svc.Name,
		ImagePath:   svc.ImagePath,
		Description: svc.Description,
		TileSize:    svc.TileSize,
		PoolSize:    svc.PoolSize,
		MinZoom:     svc.MinZoom,
		MaxZoom:     svc.MaxZoom,
		Bounds:      bounds,
		Center:      center,
		ServiceType: svc.ServiceType,
		Encoding:    svc.Encoding,
		Status:      svc.Status,
		StatusText:  statusText,
		ErrorMsg:    svc.ErrorMsg,
		TileURL:     tileURL,
		CreatedAt:   svc.CreatedAt.Format("2006-01-02 15:04:05"),
		UpdatedAt:   svc.UpdatedAt.Format("2006-01-02 15:04:05"),
	}
}
