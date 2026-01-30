package routers

import (
	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/services"
	"github.com/GrainArc/SouceMap/tile_proxy"
	"github.com/GrainArc/SouceMap/views"
	"github.com/gin-gonic/gin"
	"os"
	"path/filepath"
)

func GeoRouters(r *gin.Engine) {
	UserController := &views.UserController{}
	homeDir, _ := os.UserHomeDir()
	OutFilePath := filepath.Join(homeDir, "BoundlessMap", "OutFile")
	mapRouter := r.Group("/geo")
	{
		mapRouter.GET(":tablename/:z/:x/:y.pbf", UserController.OutMVT)
		mapRouter.GET("/TileSizeChange", UserController.TileSizeChange)
		mapRouter.GET("/GetColorSet", UserController.GetColorSet)
		mapRouter.GET("/GetSchema", UserController.GetSchema)
		mapRouter.GET("/SchemaToExcel", UserController.SchemaToExcel)
		mapRouter.GET("/GetSchemaByUnits", UserController.GetSchemaByUnits)
		mapRouter.POST("/AddUpdateColorSet", UserController.AddUpdateColorSet)
		mapRouter.POST("/SearchGeoFromSchema", UserController.SearchGeoFromSchema)
		mapRouter.POST("/ShowSingleGeoByXY", UserController.ShowSingleGeoByXY)
		mapRouter.POST("/ShowGeoByBox", UserController.ShowGeoByBox)
		mapRouter.GET("/ShowSingleGeo", UserController.ShowSingleGeo)
		mapRouter.POST("/SpaceIntersect", UserController.SpaceIntersect)
		mapRouter.GET("/GetTableAttributes", UserController.GetTableAttributes)
		mapRouter.POST("/Area", UserController.Area)
		mapRouter.POST("/GeodesicArea", UserController.GeodesicArea)
		mapRouter.POST("/AddSchema", UserController.AddSchema)
		mapRouter.POST("/GetGDBMetadata", UserController.GetGDBMetadata)
		mapRouter.GET("/DelSchema", UserController.DelSchema)
		mapRouter.POST("/ChangeSchema", UserController.ChangeSchema)
		mapRouter.POST("/ChangeLayerStyle", UserController.ChangeLayerStyle)
		mapRouter.POST("/GetExcavationFillVolume", UserController.GetExcavationFillVolume)
		mapRouter.POST("/GetHeightFromDEM", UserController.GetHeightFromDEM)
		mapRouter.Static("/OutFile", OutFilePath)
		mapRouter.POST("/OutIntersect", UserController.OutIntersect)
		mapRouter.GET("/OutLayer", UserController.OutLayer)
		// 在路由注册文件中添加
		mapRouter.GET("/GetDirectoryTree", UserController.GetDirectoryTree)
		mapRouter.POST("/UpdateLayer", UserController.UpdateLayer)
		mapRouter.POST("/AppendLayer", UserController.AppendLayer)
		mapRouter.GET("/GetUpdateMSG", UserController.GetUpdateMSG)
		mapRouter.POST("/DownloadSearchGeoFromSchema", UserController.DownloadSearchGeoFromSchema)
		mapRouter.GET("/GetLayerExtent", UserController.GetLayerExtent)
		mapRouter.POST("/GetTablePropertyValues", UserController.GetTablePropertyValues)

	}
	editRouter := r.Group("/edit")
	{
		editRouter.POST("/LineOnPolygonOverlay", UserController.LineOnPolygonOverlay)
		editRouter.POST("/OffsetFeature", UserController.OffsetFeature)
		editRouter.POST("/DeAggregatorFeature", UserController.ExplodeFeature)
		editRouter.POST("/AreaOnAreaAnalysis", UserController.AreaOnAreaAnalysis)
		editRouter.POST("/SplitFeature", UserController.SplitFeature)
		editRouter.POST("/DissolveFeature", UserController.DissolveFeature)
		editRouter.POST("/DonutBuilder", UserController.DonutBuilder)
		editRouter.POST("/AggregatorFeature", UserController.AggregatorFeature)
		editRouter.POST("/Capture", UserController.Capture)
		editRouter.POST("/AutoPolygon", UserController.AutoPolygon)
		editRouter.POST("/SplitGeo", UserController.SplitGeo)
		editRouter.POST("/DissolverGeo", UserController.DissolverGeo)
		editRouter.GET("/GetReatoreFile", UserController.GetReatoreFile)
		editRouter.POST("/ImportPGToGDBHandler", UserController.ImportPGToGDBHandler)
		editRouter.GET("/GetChangeRecord", UserController.GetChangeRecord)
		editRouter.GET("/DelChangeRecord", UserController.DelChangeRecord)
		editRouter.POST("/ChangeGeoToSchema", UserController.ChangeGeoToSchema)
		editRouter.GET("/BackUpRecord", UserController.BackUpRecord)
		editRouter.GET("/SyncToFile", UserController.SyncToFile)
		editRouter.POST("/GetGeoFromSchema", UserController.GetGeoFromSchema)
		editRouter.POST("/AddGeoToSchema", UserController.AddGeoToSchema)
		editRouter.POST("/DelGeoToSchema", UserController.DelGeoToSchema)
		editRouter.POST("/DelGeosToSchema", UserController.DelGeosToSchema)
	}
	tempLayerRouter := r.Group("/temp_layer")
	{
		tempLayerRouter.POST("/ShowTempLayerHeader", UserController.ShowTempLayerHeader)
		tempLayerRouter.GET("/DelTempLayer", UserController.DelTempLayer)
		tempLayerRouter.GET("/DelTempGeo", UserController.DelTempGeo)
		tempLayerRouter.POST("/ShowTempGeoList", UserController.ShowTempGeoList)
		tempLayerRouter.POST("/SaveTempGeo", UserController.InTempGeo)
		tempLayerRouter.GET("/ShowTempGeo", UserController.ShowTempGeo)
		tempLayerRouter.GET("/DownloadTempLayer", UserController.DownloadTempLayer)
		tempLayerRouter.POST("/InTempLayer", UserController.InTempLayer)
		tempLayerRouter.POST("/ShowTempLayer", UserController.ShowTempLayer)
	}
	ShareRouter := r.Group("/share")
	{
		ShareRouter.GET("/GetEncryptedDSN", UserController.GetEncryptedDSN)
		ShareRouter.GET("/GetDeviceName", UserController.GetDeviceName)
		ShareRouter.GET("/GetAllDeviceName", UserController.GetAllDeviceName)
		ShareRouter.POST("/UpdateDevice", UserController.UpdateDevice)
		mapRouter.GET("/RestoreOfflineLayer", UserController.RestoreOfflineLayer)
		ShareRouter.POST("/ChangeDeviceName", UserController.ChangeDeviceName)
		mapRouter.GET("/DownloadOfflineLayer", UserController.DownloadOfflineLayer)
	}
	SurveyRouter := r.Group("/Survey")
	PICPath := filepath.Join(homeDir, "BoundlessMap", "PIC")
	{
		SurveyRouter.Static("/PIC", PICPath)
		SurveyRouter.POST("/MsgUpload", UserController.MsgUpload)
		SurveyRouter.POST("/PicUpload", UserController.PicUpload)
		SurveyRouter.POST("/ZDTUpload", UserController.ZDTUpload)
		SurveyRouter.GET("/PicDelete", UserController.PicDel)
		SurveyRouter.GET("/SurveyDataGet", UserController.SurveyDataGet)
	}
	StaticRouter := r.Group("/raster")
	{
		StaticRouter.GET("/GetRasterName", UserController.GetRasterName)
		StaticRouter.GET(":dbname/:z/:x/:y.png", UserController.Raster)
	}
	DynamicRouter := r.Group("/raster/dynamic")
	{
		// 服务管理
		DynamicRouter.POST("/service", UserController.CreateDynamicRasterService)         // 创建服务
		DynamicRouter.GET("/services", UserController.ListDynamicRasterServices)          // 列出所有服务
		DynamicRouter.GET("/service/:name", UserController.GetDynamicRasterService)       // 获取服务信息
		DynamicRouter.PUT("/service/:name", UserController.UpdateDynamicRasterService)    // 更新服务
		DynamicRouter.DELETE("/service/:name", UserController.DeleteDynamicRasterService) // 删除服务
		// 服务控制
		DynamicRouter.POST("/service/:name/start", UserController.StartDynamicRasterService)     // 启动服务
		DynamicRouter.POST("/service/:name/stop", UserController.StopDynamicRasterService)       // 停止服务
		DynamicRouter.POST("/service/:name/refresh", UserController.RefreshDynamicRasterService) // 刷新服务
		// TileJSON
		DynamicRouter.GET("/tilejson/:name", UserController.GetDynamicRasterTileJSON)
		// 瓦片接口
		DynamicRouter.GET("/tile/:name/:z/:x/:y.png", UserController.GetDynamicRasterTile)
		DynamicRouter.GET("/terrain/:name/:z/:x/:y.png", UserController.GetDynamicTerrainTile)
	}
	DemRouter := r.Group("/dem")
	{
		DemRouter.GET("/:z/:x/:y.webp", UserController.Dem)
	}
	Tile3DRouter := r.Group("/tiles")
	{
		Tile3DRouter.Static("/Tile", config.Tiles3d)
		Tile3DRouter.GET("/GetTilesName", UserController.GetTilesName)

		Tile3DRouter.GET(":dbname/tileset.json", UserController.TileSetGet)

		Tile3DRouter.GET(":dbname/:folder/:name", UserController.Tiles3DJson)
	}
	FontRouter := r.Group("/resource")
	{
		FontRouter.GET("/fonts/:fontstack/:range", UserController.FontGet)
	}
	AttRouter := r.Group("/att")
	{
		AttRouter.GET("/GetCESet", UserController.GetCESet)
		AttRouter.POST("/AddUpdateCESet", UserController.AddUpdateCESet)
	}
	fields := r.Group("/fields")
	{
		fields.POST("/AddField", UserController.AddField)       // 添加字段
		fields.POST("/DeleteField", UserController.DeleteField) // 删除字段
		fields.GET("/types", UserController.GetSupportedFieldTypes)
		fields.POST("/CalculateField", UserController.CalculateField)           // 执行计算
		fields.POST("/UpdateGeometryField", UserController.UpdateGeometryField) // 预览结果
		fields.GET("/GetFieldInfo", UserController.GetFieldInfo)                // 获取单个字段信息
		fields.POST("/LayerStatistics", UserController.LayerStatistics)

	}
	report := r.Group("/report")
	{
		report.POST("/SaveReportConfig", UserController.SaveReportConfig)     // 新增报告
		report.POST("/UpdateReportConfig", UserController.UpdateReportConfig) // 更新报告
		report.POST("/GenerateReport", UserController.GenerateReport)         // 制作报告
		report.GET("/GetReportConfig", UserController.GetReportConfig)        // 获取报告配置
		report.POST("/ListReportConfigs", UserController.ListReportConfigs)   // 查询报告类型
		report.GET("/DeleteReportConfig", UserController.DeleteReportConfig)
		report.GET("/SyncReport", UserController.SyncReport)

	}
	mxd := r.Group("/mxd")
	{
		mxd.POST("/AddUpdateLayerMXD", UserController.AddUpdateLayerMXD)
		mxd.GET("/GetLayerMXDList", UserController.GetLayerMXDList)
		mxd.GET("/GetLayerMXDHeaderList", UserController.GetLayerMXDHeaderList)

		mxd.GET("/DelLayerMXD", UserController.DelLayerMXD)
		mxd.GET("/SyncLayerMXD", UserController.SyncLayerMXD)
		mxd.GET("/GetTLImg", UserController.GetTLImg)

	}
	fileService := services.NewFileService(config.MainConfig.RootPath)
	fileController := services.NewFileController(fileService)

	// 文件相关路由组
	fileGroup := r.Group("/api/files")
	{
		// 获取目录内容（懒加载）
		fileGroup.GET("/list", fileController.GetDirectoryContent)

		// 获取根目录路径
		fileGroup.GET("/root", fileController.GetRootPath)
	}

	// 创建处理器
	textureHandler := views.NewTextureHandler()

	// API路由组
	api := r.Group("/textures")
	{
		//上传纹理
		api.POST("/upload", textureHandler.Upload)
		//获取纹理列表
		api.GET("/list", textureHandler.List)
		// 获取纹理详情（含Base64数据）
		api.GET("/:id", textureHandler.Get)
		//  获取原始PNG图片
		api.GET("/:id/image", textureHandler.GetImage)
		// DELETE删除纹理
		api.DELETE("/:id", textureHandler.Delete)
		api.GET("/search", textureHandler.Search)

		api.POST("/set_layer_texture", textureHandler.SetLayerTexture)
		api.GET("/get_layer_texture", textureHandler.GetLayerTexture)
		api.GET("/get_used_textures", textureHandler.GetUsedTextures)

	}
	symbolHandler := views.NewSymbolHandler()

	// API路由组
	api2 := r.Group("/symbols")
	{
		// 上传图标
		api2.POST("/upload", symbolHandler.Upload)
		// 批量上传图标
		api2.POST("/batch_upload", symbolHandler.BatchUpload)
		// 获取图标列表
		api2.GET("/list", symbolHandler.List)
		// 搜索图标
		api2.GET("/search", symbolHandler.Search)
		// 获取所有分类
		api2.GET("/categories", symbolHandler.GetCategories)
		// 获取图标详情（含Base64数据）
		api2.GET("/:id", symbolHandler.Get)
		// 获取原始图片
		api2.GET("/:id/image", symbolHandler.GetImage)
		// 更新图标信息
		api2.PUT("/:id", symbolHandler.Update)
		// 删除图标
		api2.DELETE("/:id", symbolHandler.Delete)

		// 图层图标配置
		api2.POST("/set_layer_symbol", symbolHandler.SetLayerSymbol)
		api2.GET("/get_layer_symbol", symbolHandler.GetLayerSymbol)
		api2.GET("/get_used_symbols", symbolHandler.GetUsedSymbols)
	}
	tileProxyService := tile_proxy.NewTileProxyService()
	NewWebTileHandler := tile_proxy.NewWebTileHandler(config.MainConfig.Download)
	api3 := r.Group("/network_map")
	{
		tileProxyService.RegisterRoutes(api3)
		NewWebTileHandler.RegisterRoutes(api3)
		api3.POST("/CreateNetMap", UserController.CreateNetMap)
		api3.GET("/ListNetMaps", UserController.ListNetMaps)
		api3.GET("/:id", UserController.GetNetMapByID)
		api3.PUT("/:id", UserController.UpdateNetMap)
		api3.DELETE("/:id", UserController.DeleteNetMap)
		api3.POST("/batch-delete", UserController.BatchDeleteNetMaps)
	}
	api4 := r.Group("/wmts")
	{
		// 新增 WMTS 路由
		api4.POST("/publish", UserController.PublishWMTS)                // 发布 WMTS 服务
		api4.GET("/:layername/:z/:x/:y.png", UserController.GetWMTSTile) // 获取瓦片
		api4.GET("/style", UserController.UpdateWMTSStyle)               // 更新样式
		api4.DELETE("/:layername", UserController.UnpublishWMTS)
		api4.DELETE("/:layername/cache", UserController.ClearWMTSCache)       // 清空缓存
		api4.GET("/:layername/cache/stats", UserController.GetWMTSCacheStats) // 缓存统计// 注销服务
	}
}
