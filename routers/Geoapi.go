package routers

import (
	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/services"
	"github.com/GrainArc/SouceMap/views"
	"github.com/gin-gonic/gin"
)

func GeoRouters(r *gin.Engine) {
	UserController := &views.UserController{}
	mapRouter := r.Group("/geo")
	{
		mapRouter.GET(":tablename/:z/:x/:y.pbf", UserController.OutMVT)
		mapRouter.GET("/TileSizeChange", UserController.TileSizeChange)
		mapRouter.GET("/GetColorSet", UserController.GetColorSet)
		mapRouter.GET("/GetSchema", UserController.GetSchema)
		mapRouter.GET("/SchemaToExcel", UserController.SchemaToExcel)
		mapRouter.GET("/GetSchemaByUnits", UserController.GetSchemaByUnits)
		mapRouter.POST("/AddUpdateColorSet", UserController.AddUpdateColorSet)
		mapRouter.GET("/GetChangeRecord", UserController.GetChangeRecord)
		mapRouter.GET("/DelChangeRecord", UserController.DelChangeRecord)
		mapRouter.GET("/BackUpRecord", UserController.BackUpRecord)
		mapRouter.GET("/GetAllDeviceName", UserController.GetAllDeviceName)
		mapRouter.POST("/UpdateDevice", UserController.UpdateDevice)
		mapRouter.POST("/GetGeoFromSchema", UserController.GetGeoFromSchema)
		mapRouter.POST("/AddGeoToSchema", UserController.AddGeoToSchema)
		mapRouter.POST("/DelGeoToSchema", UserController.DelGeoToSchema)

		mapRouter.POST("/SearchGeoFromSchema", UserController.SearchGeoFromSchema)

		mapRouter.POST("/SaveTempGeo", UserController.InTempGeo)
		mapRouter.GET("/ShowTempGeo", UserController.ShowTempGeo)
		mapRouter.POST("/DownloadTempGeo", UserController.DownloadTempGeo)
		mapRouter.POST("/DownloadTempGeoALL", UserController.DownloadTempGeoALL)
		mapRouter.GET("/DownloadTempLayer", UserController.DownloadTempLayer)
		mapRouter.POST("/Capture", UserController.Capture)
		mapRouter.POST("/AutoPolygon", UserController.AutoPolygon)
		mapRouter.POST("/SplitGeo", UserController.SplitGeo)
		mapRouter.POST("/DissolverGeo", UserController.DissolverGeo)
		mapRouter.POST("/InTempLayer", UserController.InTempLayer)
		mapRouter.POST("/ShowTempLayer", UserController.ShowTempLayer)
		mapRouter.POST("/ShowSingleGeoByXY", UserController.ShowSingleGeoByXY)
		mapRouter.POST("/ShowGeoByBox", UserController.ShowGeoByBox)

		mapRouter.GET("/ShowSingleGeo", UserController.ShowSingleGeo)

		mapRouter.POST("/ShowTempLayerHeader", UserController.ShowTempLayerHeader)
		mapRouter.GET("/DelTempLayer", UserController.DelTempLayer)
		mapRouter.GET("/DelTempGeo", UserController.DelTempGeo)
		mapRouter.POST("/ShowTempGeoList", UserController.ShowTempGeoList)
		mapRouter.POST("/SpaceIntersect", UserController.SpaceIntersect)

		mapRouter.GET("/GetTableAttributes", UserController.GetTableAttributes)
		mapRouter.GET("/GetDeviceName", UserController.GetDeviceName)
		mapRouter.POST("/ChangeDeviceName", UserController.ChangeDeviceName)
		mapRouter.POST("/Area", UserController.Area)
		mapRouter.POST("/GeodesicArea", UserController.GeodesicArea)
		mapRouter.POST("/AddSchema", UserController.AddSchema)
		mapRouter.GET("/DelSchema", UserController.DelSchema)
		mapRouter.POST("/ChangeSchema", UserController.ChangeSchema)
		mapRouter.POST("/ChangeLayerStyle", UserController.ChangeLayerStyle)
		mapRouter.POST("/GetExcavationFillVolume", UserController.GetExcavationFillVolume)
		mapRouter.POST("/GetHeightFromDEM", UserController.GetHeightFromDEM)
		mapRouter.Static("/OutFile", "./OutFile")
		mapRouter.POST("/OutIntersect", UserController.OutIntersect)
		mapRouter.GET("/OutLayer", UserController.OutLayer)
		// 在路由注册文件中添加
		mapRouter.GET("/GetDirectoryTree", UserController.GetDirectoryTree)

		mapRouter.POST("/UpdateLayer", UserController.UpdateLayer)
		mapRouter.POST("/AppendLayer", UserController.AppendLayer)
		mapRouter.GET("/GetUpdateMSG", UserController.GetUpdateMSG)
		mapRouter.GET("/DownloadOfflineLayer", UserController.DownloadOfflineLayer)
		mapRouter.POST("/DownloadSearchGeoFromSchema", UserController.DownloadSearchGeoFromSchema)
		mapRouter.GET("/RestoreOfflineLayer", UserController.RestoreOfflineLayer)
		mapRouter.GET("/GetReatoreFile", UserController.GetReatoreFile)
		mapRouter.GET("/GetLayerExtent", UserController.GetLayerExtent)
		mapRouter.POST("/SplitFeature", UserController.SplitFeature)
		mapRouter.POST("/DissolveFeature", UserController.DissolveFeature)
		mapRouter.POST("/DonutBuilder", UserController.DonutBuilder)
		mapRouter.POST("/AggregatorFeature", UserController.AggregatorFeature)
		mapRouter.POST("/OffsetFeature", UserController.OffsetFeature)
		mapRouter.POST("/DeAggregatorFeature", UserController.ExplodeFeature)
		mapRouter.POST("/AreaOnAreaAnalysis", UserController.AreaOnAreaAnalysis)

		mapRouter.POST("/ChangeGeoToSchema", UserController.ChangeGeoToSchema)
		mapRouter.GET("/SyncToFile", UserController.SyncToFile)
		mapRouter.POST("/LineOnPolygonOverlay", UserController.LineOnPolygonOverlay)

	}
	SurveyRouter := r.Group("/Survey")
	{
		SurveyRouter.Static("/PIC", "./PIC")
		SurveyRouter.Static("/ZDT", "./ZDT")
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
		fields.POST("/AddField", UserController.AddField)                       // 添加字段
		fields.POST("/DeleteField", UserController.DeleteField)                 // 删除字段
		fields.POST("/ModifyField", UserController.ModifyField)                 // 修改字段
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
}
