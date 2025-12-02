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
		mapRouter.GET("/GetColorSet", UserController.GetColorSet)
		mapRouter.GET("/GetSchema", UserController.GetSchema)
		mapRouter.GET("/SchemaToExcel", UserController.SchemaToExcel)
		mapRouter.GET("/GetSchemaByUnits", UserController.GetSchemaByUnits)
		mapRouter.POST("/AddUpdateColorSet", UserController.AddUpdateColorSet)
		mapRouter.GET("/GetChangeRecord", UserController.GetChangeRecord)
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
		mapRouter.GET("/ShowSingleGeo", UserController.ShowSingleGeo)

		mapRouter.POST("/ShowTempLayerHeader", UserController.ShowTempLayerHeader)
		mapRouter.GET("/DelTempLayer", UserController.DelTempLayer)
		mapRouter.GET("/DelTempGeo", UserController.DelTempGeo)
		mapRouter.POST("/ShowTempGeoList", UserController.ShowTempGeoList)
		mapRouter.POST("/SpaceIntersect", UserController.SpaceIntersect)

		mapRouter.GET("/GetTableAttributes", UserController.GetTableAttributes)
		mapRouter.GET("/GetDeviceName", UserController.GetDeviceName)
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
		mapRouter.POST("/ChangeGeoToSchema", UserController.ChangeGeoToSchema)
		mapRouter.GET("/SyncToFile", UserController.SyncToFile)

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
}
