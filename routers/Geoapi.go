package routers

import (
	"github.com/GrainArc/SouceMap/config"
	"github.com/GrainArc/SouceMap/methods"
	"github.com/GrainArc/SouceMap/views"
	"github.com/gin-gonic/gin"
)

func GeoRouters(r *gin.Engine) {
	UserController := &views.UserController{}
	fieldCalcCtrl := views.NewFieldCalculatorController()
	geomService := methods.NewGeometryService()
	geomHandler := views.NewGeometryHandler(geomService)
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
		mapRouter.POST("/ChangeGeoToSchema", UserController.ChangeGeoToSchema)
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
		mapRouter.POST("/UpdateLayer", UserController.UpdateLayer)
		mapRouter.POST("/AppendLayer", UserController.AppendLayer)
		mapRouter.GET("/GetUpdateMSG", UserController.GetUpdateMSG)
		mapRouter.GET("/DownloadOfflineLayer", UserController.DownloadOfflineLayer)
		mapRouter.GET("/RestoreOfflineLayer", UserController.RestoreOfflineLayer)
		mapRouter.GET("/GetReatoreFile", UserController.GetReatoreFile)
		mapRouter.GET("/GetLayerExtent", UserController.GetLayerExtent)

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
		fields.POST("/AddField", UserController.AddField)                    // 添加字段
		fields.POST("/DeleteField", UserController.DeleteField)              // 删除字段
		fields.POST("/ModifyField", UserController.ModifyField)              // 修改字段
		fields.POST("/CalculateField", fieldCalcCtrl.CalculateField)         // 执行计算
		fields.POST("/UpdateGeometryField", geomHandler.UpdateGeometryField) // 预览结果
		fields.GET("/GetFieldInfo", UserController.GetFieldInfo)             // 获取单个字段信息

	}
	mxd := r.Group("/mxd")
	{
		mxd.POST("/AddUpdateLayerMXD", UserController.AddUpdateLayerMXD)
		mxd.GET("/GetLayerMXDList", UserController.GetLayerMXDList)
		mxd.GET("/GetLayerMXDHeaderList", UserController.GetLayerMXDHeaderList)

		mxd.GET("/DelLayerMXD", UserController.DelLayerMXD)
		mxd.GET("/SyncLayerMXD", UserController.SyncLayerMXD)

	}

}
