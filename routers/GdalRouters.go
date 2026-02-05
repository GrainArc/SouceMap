package routers

import (
	"github.com/GrainArc/SouceMap/GdalView"
	"github.com/GrainArc/SouceMap/views"
	"github.com/gin-gonic/gin"
)

func GDALRouters(r *gin.Engine) {
	UserController := &GdalView.UserController{}
	trackHandler := views.NewTrackHandler()
	mapRouter := r.Group("/gdal")
	{
		// POST用于提交分析任务配置
		mapRouter.POST("/Intersect/start", UserController.StartIntersect)
		// GET用于WebSocket连接
		mapRouter.GET("/Intersect/ws/:taskId", UserController.IntersectWebSocket)
		// GET用于查询任务状态（可选）
		mapRouter.GET("/Intersect/status/:taskId", UserController.GetTaskStatus)
	}
	{
		// POST用于提交对称差异分析任务配置
		mapRouter.POST("/SymDifference/start", UserController.StartSymDifference)
		// GET用于WebSocket连接
		mapRouter.GET("/SymDifference/ws/:taskId", UserController.SymDifferenceWebSocket)
		// GET用于查询任务状态（可选）
		mapRouter.GET("/SymDifference/status/:taskId", UserController.GetTaskStatus)
	}
	{
		// POST用于提交对称差异分析任务配置
		mapRouter.POST("/Union/start", UserController.StartUnion)
		// GET用于WebSocket连接
		mapRouter.GET("/Union/ws/:taskId", UserController.UnionWebSocket)
		// GET用于查询任务状态（可选）
		mapRouter.GET("/Union/status/:taskId", UserController.GetUnionTaskStatus)
	}
	{
		mapRouter.POST("/Clip/start", UserController.StartClip)
		mapRouter.GET("/Clip/ws/:taskId", UserController.ClipWebSocket)
		mapRouter.GET("/Clip/status/:taskId", UserController.GetClipTaskStatus)
	}
	{
		// POST用于提交擦除分析任务配置
		mapRouter.POST("/Erase/start", UserController.StartErase)
		// GET用于WebSocket连接
		mapRouter.GET("/Erase/ws/:taskId", UserController.EraseWebSocket)
		// GET用于查询任务状态
		mapRouter.GET("/Erase/status/:taskId", UserController.GetEraseTaskStatus)
	}
	{
		// Identity 相关路由
		mapRouter.POST("/Identity/start", UserController.StartIdentity)
		mapRouter.GET("/Identity/ws/:taskId", UserController.IdentityWebSocket)
		mapRouter.GET("/Identity/status/:taskId", UserController.GetIdentityTaskStatus)
	}
	{
		// 空间更新分析路由
		mapRouter.POST("/Update/start", UserController.StartUpdate)
		mapRouter.GET("/Update/ws/:taskId", UserController.UpdateWebSocket)
		mapRouter.GET("/Update/status/:taskId", UserController.GetUpdateTaskStatus)
	}
	{
		//栅格切片view
		mapRouter.POST("/RasterTile/start", UserController.StartRasterTile)
		mapRouter.GET("/RasterTile/ws/:taskId", UserController.RasterTileWebSocket)
		mapRouter.GET("/RasterTile/status/:taskId", UserController.GetRasterTileTaskStatus)
		//地形切片view
		mapRouter.POST("/TerrainTile/start", UserController.StartTerrainTile)
		mapRouter.GET("/TerrainTile/ws/:taskId", UserController.TerrainTileWebSocket)
		mapRouter.GET("/TerrainTile/status/:taskId", UserController.GetTerrainTileTaskStatus)
	}

	{
		mapRouter.POST("/track/init", trackHandler.InitTrack)     // 初始化
		mapRouter.GET("/track/ws", trackHandler.ConnectWebSocket) // WebSocket 连接
	}
	{
		mapRouter.POST("/raster/ClipRaster", UserController.ClipRaster) // 初始化
		mapRouter.GET("/raster/GetRasterTaskStatus", UserController.GetRasterTaskStatus)
		mapRouter.POST("/raster/MosaicRaster", UserController.MosaicRaster)
		mapRouter.POST("/raster/GetMosaicPreview", UserController.GetMosaicPreview)
		mapRouter.GET("/raster/GetProjectionInfo", UserController.GetProjectionInfo)                                // 获取投影信息
		mapRouter.POST("/raster/DefineProjection", UserController.DefineProjection)                                 // 定义投影
		mapRouter.POST("/raster/DefineProjectionWithGeoTransform", UserController.DefineProjectionWithGeoTransform) // 定义投影+地理变换
		mapRouter.POST("/raster/ReprojectRaster", UserController.ReprojectRaster)
	}
	// 在路由配置文件中添加以下路由

	// ==================== 调色接口路由 ====================
	{
		// 综合调色
		mapRouter.POST("/raster/color/AdjustColors", UserController.AdjustColors)

		// 单项调整
		mapRouter.POST("/raster/color/AdjustBrightness", UserController.AdjustBrightness)
		mapRouter.POST("/raster/color/AdjustContrast", UserController.AdjustContrast)
		mapRouter.POST("/raster/color/AdjustSaturation", UserController.AdjustSaturation)
		mapRouter.POST("/raster/color/AdjustGamma", UserController.AdjustGamma)
		mapRouter.POST("/raster/color/AdjustHue", UserController.AdjustHue)

		// 色阶和曲线
		mapRouter.POST("/raster/color/AdjustLevels", UserController.AdjustLevels)
		mapRouter.POST("/raster/color/AdjustCurves", UserController.AdjustCurves)

		// 自动调整
		mapRouter.POST("/raster/color/AutoLevels", UserController.AutoLevels)
		mapRouter.POST("/raster/color/AutoContrast", UserController.AutoContrast)
		mapRouter.POST("/raster/color/AutoWhiteBalance", UserController.AutoWhiteBalance)

		// 直方图处理
		mapRouter.POST("/raster/color/HistogramEqualize", UserController.HistogramEqualize)
		mapRouter.POST("/raster/color/CLAHEEqualize", UserController.CLAHEEqualize)

		// 预设和特效
		mapRouter.POST("/raster/color/PresetColor", UserController.PresetColor)
		mapRouter.POST("/raster/color/SCurveContrast", UserController.SCurveContrast)

		// 调色管道
		mapRouter.POST("/raster/color/ColorPipeline", UserController.ColorPipeline)
	}

	// ==================== 匀色接口路由 ====================
	{
		// 统计信息（同步接口）
		mapRouter.POST("/raster/balance/GetColorStatistics", UserController.GetColorStatistics)
		mapRouter.POST("/raster/balance/GetBandStatistics", UserController.GetBandStatistics)

		// 匀色方法
		mapRouter.POST("/raster/balance/HistogramMatch", UserController.HistogramMatch)
		mapRouter.POST("/raster/balance/MeanStdMatch", UserController.MeanStdMatch)
		mapRouter.POST("/raster/balance/WallisFilter", UserController.WallisFilter)
		mapRouter.POST("/raster/balance/MomentMatch", UserController.MomentMatch)
		mapRouter.POST("/raster/balance/LinearRegressionBalance", UserController.LinearRegressionBalance)
		mapRouter.POST("/raster/balance/DodgingBalance", UserController.DodgingBalance)

		// 融合
		mapRouter.POST("/raster/balance/GradientBlend", UserController.GradientBlend)

		// 通用匀色
		mapRouter.POST("/raster/balance/ColorBalance", UserController.ColorBalance)
		mapRouter.POST("/raster/balance/AutoColorBalance", UserController.AutoColorBalance)
		mapRouter.POST("/raster/balance/BatchColorBalance", UserController.BatchColorBalance)
	}
	// router.go 中添加以下路由
	{
		// ==================== 波段信息查询 ====================
		mapRouter.GET("/raster/band/GetBandInfo", UserController.GetBandInfo)            // 获取单个波段信息
		mapRouter.GET("/raster/band/GetAllBandsInfo", UserController.GetAllBandsInfo)    // 获取所有波段信息
		mapRouter.POST("/raster/band/GetBandHistogram", UserController.GetBandHistogram) // 获取波段直方图
		mapRouter.GET("/raster/band/GetPaletteInfo", UserController.GetPaletteInfo)      // 获取调色板信息

		// ==================== 波段属性设置 ====================
		mapRouter.POST("/raster/band/SetColorInterp", UserController.SetBandColorInterp) // 设置颜色解释
		mapRouter.POST("/raster/band/SetNoData", UserController.SetBandNoData)           // 设置NoData值

		// ==================== 波段操作 ====================
		mapRouter.POST("/raster/band/AddBand", UserController.AddBand)             // 添加波段
		mapRouter.POST("/raster/band/RemoveBand", UserController.RemoveBand)       // 删除波段
		mapRouter.POST("/raster/band/ReorderBands", UserController.ReorderBands)   // 重排波段
		mapRouter.POST("/raster/band/ConvertType", UserController.ConvertBandType) // 转换数据类型
		mapRouter.POST("/raster/band/MergeBands", UserController.MergeBands)       // 合并波段

		// ==================== 波段运算 ====================
		mapRouter.POST("/raster/band/BandMath", UserController.BandMath)             // 波段数学运算
		mapRouter.POST("/raster/band/CalculateIndex", UserController.CalculateIndex) // 计算指数(NDVI/NDWI/EVI)
		mapRouter.POST("/raster/band/Normalize", UserController.NormalizeBand)       // 归一化

		// ==================== 滤波与重分类 ====================
		mapRouter.POST("/raster/band/ApplyFilter", UserController.ApplyFilter)   // 应用滤波器
		mapRouter.POST("/raster/band/Reclassify", UserController.ReclassifyBand) // 重分类

		// ==================== 调色板操作 ====================
		mapRouter.POST("/raster/band/SetPalette", UserController.SetPalette)     // 设置调色板
		mapRouter.POST("/raster/band/PaletteToRGB", UserController.PaletteToRGB) // 调色板转RGB
		mapRouter.POST("/raster/band/RGBToPalette", UserController.RGBToPalette) // RGB转调色板

		// ==================== 元数据操作 ====================
		mapRouter.POST("/raster/band/SetMetadata", UserController.SetBandMetadata)       // 设置元数据
		mapRouter.GET("/raster/band/GetMetadata", UserController.GetBandMetadata)        // 获取元数据
		mapRouter.POST("/raster/band/SetDescription", UserController.SetBandDescription) // 设置描述
		mapRouter.GET("/raster/band/GetDescription", UserController.GetBandDescription)  // 获取描述
		mapRouter.GET("/raster/band/GetScaleOffset", UserController.GetBandScaleOffset)  // 获取缩放偏移
		// ==================== 元数据操作（续） ====================
		mapRouter.GET("/raster/band/GetScaleOffset", UserController.GetBandScaleOffset) // 获取缩放偏移
		mapRouter.GET("/raster/band/GetUnitType", UserController.GetBandUnitType)       // 获取单位类型
	}
	{
		// ==================== 栅格计算器 ====================
		mapRouter.POST("/raster/calc/Expression", UserController.CalculateExpression)                 // 表达式计算
		mapRouter.POST("/raster/calc/ExpressionWithCondition", UserController.CalculateWithCondition) // 条件表达式计算
		mapRouter.POST("/raster/calc/ConditionalReplace", UserController.ConditionalReplace)          // 条件替换
		mapRouter.POST("/raster/calc/Batch", UserController.CalculateBatch)                           // 批量表达式计算
		mapRouter.POST("/raster/calc/Block", UserController.CalculateBlock)                           // 分块计算（大影像）
		mapRouter.POST("/raster/calc/ValidateExpression", UserController.ValidateExpression)          // 验证表达式

		// ==================== 遥感指数计算 ====================
		mapRouter.POST("/raster/index/NDVI", UserController.CalculateNDVI)   // 植被指数
		mapRouter.POST("/raster/index/NDWI", UserController.CalculateNDWI)   // 水体指数
		mapRouter.POST("/raster/index/EVI", UserController.CalculateEVI)     // 增强植被指数
		mapRouter.POST("/raster/index/SAVI", UserController.CalculateSAVI)   // 土壤调节植被指数
		mapRouter.POST("/raster/index/MNDWI", UserController.CalculateMNDWI) // 改进水体指数
		mapRouter.POST("/raster/index/NDBI", UserController.CalculateNDBI)   // 建筑指数
		mapRouter.POST("/raster/index/NDSI", UserController.CalculateNDSI)   // 雪指数
		mapRouter.POST("/raster/index/LAI", UserController.CalculateLAI)     // 叶面积指数

	}

}
