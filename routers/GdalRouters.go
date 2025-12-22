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
}
